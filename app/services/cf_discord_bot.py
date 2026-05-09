"""
ClipFusion Discord Bot — slash command `/request quantite:N modele:X`
Permet aux VAs de demander un batch automatique sur Discord.

Architecture :
  - Greffe sur le bot Discord existant (discord_bot.py de Repurpose)
  - File d'attente FIFO : 1 mix à la fois pour pas saturer Railway
  - Réactions emoji pour feedback visuel (⏳ → ⚙️ → ✅ ou ❌)
  - DM final au VA avec lien Drive
  - Détection auto de l'équipe selon le serveur Discord (CF_GUILD_ID_GEELARK / CF_GUILD_ID_INSTAGRAM)

Variables d'env :
  CF_REQUEST_CHANNEL_IDS    : IDs de canaux Discord (CSV) où /request est dispo
  CF_DEFAULT_TEAM           : équipe par défaut si guild non reconnu (default geelark)
  CF_REQUEST_MAX_VIDEOS     : limite max par demande (default 200)
  CF_CHANNEL_MSG_TTL        : durée (sec) avant auto-delete des messages canal (default 60)
  CF_GUILD_ID_GEELARK       : ID du serveur Discord Geelark (équipe geelark)
  CF_GUILD_ID_INSTAGRAM     : ID du serveur Discord Instagram (équipe instagram)
"""
from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass, field
from typing import List, Optional

try:
    import discord
    from discord import app_commands
    from discord.ext import commands
    DISCORD_AVAILABLE = True
except Exception:
    DISCORD_AVAILABLE = False

from app.services import cf_storage
from app.services import cf_mixer as mixer_service
from app.utils.logger import get_logger

logger = get_logger("cf_discord_bot")


def _get_request_channel_ids() -> List[int]:
    """Liste de canaux où /request fonctionne. Si vide, slash command dispo partout."""
    raw = os.environ.get("CF_REQUEST_CHANNEL_IDS", "")
    out: List[int] = []
    for piece in raw.replace(";", ",").split(","):
        p = piece.strip()
        if p.isdigit():
            out.append(int(p))
    return out


def _default_team() -> str:
    return os.environ.get("CF_DEFAULT_TEAM", "geelark").lower().strip()


def _max_videos_per_request() -> int:
    try:
        return int(os.environ.get("CF_REQUEST_MAX_VIDEOS", "200"))
    except Exception:
        return 200


def _channel_msg_ttl() -> int:
    """Durée (en secondes) avant auto-suppression des messages dans le canal."""
    try:
        return int(os.environ.get("CF_CHANNEL_MSG_TTL", "30"))
    except Exception:
        return 30


def _seconds_per_video() -> int:
    """Estimation moyenne du temps de génération d'1 vidéo (en secondes)."""
    try:
        return int(os.environ.get("CF_SECONDS_PER_VIDEO", "8"))
    except Exception:
        return 8


def _format_eta(seconds: int) -> str:
    """Formate une durée en secondes en texte lisible (ex: '~5 minutes', '~30 secondes')."""
    if seconds < 60:
        return f"~{seconds} secondes"
    minutes = (seconds + 30) // 60  # arrondi au plus proche
    if minutes == 1:
        return "~1 minute"
    return f"~{minutes} minutes"


def _compute_queue_eta_seconds(new_videos: int) -> int:
    """
    Calcule l'ETA totale (en secondes) pour une nouvelle demande de N vidéos.
    Inclut : mix en cours (si dispo) + file d'attente + nouvelle demande.
    """
    spv = _seconds_per_video()
    total = new_videos * spv

    # Ajoute les vidéos en cours de mix (estimation : on assume qu'il reste la moitié)
    if _current is not None:
        try:
            total += (_current.quantite * spv) // 2
        except Exception:
            pass

    # Ajoute toutes les demandes en attente devant nous
    for p in _pending:
        try:
            total += p.quantite * spv
        except Exception:
            pass

    return total


def _detect_team_from_guild(guild_id: Optional[int]) -> str:
    """
    Détecte l'équipe selon le serveur Discord d'où vient la commande.
    Si le guild_id correspond à CF_GUILD_ID_GEELARK → 'geelark'
    Si le guild_id correspond à CF_GUILD_ID_INSTAGRAM → 'instagram'
    Sinon → fallback sur CF_DEFAULT_TEAM
    """
    if not guild_id:
        return _default_team()

    geelark_id = os.environ.get("CF_GUILD_ID_GEELARK", "").strip()
    instagram_id = os.environ.get("CF_GUILD_ID_INSTAGRAM", "").strip()

    if geelark_id and str(guild_id) == geelark_id:
        return "geelark"
    if instagram_id and str(guild_id) == instagram_id:
        return "instagram"

    # Pas de match : fallback sur la valeur par défaut
    logger.warning(
        f"Guild ID {guild_id} ne matche ni Geelark ({geelark_id}) ni Instagram ({instagram_id}), "
        f"fallback sur CF_DEFAULT_TEAM={_default_team()}"
    )
    return _default_team()


# ============================================================================
# RATE LIMITING par VA (anti-abus)
# ============================================================================
def _rate_limit_config() -> tuple:
    """
    Retourne (max_videos, period_days) pour le rate limit.
    Variables d'env :
      CF_RATE_LIMIT_VIDEOS  : quota de vidéos sur la période (default 300)
      CF_RATE_LIMIT_DAYS    : période en jours (default 3)
    Si CF_RATE_LIMIT_VIDEOS = 0, le rate limit est désactivé.
    """
    try:
        max_v = int(os.environ.get("CF_RATE_LIMIT_VIDEOS", "300"))
    except Exception:
        max_v = 300
    try:
        days = max(1, int(os.environ.get("CF_RATE_LIMIT_DAYS", "3")))
    except Exception:
        days = 3
    return max_v, days


def _is_admin(member) -> bool:
    """Bypass admin : les administrateurs Discord ignorent le rate limit."""
    if not isinstance(member, discord.Member):
        return False
    try:
        return bool(member.guild_permissions.administrator)
    except Exception:
        return False


def _check_rate_limit(va_name: str, requested_qty: int) -> tuple:
    """
    Vérifie si un VA peut faire une demande de N vidéos.
    Retourne (allowed: bool, used: int, limit: int, remaining: int).

    Compte uniquement les vidéos GÉNÉRÉES avec succès (videos_count > 0)
    sur les CF_RATE_LIMIT_DAYS derniers jours.
    """
    max_v, days = _rate_limit_config()

    # Rate limit désactivé si max=0
    if max_v <= 0:
        return (True, 0, 0, 0)

    try:
        used = cf_storage.count_va_videos_recent(va_name, days=days)
    except Exception as e:
        logger.warning(f"count_va_videos_recent failed pour {va_name}: {e}")
        # En cas d'échec DB, on laisse passer (fail-open) pour pas bloquer le service
        return (True, 0, max_v, max_v)

    remaining = max(0, max_v - used)
    # Le VA ne peut demander que ce qu'il lui reste
    allowed = (used + requested_qty) <= max_v
    return (allowed, used, max_v, remaining)


async def _notify_admin_rate_limit_exceeded(
    bot, team: str, user_name: str, user_id: int,
    requested: int, used: int, limit: int, days: int,
):
    """
    Envoie une notif dans le canal admin (#spoofbot-notifs) quand un VA dépasse
    le rate limit. Permet aux managers/CEO de surveiller les VAs trop agressifs.
    """
    try:
        # Récupère le canal selon l'équipe (réutilise la même logique que les notifs batch)
        env_name = {
            "geelark": "DISCORD_BATCH_CHANNEL_ID_GEELARK",
            "instagram": "DISCORD_BATCH_CHANNEL_ID_INSTAGRAM",
        }.get(team, "DISCORD_BATCH_CHANNEL_ID")

        channel_id_str = os.environ.get(env_name, "").strip()
        if not channel_id_str:
            return
        channel = bot.get_channel(int(channel_id_str))
        if not channel:
            return

        embed = discord.Embed(
            title="🚫 Rate limit dépassé",
            description=f"Un VA a tenté de demander des vidéos au-delà du quota.",
            color=0xef4444,
        )
        embed.add_field(name="👤 VA", value=f"<@{user_id}> ({user_name})", inline=True)
        embed.add_field(name="📊 Équipe", value=team, inline=True)
        embed.add_field(
            name="📈 Quota",
            value=f"{used}/{limit} vidéos sur {days}j",
            inline=False,
        )
        embed.add_field(
            name="❌ Demande refusée",
            value=f"**{requested}** vidéos demandées",
            inline=True,
        )
        await channel.send(embed=embed)
    except Exception as e:
        logger.warning(f"Notif admin rate limit échoué: {e}")


# ============================================================================
# QUEUE FIFO : un mix à la fois pour pas exploser Railway
# ============================================================================
@dataclass
class CFRequest:
    """Un job dans la queue."""
    interaction_id: int
    channel_id: int
    user_id: int
    user_name: str
    quantite: int
    model_id: int
    team: str
    # Discord references (résolues à l'exécution)
    interaction: Optional["discord.Interaction"] = None


_queue: "asyncio.Queue[CFRequest]" = None  # type: ignore
_worker_task: Optional[asyncio.Task] = None
_current: Optional[CFRequest] = None
_pending: List[CFRequest] = []


def _ensure_queue():
    global _queue
    if _queue is None:
        _queue = asyncio.Queue()


# ============================================================================
# WORKER : traite la queue séquentiellement
# ============================================================================
async def _worker_loop():
    """Boucle infinie qui processe les requêtes une par une."""
    global _current
    while True:
        try:
            req: CFRequest = await _queue.get()
            _current = req
            try:
                _pending.remove(req)
            except ValueError:
                pass
            try:
                await _process_request(req)
            except Exception as e:
                logger.exception(f"Erreur worker: {e}")
                try:
                    if req.interaction:
                        # Message d'erreur s'auto-supprime aussi
                        await _say(req, f"❌ Erreur pendant le mix : {e}")
                except Exception:
                    pass
            finally:
                _current = None
                _queue.task_done()
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.exception(f"Worker loop crash: {e}")
            await asyncio.sleep(1)


async def _process_request(req: CFRequest):
    """Lance un mix complet pour la requête + envoie les feedback Discord."""
    # 1. Vérifie qu'on a bien des templates et des vidéos pour ce modèle
    templates = cf_storage.list_templates() or []
    videos = cf_storage.list_videos(model_id=req.model_id) or []

    if not templates:
        await _say(req, f"❌ Aucun template configuré dans ClipFusion (admin doit en ajouter d'abord).")
        return
    if not videos:
        model = cf_storage.get_model(req.model_id)
        label = model.get("label") if model else f"ID {req.model_id}"
        await _say(req, f"❌ Aucune vidéo dans la catégorie `{label}` (ID {req.model_id}). L'admin doit uploader des vidéos d'abord.")
        return

    # 2. Caps : on ne peut pas demander plus que tpl × videos disponibles
    max_possible = len(templates) * len(videos)
    actual_qty = min(req.quantite, max_possible)

    # 3. Récupère les infos VA depuis Discord
    va_name = req.user_name  # Display name du VA
    # Tente de récupérer l'email du VA via discord_va_sync (si configuré)
    va_email_known = False
    try:
        from app.services.va_emails_db import load_all_emails, is_db_enabled as va_db_enabled
        if va_db_enabled():
            emails = load_all_emails() or {}
            if str(req.user_id) in emails:
                va_email_known = True
    except Exception:
        pass

    model = cf_storage.get_model(req.model_id)
    model_label = model.get("label", f"ID{req.model_id}") if model else f"ID{req.model_id}"

    await _say(req, f"⚙️ Lancement : **{actual_qty}** vidéos · modèle **{model_label}** · équipe **{req.team}**" + (
        "" if va_email_known else f"\n⚠️ Pas d'email enregistré pour toi ({va_name}), le partage Drive auto sera skippé."
    ))

    # 4. Lance le mix en consommant le générateur
    progress_count = 0
    last_update_count = 0
    drive_url: Optional[str] = None

    try:
        for ev in mixer_service.mix_batch_stream(
            templates=templates,
            videos=videos,
            music_list=None,
            max_variants=actual_qty,
            size_label="L",
            audio_priority="template",
            position_pct=50,
            font_size_px=56,
            max_duration=None,
            caption_style="outlined",
            device_choice="iphone_random",
            va_name=va_name,
            team=req.team,
            enabled_filters=None,    # tous activés
            custom_ranges=None,      # plages par défaut
            model_id=req.model_id,
        ):
            t = ev.get("type")
            # FIX : le mixer émet "item_done" pour chaque vidéo terminée (pas "progress")
            if t == "item_done":
                progress_count += 1
                # Progress chaque 10 vidéos
                if progress_count - last_update_count >= 10 or progress_count == actual_qty:
                    last_update_count = progress_count
                    await _say(req, f"⚡ {progress_count}/{actual_qty} vidéos générées...")
            elif t == "done":
                drive = ev.get("drive") or {}
                drive_url = drive.get("folder_url") or None
            elif t == "error":
                msg = ev.get("message", "Erreur inconnue")
                await _say(req, f"❌ Mix échoué : {msg}")
                return
    except Exception as e:
        logger.exception("Mix échoué")
        await _say(req, f"❌ Erreur pendant le mix : {e}")
        return

    # 5. Notif finale dans le canal — courte, pas de lien (le lien va en DM privé)
    if drive_url:
        msg = f"✅ Mix terminé : **{progress_count}** vidéos prêtes ! 📩 Lien envoyé en DM."
    else:
        msg = f"✅ Mix terminé : **{progress_count}** vidéos générées (Drive non configuré)."
    await _say(req, msg)

    # 6. DM au VA avec le lien Drive (vrai message privé)
    if drive_url and req.interaction:
        try:
            user = await req.interaction.client.fetch_user(req.user_id)
            await user.send(
                f"🎬 **Ton batch ClipFusion est prêt !**\n"
                f"📊 **{progress_count}** vidéos · modèle **{model_label}** · équipe **{req.team}**\n"
                f"📁 {drive_url}"
            )
        except discord.Forbidden:
            # VA a désactivé les DMs : on lui dit dans le canal (s'auto-supprime)
            await _say(req, f"⚠️ <@{req.user_id}> impossible de t'envoyer un DM (DMs désactivés). Lien Drive : <{drive_url}>")
        except Exception as e:
            logger.warning(f"DM VA échoué: {e}")
            await _say(req, f"⚠️ DM échoué. Lien Drive : <{drive_url}>")


async def _say(req: CFRequest, content: str):
    """
    Envoie un follow-up sur l'interaction (visible à tous dans le canal),
    qui s'auto-supprime après CF_CHANNEL_MSG_TTL secondes (60 par défaut).
    Discord supporte le paramètre delete_after sur followup.send.
    """
    try:
        if req.interaction:
            ttl = _channel_msg_ttl()
            await req.interaction.followup.send(
                content,
                ephemeral=False,
                # delete_after = secondes avant suppression auto par Discord
                **({"delete_after": float(ttl)} if ttl > 0 else {}),
            )
    except Exception as e:
        logger.warning(f"Reply Discord échoué: {e}")


# ============================================================================
# INSTALLATION DU SLASH COMMAND SUR LE BOT EXISTANT
# ============================================================================
def _has_model_role(member: "discord.Member", model_id: int) -> bool:
    """
    Check si le VA a le rôle Discord 'ID{X}' qui l'autorise à utiliser ce modèle.
    Match insensible à la casse : 'ID1', 'id1', 'Id1' tous OK.
    Les admins (permissions.administrator) bypassent toujours.
    """
    if not isinstance(member, discord.Member):
        return False
    # Admin bypass
    try:
        if member.guild_permissions.administrator:
            return True
    except Exception:
        pass
    target = f"id{int(model_id)}"
    for role in member.roles:
        if (role.name or "").strip().lower() == target:
            return True
    return False


def _allowed_models_for_member(member: "discord.Member") -> List[int]:
    """Liste des IDs de modèles auxquels le membre a accès via ses rôles."""
    if not isinstance(member, discord.Member):
        return []
    # Admin = tout
    try:
        if member.guild_permissions.administrator:
            return [m["id"] for m in cf_storage.list_models()]
    except Exception:
        pass
    out: List[int] = []
    import re as _re
    for role in member.roles:
        m = _re.match(r"^id(\d+)$", (role.name or "").strip().lower())
        if m:
            out.append(int(m.group(1)))
    return out


def install_clipfusion_commands(bot: "commands.Bot") -> None:
    """
    À appeler une fois le bot construit (avant tree.sync). Ajoute /request
    et démarre le worker async.
    """
    if not DISCORD_AVAILABLE:
        logger.warning("discord.py non disponible, slash commands ClipFusion skip")
        return

    @bot.tree.command(name="request", description="Demander un batch ClipFusion (vidéos prêtes à poster)")
    @app_commands.describe(
        quantite="Nombre de vidéos à générer (max 200)",
        modele="ID du modèle (créatrice). Liste les modèles avec /models",
    )
    async def request_cmd(interaction: "discord.Interaction", quantite: int, modele: int):
        # 1. Filtrage canal si configuré
        allowed = _get_request_channel_ids()
        if allowed and interaction.channel_id not in allowed:
            await interaction.response.send_message(
                f"❌ Cette commande n'est pas dispo dans ce canal.",
                ephemeral=True,
            )
            return

        # 2. CONTRÔLE D'ACCÈS PAR RÔLE : le VA doit avoir le rôle ID{modele}
        if not _has_model_role(interaction.user, modele):
            allowed_ids = _allowed_models_for_member(interaction.user)
            if allowed_ids:
                lst = ", ".join(f"`ID{i}`" for i in sorted(allowed_ids))
                msg = (
                    f"❌ Tu n'as pas accès au modèle **ID{modele}**.\n"
                    f"Tes modèles autorisés : {lst}"
                )
            else:
                msg = (
                    f"❌ Tu n'as pas accès au modèle **ID{modele}**.\n"
                    f"Aucun rôle de modèle assigné. Demande à un admin de t'attribuer un rôle `IDX`."
                )
            await interaction.response.send_message(msg, ephemeral=True)
            return

        # 3. Validation quantité
        max_q = _max_videos_per_request()
        if quantite < 1:
            await interaction.response.send_message(
                f"❌ Quantité invalide (min 1).",
                ephemeral=True,
            )
            return
        if quantite > max_q:
            await interaction.response.send_message(
                f"❌ Quantité trop élevée (max {max_q}).",
                ephemeral=True,
            )
            return

        # 4. Validation modèle
        model = cf_storage.get_model(modele)
        if not model:
            available = cf_storage.list_models()
            if available:
                lst = ", ".join(f"`{m['id']}` ({m['label']})" for m in available[:20])
                msg = f"❌ Modèle ID **{modele}** introuvable.\nDispos : {lst}"
            else:
                msg = f"❌ Modèle ID **{modele}** introuvable.\nAucun modèle créé pour l'instant. Demande à l'admin."
            await interaction.response.send_message(msg, ephemeral=True)
            return

        # 4b. RATE LIMIT (anti-abus) — bypass admin
        va_name_for_check = interaction.user.display_name or interaction.user.name
        is_admin_user = _is_admin(interaction.user)
        if not is_admin_user:
            allowed, used, limit, remaining = _check_rate_limit(va_name_for_check, quantite)
            if not allowed:
                _, days = _rate_limit_config()
                if remaining > 0:
                    msg = (
                        f"🚫 **Rate limit atteint.**\n"
                        f"Tu as déjà demandé **{used}/{limit}** vidéos sur les **{days}** derniers jours.\n"
                        f"Tu peux encore demander **{remaining}** vidéos avant ta prochaine fenêtre.\n"
                        f"_(quota réinitialisé progressivement au fil des jours)_"
                    )
                else:
                    msg = (
                        f"🚫 **Rate limit atteint.**\n"
                        f"Tu as épuisé ton quota de **{limit}** vidéos sur les **{days}** derniers jours.\n"
                        f"Reviens dans quelques jours, le quota se réinitialise progressivement."
                    )
                await interaction.response.send_message(msg, ephemeral=True)
                # Notif admin dans le canal #spoofbot-notifs
                guild_id = interaction.guild_id if interaction.guild_id else None
                team_for_notif = _detect_team_from_guild(guild_id)
                await _notify_admin_rate_limit_exceeded(
                    bot=interaction.client,
                    team=team_for_notif,
                    user_name=va_name_for_check,
                    user_id=interaction.user.id,
                    requested=quantite,
                    used=used,
                    limit=limit,
                    days=days,
                )
                return

        # 5. Construit la requête + ajoute dans la queue
        # NOUVEAU : détection auto de l'équipe selon le serveur Discord
        _ensure_queue()
        guild_id = interaction.guild_id if interaction.guild_id else None
        team = _detect_team_from_guild(guild_id)

        # Calcul ETA AVANT d'ajouter à la queue (pour pas se compter soi-même 2 fois)
        eta_seconds = _compute_queue_eta_seconds(quantite)
        eta_str = _format_eta(eta_seconds)

        req = CFRequest(
            interaction_id=interaction.id,
            channel_id=interaction.channel_id or 0,
            user_id=interaction.user.id,
            user_name=interaction.user.display_name or interaction.user.name,
            quantite=quantite,
            model_id=modele,
            team=team,
            interaction=interaction,
        )
        _pending.append(req)
        await _queue.put(req)

        # 6. Réponse initiale dans le canal (s'auto-supprime après CF_CHANNEL_MSG_TTL = 30s)
        position = len(_pending) + (1 if _current and _current is not req else 0)
        if position <= 1 and _current is None:
            ack = (
                f"⚙️ Demande acceptée : **{quantite}** vidéos pour modèle **{model['label']}** · équipe **{team}**\n"
                f"📩 <@{interaction.user.id}> tu as reçu un DM avec les détails. Lancement immédiat..."
            )
        else:
            ack = (
                f"⏳ Demande acceptée : **{quantite}** vidéos pour modèle **{model['label']}** · équipe **{team}**\n"
                f"📩 <@{interaction.user.id}> tu as reçu un DM avec les détails. Position dans la file : **{position}**"
            )
        ttl = _channel_msg_ttl()
        await interaction.response.send_message(
            ack,
            ephemeral=False,
            **({"delete_after": float(ttl)} if ttl > 0 else {}),
        )

        # 7. DM "en préparation" envoyé immédiatement au VA avec l'ETA
        try:
            user = await interaction.client.fetch_user(interaction.user.id)
            position_msg = ""
            if position > 1:
                position_msg = f"\n⏳ Position dans la file : **{position}**"
            await user.send(
                f"🎬 **Ta demande est en préparation !**\n"
                f"📊 **{quantite}** vidéos · modèle **{model['label']}** · équipe **{team}**\n"
                f"⏱️ Tu vas recevoir ton drive dans environ **{eta_str}**{position_msg}\n"
                f"Je t'enverrai le lien Drive dès que c'est prêt 🚀"
            )
        except discord.Forbidden:
            logger.warning(f"DM 'en préparation' refusé pour user {interaction.user.id} (DMs désactivés)")
        except Exception as e:
            logger.warning(f"DM 'en préparation' échoué pour user {interaction.user.id}: {e}")

        # Démarre le worker s'il ne tourne pas
        global _worker_task
        if _worker_task is None or _worker_task.done():
            _worker_task = asyncio.create_task(_worker_loop())

    @bot.tree.command(name="models", description="Liste les modèles auxquels tu as accès")
    async def models_cmd(interaction: "discord.Interaction"):
        allowed = _get_request_channel_ids()
        if allowed and interaction.channel_id not in allowed:
            await interaction.response.send_message(
                f"❌ Cette commande n'est pas dispo dans ce canal.",
                ephemeral=True,
            )
            return
        all_models = cf_storage.list_models()
        if not all_models:
            await interaction.response.send_message(
                "Aucun modèle configuré. Demande à l'admin d'en créer.",
                ephemeral=True,
            )
            return
        # Filtre selon les rôles du VA
        my_ids = set(_allowed_models_for_member(interaction.user))
        my_models = [m for m in all_models if m["id"] in my_ids]

        if not my_models:
            await interaction.response.send_message(
                "❌ Tu n'as accès à aucun modèle.\nDemande à un admin de t'attribuer un rôle `IDX` (ex: `ID1`, `ID2`).",
                ephemeral=True,
            )
            return

        lines = []
        for m in my_models:
            n_videos = len(cf_storage.list_videos(model_id=m["id"]))
            lines.append(f"• **ID {m['id']}** — {m['label']} ({n_videos} vidéos)")
        msg = "**📋 Tes modèles autorisés :**\n" + "\n".join(lines)
        msg += "\n\nUtilise `/request quantite:50 modele:1`"
        await interaction.response.send_message(msg, ephemeral=True)

    @bot.tree.command(name="status", description="Voir l'état de la file d'attente ClipFusion")
    async def status_cmd(interaction: "discord.Interaction"):
        if _current is None and not _pending:
            await interaction.response.send_message(
                "✅ File d'attente vide. Aucun mix en cours.",
                ephemeral=True,
            )
            return
        msg = "**📊 État de la file ClipFusion :**\n"
        if _current:
            msg += f"⚙️ En cours : {_current.user_name} · {_current.quantite} vidéos · ID {_current.model_id}\n"
        if _pending:
            msg += f"\n**File d'attente** ({len(_pending)}) :\n"
            for i, p in enumerate(_pending[:10], 1):
                msg += f"{i}. {p.user_name} · {p.quantite} vidéos · ID {p.model_id}\n"
        await interaction.response.send_message(msg, ephemeral=True)

    logger.info("Slash commands ClipFusion installés (/request, /models, /status)")
