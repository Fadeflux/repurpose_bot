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
import random
import tempfile
import uuid
from datetime import datetime
from pathlib import Path
from dataclasses import dataclass, field
from typing import List, Optional, Dict

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


def _get_respoof_channel_ids() -> List[int]:
    """
    Liste de canaux où /respoof fonctionne.
    Si la variable d'env CF_RESPOOF_CHANNEL_IDS n'est pas définie,
    fallback sur les IDs hardcodés (Geelark + Instagram).
    NB : Threads n'utilise PAS /respoof — il a son propre canal auto-spoof
    (cf discord_bot._get_spoof_channel_ids).
    """
    raw = os.environ.get("CF_RESPOOF_CHANNEL_IDS", "").strip()
    if not raw:
        # Fallback hardcodé sur les 2 canaux respoof configurés
        return [
            1497103659094380625,  # Geelark
            1497103579633418280,  # Instagram
        ]
    out: List[int] = []
    for piece in raw.replace(";", ",").split(","):
        p = piece.strip()
        if p.isdigit():
            out.append(int(p))
    return out


def _default_team() -> str:
    return os.environ.get("CF_DEFAULT_TEAM", "geelark").lower().strip()


def _max_videos_per_request() -> int:
    """
    Limite max de vidéos par /request (par compte, par batch).
    18 = pile-poil 6 par fenêtre horaire (matin/soir/nuit), bonne pratique
    anti-détection (pas trop de posts par fenêtre, ressemble à un humain).
    """
    try:
        return int(os.environ.get("CF_REQUEST_MAX_VIDEOS", "18"))
    except Exception:
        return 18


def _channel_msg_ttl() -> int:
    """Durée (en secondes) avant auto-suppression des messages dans le canal."""
    try:
        return int(os.environ.get("CF_CHANNEL_MSG_TTL", "30"))
    except Exception:
        return 30


def _seconds_per_video() -> int:
    """
    Estimation moyenne du temps de génération d'1 vidéo (en secondes).
    Augmenté de 8s à 22s après passage du preset libx264 veryfast → medium
    (compression 2-3x plus efficace mais encode 2-3x plus lent).
    """
    try:
        return int(os.environ.get("CF_SECONDS_PER_VIDEO", "22"))
    except Exception:
        return 22


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
      CF_RATE_LIMIT_VIDEOS  : quota de vidéos sur la période (default 500)
      CF_RATE_LIMIT_DAYS    : période en jours (default 3)
    Si CF_RATE_LIMIT_VIDEOS = 0, le rate limit est désactivé.
    """
    try:
        max_v = int(os.environ.get("CF_RATE_LIMIT_VIDEOS", "500"))
    except Exception:
        max_v = 500
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
    # Compte Insta cible (optionnel) - dict avec username, device_choice, gps_lat, gps_lng, gps_city
    account: Optional[dict] = None
    # Timezone du VA pour les fenêtres horaires (benin / madagascar)
    tz_name: str = "benin"


_queue: "asyncio.Queue[CFRequest]" = None  # type: ignore
_worker_task: Optional[asyncio.Task] = None
_current: Optional[CFRequest] = None
_pending: List[CFRequest] = []
# Set des interaction_ids des batchs annulés via /cancel.
# Le worker check ce set quand il pull de la queue : si match → skip.
# On n'annule QUE les pending (pas _current qui est déjà en FFmpeg).
_cancelled_ids: set = set()


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
            # Check si le batch a été annulé via /cancel avant d'être traité.
            # Dans ce cas on skip proprement (pas de process, juste task_done).
            if req.interaction_id in _cancelled_ids:
                logger.info(
                    f"Batch annulé (interaction_id={req.interaction_id}, "
                    f"user={req.user_name}) → skip"
                )
                _cancelled_ids.discard(req.interaction_id)
                try:
                    _pending.remove(req)
                except ValueError:
                    pass
                _queue.task_done()
                continue

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
            account=req.account,     # NOUVEAU : si fourni, active le mode fenêtres
            tz_name=req.tz_name,     # NOUVEAU : timezone du VA (benin/madagascar)
            channel_id=req.channel_id,  # Fallback canal si DMs du VA fermés
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

    Note : on n'utilise PAS le param `delete_after` de followup.send car certaines
    versions de discord.py ne le supportent pas pour les Webhooks/Followups.
    On gère la suppression manuellement via une tâche async.
    """
    try:
        if req.interaction:
            ttl = _channel_msg_ttl()
            msg = await req.interaction.followup.send(
                content,
                ephemeral=False,
            )
            # Suppression auto après TTL (si supportée par la version discord.py)
            if ttl > 0 and msg:
                async def _auto_delete():
                    try:
                        await asyncio.sleep(ttl)
                        await msg.delete()
                    except Exception:
                        pass  # Message déjà supprimé / canal inaccessible / etc.
                asyncio.create_task(_auto_delete())
    except Exception as e:
        logger.warning(f"Reply Discord échoué: {e}")


# ============================================================================
# INSTALLATION DU SLASH COMMAND SUR LE BOT EXISTANT
# ============================================================================
def _get_model_role_id_map() -> Dict[int, List[int]]:
    """
    Parse la variable DISCORD_MODEL_ROLE_IDS qui contient une map model_id → [role_ids].
    Format attendu : '1:1234567890,1:9876543210,2:5555555555,...'
    Une même clé model_id peut apparaître plusieurs fois (1 par serveur Discord).
    Retourne {model_id: [role_id1, role_id2, ...]} si la var est configurée, sinon {}.
    """
    raw = os.environ.get("DISCORD_MODEL_ROLE_IDS", "").strip()
    if not raw:
        return {}
    result: Dict[int, List[int]] = {}
    for pair in raw.split(","):
        pair = pair.strip()
        if not pair or ":" not in pair:
            continue
        try:
            mid_str, rid_str = pair.split(":", 1)
            mid = int(mid_str.strip())
            rid = int(rid_str.strip())
            result.setdefault(mid, []).append(rid)
        except (ValueError, TypeError):
            continue
    return result


def _has_model_role(member: "discord.Member", model_id: int) -> bool:
    """
    Check si le VA a le rôle Discord qui l'autorise à utiliser ce modèle.
    
    Priorité : 
    1. Si DISCORD_MODEL_ROLE_IDS configuré, check si le membre a UN des role_ids associés au modèle
    2. Sinon, fallback sur le nom du rôle 'ID{X}' (insensible à la casse)
    
    Les admins (permissions.administrator) bypassent toujours.
    """
    try:
        if not isinstance(member, discord.Member):
            logger.warning(f"[_has_model_role] member n'est pas Member: type={type(member).__name__}")
            return False
        # Admin bypass
        try:
            if member.guild_permissions.administrator:
                return True
        except Exception as e:
            logger.warning(f"[_has_model_role] admin check failed: {e}")
        
        # Mode 1 : matching par ID Discord (si configuré)
        role_map = _get_model_role_id_map()
        if role_map:
            target_role_ids = set(role_map.get(int(model_id), []))
            member_role_ids = []
            try:
                member_role_ids = [r.id for r in member.roles]
            except Exception as e:
                logger.warning(f"[_has_model_role] member.roles failed: {e}")
                return False
            logger.info(f"[_has_model_role] member {member.id} model_id={model_id} member_roles={member_role_ids} target_roles={list(target_role_ids)}")
            if target_role_ids:
                for rid in member_role_ids:
                    if rid in target_role_ids:
                        return True
                return False
            # Si model_id pas dans la map, fallback sur le nom
        
        # Mode 2 : matching par nom du rôle 'ID{X}'
        target = f"id{int(model_id)}"
        for role in member.roles:
            if (role.name or "").strip().lower() == target:
                return True
        return False
    except Exception as e:
        logger.error(f"[_has_model_role] CRASH: {e}", exc_info=True)
        return False


def _allowed_models_for_member(member: "discord.Member") -> List[int]:
    """
    Liste des IDs de modèles auxquels le membre a accès via ses rôles.
    
    Priorité :
    1. Si DISCORD_MODEL_ROLE_IDS configuré, check par IDs Discord
    2. Sinon, fallback sur les noms 'ID{X}'
    """
    try:
        if not isinstance(member, discord.Member):
            logger.warning(f"[_allowed_models] member n'est pas Member: type={type(member).__name__}")
            return []
        # Admin = tout
        try:
            if member.guild_permissions.administrator:
                return [m["id"] for m in cf_storage.list_models()]
        except Exception as e:
            logger.warning(f"[_allowed_models] admin check failed: {e}")
        
        out: List[int] = []
        
        # Mode 1 : matching par ID Discord
        role_map = _get_model_role_id_map()
        if role_map:
            # Inverse la map : {role_id: model_id}
            inverse: Dict[int, int] = {}
            for mid, rids in role_map.items():
                for rid in rids:
                    inverse[rid] = mid
            member_role_ids = []
            try:
                member_role_ids = [r.id for r in member.roles]
            except Exception as e:
                logger.warning(f"[_allowed_models] member.roles failed: {e}")
                return []
            for rid in member_role_ids:
                if rid in inverse:
                    mid = inverse[rid]
                    if mid not in out:
                        out.append(mid)
            logger.info(f"[_allowed_models] member {member.id} member_roles={member_role_ids} → models={out}")
            return out
        
        # Mode 2 : matching par nom du rôle 'ID{X}' (fallback)
        import re as _re
        for role in member.roles:
            m = _re.match(r"^id(\d+)$", (role.name or "").strip().lower())
            if m:
                out.append(int(m.group(1)))
        return out
    except Exception as e:
        logger.error(f"[_allowed_models] CRASH: {e}", exc_info=True)
        return []


if DISCORD_AVAILABLE:
    class _BigBatchConfirmView(discord.ui.View):
        """
        View avec boutons ✅/❌ pour confirmer un gros batch (>= seuil).
        Évite les misclick à 50 vidéos.

        Timeout = 30s par défaut. Si pas de réponse, confirmed reste None
        (l'appelant doit le traiter comme "annulé").
        """

        def __init__(self, timeout: float = 30.0):
            super().__init__(timeout=timeout)
            self.confirmed: Optional[bool] = None

        @discord.ui.button(label="✅ Confirmer", style=discord.ButtonStyle.green)
        async def confirm_btn(
            self,
            interaction: "discord.Interaction",
            button: "discord.ui.Button",
        ):
            await interaction.response.defer()
            self.confirmed = True
            self.stop()

        @discord.ui.button(label="❌ Annuler", style=discord.ButtonStyle.red)
        async def cancel_btn(
            self,
            interaction: "discord.Interaction",
            button: "discord.ui.Button",
        ):
            await interaction.response.defer()
            self.confirmed = False
            self.stop()


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
        quantite="Nombre de vidéos à générer (max 18 par compte, réparties sur 3 fenêtres horaires)",
        modele="ID du modèle (créatrice). Liste les modèles avec /models",
        compte="Username du compte Insta (sans @). Tape pour autocomplete tes comptes.",
    )
    async def request_cmd(
        interaction: "discord.Interaction",
        quantite: int,
        modele: int,
        compte: str,
    ):
        # 1. Filtrage canal si configuré
        allowed = _get_request_channel_ids()
        if allowed and interaction.channel_id not in allowed:
            await interaction.response.send_message(
                f"❌ Cette commande n'est pas dispo dans ce canal.",
                ephemeral=True,
            )
            return
        # Empêcher /request dans les canaux dédiés respoof
        respoof_channels = _get_respoof_channel_ids()
        if respoof_channels and interaction.channel_id in respoof_channels:
            await interaction.response.send_message(
                f"❌ Ce canal est réservé aux **respoof** (vite fait, 1 fichier).\n"
                f"Pour un batch complet de vidéos avec captions, utilise `/request` "
                f"dans le canal dédié aux demandes de Drive.",
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

        # DEFER : à partir d'ici on fait des appels DB (email, rate limit, model
        # lookup, accounts, interval check) qui peuvent prendre > 3s sur Railway.
        # Sans defer, Discord affiche "L'application ne répond plus".
        # On peut envoyer des followups ephemeral OU publics après ce defer.
        await interaction.response.defer(ephemeral=False, thinking=True)

        # === VALIDATIONS NO-DB EN PREMIER (fail fast, pas besoin d'attendre la DB) ===
        max_q = _max_videos_per_request()
        if quantite < 1:
            await interaction.followup.send(
                f"❌ Quantité invalide (min 1).",
                ephemeral=True,
            )
            return
        if quantite > max_q:
            await interaction.followup.send(
                f"❌ Quantité trop élevée (max {max_q}).",
                ephemeral=True,
            )
            return

        clean_username = compte.strip().lstrip("@").strip() if compte else ""
        if not clean_username:
            await interaction.followup.send(
                "❌ Le nom du compte est invalide. Tape par exemple `compte:sara_official_2026` (sans @).",
                ephemeral=True,
            )
            return

        # === CONFIRMATION pour les gros batchs ===
        # Évite les misclicks à 50 vidéos : au-delà du seuil, on demande une
        # confirmation explicite via boutons. Configurable via CF_CONFIRM_THRESHOLD.
        try:
            confirm_threshold = int(os.environ.get("CF_CONFIRM_THRESHOLD", "30"))
        except Exception:
            confirm_threshold = 30
        if confirm_threshold > 0 and quantite >= confirm_threshold:
            confirm_view = _BigBatchConfirmView(timeout=30.0)
            confirm_msg = await interaction.followup.send(
                f"⚠️ Tu demandes **{quantite} vidéos**. C'est un gros batch.\n"
                f"📦 Modèle : `ID{modele}` · 👤 Compte : `@{clean_username}`\n\n"
                f"Confirme dans 30s sinon la demande s'annule.",
                view=confirm_view,
                ephemeral=True,
            )
            await confirm_view.wait()
            if confirm_view.confirmed is None:
                try:
                    await confirm_msg.edit(
                        content=f"⏱️ Confirmation timeout (30s). Batch de **{quantite}** vidéos annulé.",
                        view=None,
                    )
                except Exception:
                    pass
                return
            if not confirm_view.confirmed:
                try:
                    await confirm_msg.edit(
                        content=f"❌ Batch de **{quantite}** vidéos annulé.",
                        view=None,
                    )
                except Exception:
                    pass
                return
            try:
                await confirm_msg.edit(
                    content=f"✅ Confirmé, lancement de **{quantite}** vidéos...",
                    view=None,
                )
            except Exception:
                pass

        # === CHECKS DB EN PARALLÈLE (gain ~500ms sur Railway) ===
        # 4 lectures DB indépendantes (email, model, rate limit, account existant).
        # En série chaque appel ≈ 100-300ms sur Railway → 4 = 400-1200ms.
        # En parallèle = max(individuels) ≈ 100-300ms. Gros gain de réactivité UX.
        is_admin_user = _is_admin(interaction.user)
        user_id_str = str(interaction.user.id)
        va_name_for_check = interaction.user.display_name or interaction.user.name

        async def _email_check_async():
            if is_admin_user:
                return ""  # admin bypass, valeur ignorée downstream
            try:
                from app.services import va_emails_db as _veb_req
                all_emails = await asyncio.to_thread(_veb_req.load_all_emails)
                return (all_emails or {}).get(user_id_str, "") or ""
            except Exception:
                return ""

        async def _model_lookup_async():
            try:
                return await asyncio.to_thread(cf_storage.get_model_by_label_number, modele)
            except Exception:
                return None

        async def _rate_limit_async():
            if is_admin_user:
                return (True, 0, 0, 0)  # admin bypass
            try:
                return await asyncio.to_thread(_check_rate_limit, va_name_for_check, quantite)
            except Exception:
                return (True, 0, 0, 0)  # fail-open : si DB foire on laisse passer

        async def _account_lookup_async():
            try:
                return await asyncio.to_thread(cf_storage.find_account_any_model, clean_username)
            except Exception:
                return None

        va_email_req, model, rate_result, existing = await asyncio.gather(
            _email_check_async(),
            _model_lookup_async(),
            _rate_limit_async(),
            _account_lookup_async(),
        )

        # === TRAITEMENT DES RÉSULTATS DANS L'ORDRE ===

        # 2.5. CHECK EMAIL VA (obligatoire pour partager le Drive auto)
        if not is_admin_user and not va_email_req:
            await interaction.followup.send(
                "❌ **Tu n'as pas encore enregistré ton email.**\n\n"
                "Pour utiliser `/request`, tu dois d'abord poster ton adresse Gmail "
                "dans le canal **#email-drive**.\n\n"
                "Une fois ton email enregistré, ton drive te sera automatiquement "
                "partagé sur cette adresse. 📧",
                ephemeral=True,
            )
            return

        # 4. Validation modèle — LOOKUP PAR LABEL (pas par DB id)
        # Les VAs voient des labels "ID1", "ID8" etc. sur le site. Les IDs DB
        # peuvent être décalés (suppressions = trous dans l'auto-increment), donc
        # on cherche le modèle dont le label contient le numéro tapé, pas l'id DB.
        if not model:
            available = await asyncio.to_thread(cf_storage.list_models)
            if available:
                lst = ", ".join(f"`{m['label']}`" for m in available[:20])
                msg = f"❌ Modèle **ID{modele}** introuvable.\nDispos : {lst}"
            else:
                msg = f"❌ Modèle **ID{modele}** introuvable.\nAucun modèle créé pour l'instant. Demande à l'admin."
            await interaction.followup.send(msg, ephemeral=True)
            return

        # À partir d'ici, on remplace `modele` (numéro VA) par le VRAI DB id.
        # Comme ça toutes les lookups downstream (vidéos, comptes, request)
        # tapent dans la bonne entrée DB, peu importe le décalage label↔id.
        modele = int(model["id"])

        # 4b. RATE LIMIT (anti-abus) — bypass admin
        if not is_admin_user:
            allowed, used, limit, remaining = rate_result
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
                await interaction.followup.send(msg, ephemeral=True)
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

        # Résolution du compte (déjà fetché via gather plus haut → `existing`)
        # Cas possibles :
        # - Compte n'existe pas → création auto (device + GPS US random lockés)
        # - Compte existe ET appartient au VA → utilisation
        # - Compte existe MAIS appartient à un AUTRE VA → REFUS (anti-vol de compte)
        account_data = None
        account_msg_extra = ""
        is_new_account = False

        if existing:
            # Compte archivé (Lola webhook va_banned ou archivage manuel) →
            # interdit même à l'owner. Admin doit le restaurer en DB s'il veut le réutiliser.
            if existing.get("archived_at"):
                reason = existing.get("archive_reason") or "raison inconnue"
                await interaction.followup.send(
                    f"🚫 Le compte **@{clean_username}** a été **archivé** "
                    f"(`{reason}`).\n"
                    f"Demande à un admin de le restaurer si besoin.",
                    ephemeral=True,
                )
                return
            # Compte déjà créé : vérifier la propriété (sauf admin qui peut tout)
            owner_id = str(existing.get("va_discord_id", "") or "")
            if owner_id and owner_id != user_id_str and not is_admin_user:
                # Trouver le nom du vrai propriétaire pour le message d'erreur
                owner_name = existing.get("va_name", "un autre VA")
                await interaction.followup.send(
                    f"❌ Le compte **@{clean_username}** appartient déjà à **{owner_name}**.\n"
                    f"Si c'est une erreur, demande à un admin de le réassigner.",
                    ephemeral=True,
                )
                return
            account_data = existing
            account_msg_extra = (
                f"\n🔒 Compte connu : @**{account_data['username']}** "
                f"({account_data['device_choice'].replace('_', ' ').title()} · {account_data['gps_city']})"
            )
        else:
            # Création auto : random device + GPS US, lockés à vie sur ce compte.
            # asyncio.to_thread parce que c'est un INSERT DB synchrone.
            is_new_account = True
            try:
                account_data = await asyncio.to_thread(
                    cf_storage.create_account,
                    username=clean_username,
                    model_id=modele,
                    va_discord_id=user_id_str,
                    va_name=interaction.user.display_name or interaction.user.name,
                )
            except Exception as _create_err:
                logger.error(f"create_account failed: {_create_err}")
                account_data = None
            if not account_data:
                await interaction.followup.send(
                    f"❌ Impossible de créer le compte @{clean_username}. Réessaie ou ping un admin.",
                    ephemeral=True,
                )
                return
            account_msg_extra = (
                f"\n✨ Nouveau compte créé : @**{account_data['username']}**\n"
                f"📱 Device : **{account_data['device_choice'].replace('_', ' ').title()}**\n"
                f"📍 GPS : **{account_data['gps_city']}**"
            )

        # 4c. INTERVALLE MIN entre batchs sur le même compte (anti-pattern non humain)
        # Variable d'env CF_MIN_INTERVAL_HOURS_PER_ACCOUNT (default 6h)
        # Si 0, désactivé. Bypass admin (comme rate limit).
        if account_data and not is_admin_user and not is_new_account:
            try:
                min_interval_h = int(os.environ.get("CF_MIN_INTERVAL_HOURS_PER_ACCOUNT", "6"))
            except Exception:
                min_interval_h = 6
            if min_interval_h > 0:
                last_batch_time = cf_storage.get_last_batch_time_for_account(account_data["username"])
                if last_batch_time:
                    from datetime import datetime, timezone, timedelta
                    now_utc = datetime.now(timezone.utc)
                    # Si last_batch_time est naive, on le considère en UTC
                    if last_batch_time.tzinfo is None:
                        last_batch_time = last_batch_time.replace(tzinfo=timezone.utc)
                    elapsed = now_utc - last_batch_time
                    if elapsed < timedelta(hours=min_interval_h):
                        wait = timedelta(hours=min_interval_h) - elapsed
                        h = int(wait.total_seconds() // 3600)
                        m = int((wait.total_seconds() % 3600) // 60)
                        wait_str = f"{h}h{m:02d}" if h > 0 else f"{m} min"
                        await interaction.followup.send(
                            f"⏰ **Intervalle min non respecté pour @{account_data['username']}**\n"
                            f"Le dernier batch sur ce compte a eu lieu il y a moins de **{min_interval_h}h**.\n"
                            f"Reviens dans **{wait_str}** pour que ce compte ne paraisse pas suspect.\n"
                            f"_(anti-pattern non humain : poster 60 vidéos toutes les 30 min = drapeau rouge)_",
                            ephemeral=True,
                        )
                        return

        # Détermine la timezone selon la team (geelark = Bénin par défaut)
        # On peut surcharger via env var CF_TZ_GEELARK / CF_TZ_INSTAGRAM
        tz_name = os.environ.get(f"CF_TZ_{team.upper()}", "benin").lower().strip()
        if tz_name not in ("benin", "madagascar"):
            tz_name = "benin"

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
            account=account_data,
            tz_name=tz_name,
        )
        _pending.append(req)
        await _queue.put(req)

        # 6. Réponse initiale dans le canal (s'auto-supprime après CF_CHANNEL_MSG_TTL = 30s)
        position = len(_pending) + (1 if _current and _current is not req else 0)
        # Bloc info compte (device + GPS + état) — visible direct dans le canal,
        # comme ça le VA voit quel compte il a hit AVANT d'ouvrir son DM.
        # account_msg_extra a été construit plus haut selon : compte connu OU nouveau.
        if position <= 1 and _current is None:
            ack = (
                f"⚙️ Demande acceptée : **{quantite}** vidéos pour modèle **{model['label']}** · équipe **{team}**"
                f"{account_msg_extra}\n"
                f"📩 <@{interaction.user.id}> tu as reçu un DM avec les détails. Lancement immédiat..."
            )
        else:
            ack = (
                f"⏳ Demande acceptée : **{quantite}** vidéos pour modèle **{model['label']}** · équipe **{team}**"
                f"{account_msg_extra}\n"
                f"📩 <@{interaction.user.id}> tu as reçu un DM avec les détails. Position dans la file : **{position}**"
            )
        ttl = _channel_msg_ttl()
        # On a defer en haut → faut envoyer un followup. followup.send retourne
        # la message en discord.py 2.x, on programme la suppression via task async.
        sent_msg = None
        try:
            sent_msg = await interaction.followup.send(ack, ephemeral=False)
        except Exception as _send_err:
            logger.warning(f"followup.send ack failed: {_send_err}")
            sent_msg = None

        if sent_msg is not None and ttl > 0:
            async def _auto_delete_ack(msg=sent_msg, delay=float(ttl)):
                try:
                    await asyncio.sleep(delay)
                    await msg.delete()
                except Exception:
                    pass
            asyncio.create_task(_auto_delete_ack())

        # 7. DM "en préparation" envoyé immédiatement au VA avec l'ETA (en embed)
        try:
            user = await interaction.client.fetch_user(interaction.user.id)

            embed = discord.Embed(
                title="🎬 Ta demande est en préparation !",
                color=0xF39C12,  # orange "en cours"
            )
            embed.add_field(
                name="📊 Demande",
                value=(
                    f"**{quantite}** vidéos · modèle **{model['label']}** · "
                    f"équipe **{team}**"
                ),
                inline=False,
            )

            if account_data:
                tz_label = "Bénin GMT+1" if tz_name == "benin" else "Madagascar GMT+3"
                embed.add_field(
                    name="📍 Compte",
                    value=(
                        f"@**{account_data['username']}**\n"
                        f"📱 {account_data['device_choice'].replace('_', ' ').title()}\n"
                        f"🌍 {account_data['gps_city']}"
                    ),
                    inline=True,
                )
                n_per = quantite // 3
                extra = quantite % 3
                n_matin = n_per + extra
                n_soir = n_per
                n_nuit = n_per
                embed.add_field(
                    name=f"📦 Répartition ({tz_label})",
                    value=(
                        f"🌅 **{n_matin}** matin (8h-9h)\n"
                        f"🌇 **{n_soir}** soir (16h-17h)\n"
                        f"🌙 **{n_nuit}** nuit (22h-23h)"
                    ),
                    inline=True,
                )

            eta_val = f"~**{eta_str}**"
            if position > 1:
                eta_val += f"\n⏳ Position dans la file : **{position}**"
            embed.add_field(name="⏱️ ETA", value=eta_val, inline=False)
            embed.set_footer(
                text="Tu recevras un DM dès que le Drive est prêt 🚀"
            )

            await user.send(embed=embed)
        except discord.Forbidden:
            logger.warning(f"DM 'en préparation' refusé pour user {interaction.user.id} (DMs désactivés)")
        except Exception as e:
            logger.warning(f"DM 'en préparation' échoué pour user {interaction.user.id}: {e}")

        # Démarre le worker s'il ne tourne pas
        global _worker_task
        if _worker_task is None or _worker_task.done():
            _worker_task = asyncio.create_task(_worker_loop())

    # Autocomplete pour le paramètre `compte` : propose UNIQUEMENT les comptes
    # que ce VA précis a déjà utilisés pour le modèle sélectionné.
    # Chaque VA voit ses propres comptes, JAMAIS ceux des autres (anti-confusion).
    @request_cmd.autocomplete("compte")
    async def compte_autocomplete(
        interaction: "discord.Interaction",
        current: str,
    ) -> List[app_commands.Choice[str]]:
        try:
            user_id_str = str(interaction.user.id)

            # Récupère le model_id depuis les options déjà saisies
            modele_value = None
            for opt in (interaction.data.get("options") or []):
                if opt.get("name") == "modele":
                    modele_value = opt.get("value")
                    break

            # Liste TOUS les comptes du VA (filtre strict par va_discord_id)
            # Si modele est fourni, on filtre aussi par modèle pour réduire la liste
            accounts = cf_storage.list_accounts(va_discord_id=user_id_str)
            if modele_value:
                try:
                    # Résout le numéro tapé (VA voit le label sur le site) vers le
                    # VRAI DB id du modèle, comme dans la commande principale.
                    typed_n = int(modele_value)
                    resolved = cf_storage.get_model_by_label_number(typed_n)
                    mid = int(resolved["id"]) if resolved else typed_n
                    accounts = [a for a in accounts if int(a.get("model_id", 0)) == mid]
                except (ValueError, TypeError):
                    pass

            # Filtre par texte tapé par le user
            cur_lower = (current or "").strip().lower().lstrip("@")
            filtered = [
                a for a in accounts
                if cur_lower in a.get("username", "").lower()
            ]

            # Discord limit : max 25 choix dans un autocomplete
            return [
                app_commands.Choice(
                    name=f"@{a['username']} ({a['device_choice'].replace('_', ' ')[:18]})",
                    value=a["username"],
                )
                for a in filtered[:25]
            ]
        except Exception as e:
            logger.warning(f"compte autocomplete failed: {e}")
            return []

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

        # Filtre selon les rôles du VA.
        # _allowed_models_for_member retourne :
        # - Pour admins : tous les DB ids (vue complète)
        # - Pour VAs : les numéros tirés des rôles "IDX" (user-facing, match
        #   les labels qu'ils voient sur le site, pas les DB ids)
        # Pour les VAs, on résout chaque numéro vers son DB id réel via label
        # (sinon rôle "ID3" cherche DB id 3 qui peut être un trou et le VA
        # se retrouve sans modèle alors qu'il devrait voir celui labelé "ID 3").
        my_numbers = set(_allowed_models_for_member(interaction.user))
        is_admin_user = False
        try:
            is_admin_user = bool(interaction.user.guild_permissions.administrator)
        except Exception:
            pass

        if is_admin_user:
            my_db_ids = my_numbers
        else:
            my_db_ids = set()
            for num in my_numbers:
                resolved = cf_storage.get_model_by_label_number(num)
                if resolved:
                    my_db_ids.add(int(resolved["id"]))

        my_models = [m for m in all_models if m["id"] in my_db_ids]

        if not my_models:
            await interaction.response.send_message(
                "❌ Tu n'as accès à aucun modèle.\nDemande à un admin de t'attribuer un rôle `IDX` (ex: `ID1`, `ID2`).",
                ephemeral=True,
            )
            return

        lines = []
        for m in my_models:
            n_videos = len(cf_storage.list_videos(model_id=m["id"]))
            # Affiche juste le label (= ce que le VA voit sur le site),
            # pas le DB id qui peut être décalé et créer de la confusion.
            lines.append(f"• **{m['label']}** ({n_videos} vidéos)")
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

    @bot.tree.command(
        name="help",
        description="Liste toutes les commandes ClipFusion dispos pour toi",
    )
    async def help_cmd(interaction: "discord.Interaction"):
        is_admin_user = _is_admin(interaction.user)
        # Embed coloré bleu pour démarquer
        embed = discord.Embed(
            title="📚 Commandes ClipFusion",
            description="Voici les commandes disponibles pour toi :",
            color=0x3498DB,
        )

        # --- VA commands (toujours visibles) ---
        embed.add_field(
            name="🎬 /request `quantite` `modele` `compte`",
            value=(
                "Génère N variantes vidéo pour un compte Insta.\n"
                "Ex : `/request quantite:6 modele:3 compte:sara`"
            ),
            inline=False,
        )
        embed.add_field(
            name="🔁 /respoof `fichier` `compte` `fenetre`",
            value=(
                "Respoof une seule photo/vidéo existante (device + GPS du compte).\n"
                "Ex : `/respoof fichier:[upload] compte:sara fenetre:matin`"
            ),
            inline=False,
        )
        embed.add_field(
            name="📋 /models",
            value="Liste les modèles (ID) auxquels tu as accès.",
            inline=False,
        )
        embed.add_field(
            name="📊 /queue",
            value="Voir tes batchs en cours / en attente avec ETA.",
            inline=False,
        )
        embed.add_field(
            name="❌ /cancel",
            value="Annule tes batchs en attente (pas ceux déjà en cours FFmpeg).",
            inline=False,
        )
        embed.add_field(
            name="📈 /status",
            value="État global de la file d'attente ClipFusion.",
            inline=False,
        )

        # --- Admin commands ---
        if is_admin_user:
            embed.add_field(
                name="🔐 Commandes admin",
                value=(
                    "**/admin_account_info `compte`** — debug compte (owner, stats, history)\n"
                    "**/stats `jours:7`** — envoie le récap des N derniers jours au webhook admin"
                ),
                inline=False,
            )

        embed.set_footer(
            text="Pense à enregistrer ton Gmail dans #email-drive avant d'utiliser /request"
        )

        await interaction.response.send_message(embed=embed, ephemeral=True)

    @bot.tree.command(
        name="queue",
        description="Voir tes batchs en cours/en attente avec ETA",
    )
    async def queue_cmd(interaction: "discord.Interaction"):
        user_id = interaction.user.id
        my_pending = [(i, p) for i, p in enumerate(_pending) if p.user_id == user_id]
        is_current_mine = bool(_current and _current.user_id == user_id)

        if not my_pending and not is_current_mine:
            await interaction.response.send_message(
                "✅ Tu n'as aucun batch en cours ou en attente.",
                ephemeral=True,
            )
            return

        spv = _seconds_per_video()
        lines = ["**📊 Tes batchs ClipFusion :**"]
        if is_current_mine and _current:
            lines.append(f"⚙️ **En cours** : {_current.quantite} vidéos")
        for idx, req in my_pending:
            ahead_videos = sum(p.quantite for p in _pending[:idx])
            if _current:
                ahead_videos += _current.quantite // 2
            eta_str = _format_eta(ahead_videos * spv)
            lines.append(
                f"⏳ **Position {idx+1}** · {req.quantite} vidéos · ETA ~{eta_str}"
            )
        await interaction.response.send_message("\n".join(lines), ephemeral=True)

    @bot.tree.command(
        name="admin_account_info",
        description="[Admin] Infos détaillées + stats sur un compte Insta",
    )
    @app_commands.describe(compte="Username du compte (sans @)")
    async def admin_account_info_cmd(
        interaction: "discord.Interaction",
        compte: str,
    ):
        if not _is_admin(interaction.user):
            await interaction.response.send_message(
                "❌ Cette commande est réservée aux admins.",
                ephemeral=True,
            )
            return
        clean = (compte or "").strip().lstrip("@").strip()
        if not clean:
            await interaction.response.send_message(
                "❌ Username invalide.", ephemeral=True
            )
            return

        await interaction.response.defer(ephemeral=True, thinking=True)
        try:
            stats = await asyncio.to_thread(cf_storage.get_account_stats, clean)
        except Exception as e:
            await interaction.followup.send(
                f"❌ Erreur : `{type(e).__name__}: {str(e)[:200]}`",
                ephemeral=True,
            )
            return
        if not stats:
            await interaction.followup.send(
                f"❌ Compte **@{clean}** introuvable.",
                ephemeral=True,
            )
            return

        acc = stats["account"]
        success_pct = stats.get("success_rate_pct")
        success_str = f"{success_pct}%" if success_pct is not None else "N/A"
        created_at = (acc.get("created_at") or "")[:10]
        last_batch = stats.get("last_batch_at") or "jamais"
        if last_batch and last_batch != "jamais":
            last_batch = last_batch[:16].replace("T", " ")

        msg = (
            f"**📋 @{acc['username']}**\n"
            f"• Owner : **{acc.get('va_name') or '?'}** "
            f"(<@{acc.get('va_discord_id') or '0'}>)\n"
            f"• Device : `{acc.get('device_choice', '?')}` "
            f"(iOS `{acc.get('ios_version') or '?'}`)\n"
            f"• GPS : **{acc.get('gps_city', '?')}** "
            f"({acc.get('gps_lat', 0):.4f}, {acc.get('gps_lng', 0):.4f})\n"
            f"• Model DB id : `{acc.get('model_id', '?')}`\n"
            f"• Compte créé le : `{created_at}`\n\n"
            f"**📊 Stats** :\n"
            f"• **{stats['total_batches']}** batchs · "
            f"**{stats['total_uploaded']}/{stats['total_requested']}** vidéos "
            f"({success_str} succès)\n"
            f"• Dernier batch : `{last_batch}`\n"
            f"• Durée moyenne : `{stats['avg_duration_seconds']:.0f}s`\n"
        )
        recent = stats.get("recent_batches") or []
        if recent:
            msg += "\n**🕐 5 derniers batchs :**\n"
            for b in recent:
                date = (b.get("created_at") or "?")[:16].replace("T", " ")
                msg += (
                    f"• `{date}` · {b['videos_uploaded']}/{b['videos_count']} "
                    f"({b['duration_seconds']:.0f}s)\n"
                )

        await interaction.followup.send(msg, ephemeral=True)

    @bot.tree.command(
        name="cancel",
        description="Annule tes batchs ClipFusion en attente (pas ceux déjà en cours)",
    )
    async def cancel_cmd(interaction: "discord.Interaction"):
        # Cherche les batchs pending de ce VA
        my_pending = [p for p in _pending if p.user_id == interaction.user.id]

        if not my_pending:
            # Check si batch en cours est le sien (on PEUT pas l'annuler mais on prévient)
            if _current and _current.user_id == interaction.user.id:
                await interaction.response.send_message(
                    f"⚠️ Tu as un batch **en cours de traitement** "
                    f"(`{_current.quantite}` vidéos sur modèle DB id `{_current.model_id}`).\n"
                    f"On peut pas l'annuler une fois que FFmpeg a démarré (les vidéos sont déjà "
                    f"partiellement encodées). Attends qu'il se termine.",
                    ephemeral=True,
                )
            else:
                await interaction.response.send_message(
                    "✅ Tu n'as aucun batch en attente. Rien à annuler.",
                    ephemeral=True,
                )
            return

        # Marque tous les pending de ce VA comme annulés
        total_quantite = 0
        for req in my_pending:
            _cancelled_ids.add(req.interaction_id)
            try:
                _pending.remove(req)
            except ValueError:
                pass
            total_quantite += req.quantite

        n = len(my_pending)
        s = "s" if n > 1 else ""
        await interaction.response.send_message(
            f"✅ **{n} batch{s} annulé{s}** ({total_quantite} vidéos en moins).\n"
            f"Si un batch est déjà en cours de traitement, il continue (pas annulable une fois "
            f"que FFmpeg a démarré).",
            ephemeral=True,
        )
        logger.info(
            f"/cancel par {interaction.user.name} ({interaction.user.id}) : "
            f"{n} batchs ({total_quantite} vidéos) annulés"
        )

    @bot.tree.command(
        name="stats",
        description="[Admin] Envoie le récap hebdo des batchs ClipFusion dans le webhook admin",
    )
    @app_commands.describe(
        jours="Période en jours (défaut 7). Ex: 30 pour le mois.",
    )
    async def stats_cmd(
        interaction: "discord.Interaction",
        jours: int = 7,
    ):
        # Admin-only
        if not _is_admin(interaction.user):
            await interaction.response.send_message(
                "❌ Cette commande est réservée aux admins.",
                ephemeral=True,
            )
            return
        if jours < 1 or jours > 365:
            await interaction.response.send_message(
                "❌ Période invalide (1-365 jours).",
                ephemeral=True,
            )
            return

        # Defer parce que query DB + envoi webhook
        await interaction.response.defer(ephemeral=True, thinking=True)

        try:
            from app.services import cf_weekly_stats
            from app.services.discord_service import is_admin_webhook_enabled
            if not is_admin_webhook_enabled():
                await interaction.followup.send(
                    "❌ `DISCORD_ADMIN_WEBHOOK_URL` n'est pas configuré côté Railway.\n"
                    "Ajoute la variable pour recevoir les stats dans ton canal admin.",
                    ephemeral=True,
                )
                return

            sent = await cf_weekly_stats.send_weekly_stats(days=jours)
            if sent:
                await interaction.followup.send(
                    f"✅ Stats des {jours} derniers jours envoyées dans le canal admin.",
                    ephemeral=True,
                )
            else:
                await interaction.followup.send(
                    f"⚠️ L'envoi a échoué (check les logs Railway).",
                    ephemeral=True,
                )
        except Exception as e:
            logger.exception(f"/stats error: {e}")
            await interaction.followup.send(
                f"❌ Erreur : `{type(e).__name__}: {str(e)[:200]}`",
                ephemeral=True,
            )

    @bot.tree.command(
        name="respoof",
        description="Respoof une photo/vidéo existante avec le device + GPS lockés d'un compte",
    )
    @app_commands.describe(
        fichier="Le fichier (photo .jpg/.png/.heic OU vidéo .mp4/.mov) à respoofer",
        compte="Username du compte Insta (sans @). Le device + GPS du compte sont appliqués.",
        fenetre="Fenêtre horaire pour les metadata (matin/soir/nuit). Cohérent avec ton heure de post.",
    )
    @app_commands.choices(fenetre=[
        app_commands.Choice(name="🌅 Matin (8h-9h)", value="matin"),
        app_commands.Choice(name="🌆 Soir (16h-17h)", value="soir"),
        app_commands.Choice(name="🌙 Nuit (22h-23h)", value="nuit"),
    ])
    async def respoof_cmd(
        interaction: "discord.Interaction",
        fichier: discord.Attachment,
        compte: str,
        fenetre: app_commands.Choice[str],
    ):
        from app.services import cf_respoof, cf_storage, drive_service

        # 0. Filtrage canal : /respoof uniquement dans les canaux dédiés
        respoof_channels = _get_respoof_channel_ids()
        if respoof_channels and interaction.channel_id not in respoof_channels:
            await interaction.response.send_message(
                f"❌ La commande `/respoof` est dispo uniquement dans le canal dédié au spoof rapide.\n"
                f"Pour générer un batch complet (avec captions), utilise `/request`.",
                ephemeral=True,
            )
            return

        # 1. Détection auto du type
        file_type = cf_respoof.detect_file_type(fichier.filename)
        if file_type == "unknown":
            await interaction.response.send_message(
                f"❌ Format non supporté pour `{fichier.filename}`.\n"
                f"Photos : .jpg / .jpeg / .png / .heic / .heif / .webp\n"
                f"Vidéos : .mp4 / .mov / .m4v / .avi / .webm / .mkv",
                ephemeral=True,
            )
            return

        # 2. Validation compte
        clean_username = compte.strip().lstrip("@").strip()
        if not clean_username:
            await interaction.response.send_message(
                "❌ Le nom du compte est invalide.",
                ephemeral=True,
            )
            return

        # 2.5. CHECK EMAIL VA (obligatoire pour pouvoir partager le Drive)
        # Les admins sont bypass (ils ont déjà accès au Drive complet)
        is_admin_check = _is_admin(interaction.user)
        if not is_admin_check:
            user_id_for_check = str(interaction.user.id)
            try:
                from app.services import va_emails_db
                all_emails = va_emails_db.load_all_emails() or {}
                va_email_check = all_emails.get(user_id_for_check, "") or ""
            except Exception:
                va_email_check = ""

            if not va_email_check:
                await interaction.response.send_message(
                    "❌ **Tu n'as pas encore enregistré ton email.**\n\n"
                    "Pour utiliser `/respoof`, tu dois d'abord poster ton adresse Gmail "
                    "dans le canal **#email-drive**.\n\n"
                    "Une fois ton email enregistré, ton drive te sera automatiquement "
                    "partagé sur cette adresse. 📧",
                    ephemeral=True,
                )
                return

        # 3. DEFER IMMÉDIATEMENT pour éviter le timeout Discord (3s max sans defer)
        # On a max 15 minutes après le defer pour répondre via followup.
        await interaction.response.defer(ephemeral=False, thinking=True)

        # 4. Résolution du compte + modèle
        # - Si compte existe → on reprend son model_id
        # - Si compte n'existe pas → on prend le 1er rôle ID{X} du VA pour créer
        is_admin_user = _is_admin(interaction.user)
        user_id_str = str(interaction.user.id)
        existing = cf_storage.find_account_any_model(clean_username)

        is_new_account = False
        modele = None

        if existing:
            # Compte existe : check propriété + reprendre son model_id
            owner_id = str(existing.get("va_discord_id", "") or "")
            if owner_id and owner_id != user_id_str and not is_admin_user:
                owner_name = existing.get("va_name", "un autre VA")
                await interaction.followup.send(
                    f"❌ Le compte **@{clean_username}** appartient déjà à **{owner_name}**.",
                    ephemeral=True,
                )
                return
            account_data = existing
            modele = int(existing.get("model_id", 0))
        else:
            # Nouveau compte : déduit le modèle depuis les rôles ID{X} du VA
            allowed_ids = _allowed_models_for_member(interaction.user)
            if not allowed_ids:
                await interaction.followup.send(
                    f"❌ Tu n'as aucun rôle `ID{{N}}` sur ce serveur, donc impossible de créer "
                    f"le compte **@{clean_username}**.\n"
                    f"Demande à un admin de t'attribuer un rôle `ID1`, `ID2`, etc.",
                    ephemeral=True,
                )
                return
            # On prend le PLUS PETIT id (souvent le rôle principal du VA)
            # Le numéro vient du nom du rôle Discord ("IDX") → c'est un numéro
            # user-facing, pas un DB id. On le résout vers le vrai DB id via label,
            # comme dans /request, pour pas créer le compte sous un id décalé.
            typed_n = min(allowed_ids)
            _resolved = cf_storage.get_model_by_label_number(typed_n)
            modele = int(_resolved["id"]) if _resolved else typed_n
            is_new_account = True
            account_data = cf_storage.create_account(
                username=clean_username,
                model_id=modele,
                va_discord_id=user_id_str,
                va_name=interaction.user.display_name or interaction.user.name,
            )
            if not account_data:
                await interaction.followup.send(
                    f"❌ Impossible de créer le compte @{clean_username}.",
                    ephemeral=True,
                )
                return

        # Récupère les infos du modèle pour les logs / DM
        model = cf_storage.get_model(modele) or {"id": modele, "label": f"ID{modele}"}

        # 4. Rate limit (compte 1 vidéo dans le quota)
        if not is_admin_user:
            max_v, period_days = _rate_limit_config()
            if max_v > 0:
                used = cf_storage.count_va_videos_recent(
                    interaction.user.display_name or interaction.user.name,
                    days=period_days,
                )
                if used + 1 > max_v:
                    await interaction.followup.send(
                        f"⛔ Quota dépassé : {used}/{max_v} vidéos sur {period_days}j.",
                        ephemeral=True,
                    )
                    return

        # 5. Intervalle min entre batchs sur le même compte (6h par défaut)
        if not is_admin_user and not is_new_account:
            try:
                min_interval_h = int(os.environ.get("CF_MIN_INTERVAL_HOURS_PER_ACCOUNT", "6"))
            except Exception:
                min_interval_h = 6
            if min_interval_h > 0:
                last_batch_time = cf_storage.get_last_batch_time_for_account(account_data["username"])
                if last_batch_time:
                    from datetime import datetime as _dt, timezone as _tz, timedelta as _td
                    now_utc = _dt.now(_tz.utc)
                    if last_batch_time.tzinfo is None:
                        last_batch_time = last_batch_time.replace(tzinfo=_tz.utc)
                    elapsed = now_utc - last_batch_time
                    if elapsed < _td(hours=min_interval_h):
                        wait = _td(hours=min_interval_h) - elapsed
                        h = int(wait.total_seconds() // 3600)
                        m = int((wait.total_seconds() % 3600) // 60)
                        wait_str = f"{h}h{m:02d}" if h > 0 else f"{m} min"
                        await interaction.followup.send(
                            f"⏰ Intervalle min non respecté pour @{account_data['username']}.\n"
                            f"Reviens dans **{wait_str}**.",
                            ephemeral=True,
                        )
                        return

        # 6. Download du fichier depuis Discord (déjà déféré plus haut)
        # MEM FIX : pour les vidéos, on stream vers disque pour ne pas charger
        # 50-100 MB en RAM. Seules les photos (petites) sont chargées en mémoire
        # (car cf_respoof.respoof_photo prend input_bytes en paramètre).
        tmpdir = Path(tempfile.gettempdir()) / "cf_respoof"
        tmpdir.mkdir(parents=True, exist_ok=True)
        in_ext = Path(fichier.filename).suffix.lower() or ".bin"
        tmp_in = tmpdir / f"respoof_in_{uuid.uuid4().hex}{in_ext}"

        file_bytes = b""  # rempli seulement si photo
        try:
            if file_type == "photo":
                # Photos : on garde en RAM (petits fichiers, et l'API respoof_photo en a besoin)
                file_bytes = await fichier.read()
                tmp_in.write_bytes(file_bytes)
            else:
                # Vidéos : stream direct vers disque (pas de RAM)
                await fichier.save(str(tmp_in))
        except Exception as e:
            await interaction.followup.send(
                f"❌ Impossible de lire le fichier : {e}",
                ephemeral=True,
            )
            try:
                tmp_in.unlink(missing_ok=True)
            except Exception:
                pass
            return

        # 9. Détermine team / tz / target_hour (depuis la fenêtre choisie par le VA)
        team = _detect_team_from_guild(interaction.guild_id if interaction.guild else None)
        tz_name = os.environ.get(f"CF_TZ_{team.upper()}", "benin").lower().strip()
        if tz_name not in ("benin", "madagascar"):
            tz_name = "benin"
        # Fenêtre horaire selon le choix du VA (matin / soir / nuit)
        _FENETRE_HOURS = {"matin": 9, "soir": 17, "nuit": 23}
        fenetre_value = fenetre.value if hasattr(fenetre, "value") else str(fenetre)
        target_hour = _FENETRE_HOURS.get(fenetre_value, 9)
        fenetre_label = {
            "matin": "🌅 Matin (8h-9h)",
            "soir": "🌆 Soir (16h-17h)",
            "nuit": "🌙 Nuit (22h-23h)",
        }.get(fenetre_value, fenetre_value)

        # 10. Respoof
        try:
            if file_type == "photo":
                # Photo : retourne bytes
                spoofed_bytes, info = cf_respoof.respoof_photo(
                    input_bytes=file_bytes,
                    filename=fichier.filename,
                    account=account_data,
                    target_hour=target_hour,
                    tz_name=tz_name,
                )
                # Libère file_bytes immédiatement après usage
                del file_bytes
                file_bytes = b""
                # Sauvegarde local
                out_name = f"respoof_{clean_username}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.jpg"
                tmp_out = tmpdir / out_name
                tmp_out.write_bytes(spoofed_bytes)
                # Libère spoofed_bytes après écriture disque
                del spoofed_bytes
            else:
                # Vidéo : transcode in-place (file_bytes pas utilisé pour vidéo)
                out_name = f"respoof_{clean_username}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.mp4"
                tmp_out = tmpdir / out_name
                info = cf_respoof.respoof_video(
                    input_path=str(tmp_in),
                    output_path=str(tmp_out),
                    account=account_data,
                    target_hour=target_hour,
                    tz_name=tz_name,
                )
        except Exception as e:
            logger.error(f"Respoof failed: {e}", exc_info=True)
            await interaction.followup.send(
                f"❌ Erreur pendant le respoof : `{e}`",
                ephemeral=True,
            )
            return
        finally:
            try:
                tmp_in.unlink(missing_ok=True)
            except Exception:
                pass

        # 11. Upload sur Drive (dans un dossier au nom du compte)
        folder_id = ""
        folder_url = ""
        va_email = ""
        try:
            folder_name = (
                f"{clean_username}_respoof_"
                f"{(interaction.user.display_name or interaction.user.name)}_"
                f"{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            )
            folder_id = drive_service.create_batch_folder(folder_name) or ""
            folder_url = drive_service.get_folder_link(folder_id) if folder_id else ""

            # Mime type selon le type de fichier
            mime = "image/jpeg" if file_type == "photo" else "video/mp4"
            up = drive_service.upload_file(
                local_path=tmp_out,
                folder_id=folder_id,
                mime_type=mime,
            )

            # Partage avec le VA si email connu
            try:
                from app.services import va_emails_db
                all_emails = va_emails_db.load_all_emails() or {}
                va_email = all_emails.get(user_id_str, "") or ""
            except Exception:
                pass
            if va_email and folder_id:
                try:
                    drive_service.share_folder_with_users(folder_id, [va_email])
                except Exception as e:
                    logger.warning(f"Drive share failed: {e}")
        except Exception as e:
            logger.error(f"Drive upload failed: {e}", exc_info=True)
            await interaction.followup.send(
                f"❌ Erreur Drive : `{e}`",
                ephemeral=True,
            )
            return
        finally:
            try:
                tmp_out.unlink(missing_ok=True)
            except Exception:
                pass

        # 12. Save en historique (compte pour rate limit)
        try:
            cf_storage.add_batch(
                va_name=interaction.user.display_name or interaction.user.name,
                team=team,
                device_choice=account_data.get("device_choice", ""),
                videos_count=1,
                videos_uploaded=1,
                drive_folder_id=folder_id,
                drive_folder_url=folder_url,
                drive_folder_name=folder_name,
                va_email=va_email,
                discord_notified=True,
                duration_seconds=0,
                model_id=modele,
                model_label=model.get("label", ""),
                account_username=clean_username,
            )
        except Exception as e:
            logger.warning(f"add_batch (respoof) failed: {e}")

        # 13. Réponse au VA (canal + DM)
        type_emoji = "📷" if file_type == "photo" else "🎥"
        type_label = "Photo" if file_type == "photo" else "Vidéo"
        ack = (
            f"{type_emoji} **Respoof terminé !**\n"
            f"📁 Type : {type_label}\n"
            f"🔒 Compte : @**{clean_username}** "
            f"({info.get('device_model', '?')} · {info.get('gps_city', '?')})\n"
            f"📱 iOS : `{info.get('software', '?')}`\n"
            f"🕐 Fenêtre : {fenetre_label} (TZ {tz_name})\n"
        )
        if folder_url:
            ack += f"\n🔗 [Lien Drive]({folder_url})"

        # Message canal qui s'auto-supprime après TTL (CF_CHANNEL_MSG_TTL = 30s par défaut)
        channel_msg = await interaction.followup.send(ack, ephemeral=False)
        ttl = _channel_msg_ttl()
        if ttl > 0 and channel_msg:
            async def _auto_delete_respoof():
                try:
                    await asyncio.sleep(ttl)
                    await channel_msg.delete()
                except Exception:
                    pass
            asyncio.create_task(_auto_delete_respoof())

        # DM au VA (jamais supprimé)
        try:
            dm = await interaction.user.create_dm()
            await dm.send(ack)
        except Exception as e:
            logger.warning(f"DM respoof failed: {e}")

    # Autocomplete pour le param `compte` (filtre par VA, fallback admin = tous comptes)
    @respoof_cmd.autocomplete("compte")
    async def respoof_compte_autocomplete(
        interaction: "discord.Interaction",
        current: str,
    ) -> List[app_commands.Choice[str]]:
        try:
            user_id_str = str(interaction.user.id)
            is_admin_user = False
            try:
                is_admin_user = _is_admin(interaction.user)
            except Exception:
                pass

            # Liste les comptes (admin = tous, sinon filtre VA)
            try:
                if is_admin_user:
                    accounts = cf_storage.list_accounts(va_discord_id=None)
                    logger.info(f"[respoof autocomplete] admin user {user_id_str}, list_accounts() = {len(accounts)} comptes")
                else:
                    accounts = cf_storage.list_accounts(va_discord_id=user_id_str)
                    logger.info(f"[respoof autocomplete] user {user_id_str}, list_accounts(va_id=...) = {len(accounts)} comptes")
            except Exception as e:
                logger.error(f"[respoof autocomplete] list_accounts failed: {e}")
                accounts = []

            # Fallback : si rien trouvé pour le VA, on tente sans filtre (au cas où la
            # DB a des comptes sans va_discord_id correct)
            if not accounts and not is_admin_user:
                try:
                    all_accounts = cf_storage.list_accounts(va_discord_id=None)
                    # On garde seulement ceux qui appartiennent à ce VA OU sans owner
                    accounts = [
                        a for a in all_accounts
                        if str(a.get("va_discord_id", "") or "") in (user_id_str, "")
                    ]
                    logger.info(f"[respoof autocomplete] fallback retrieved {len(accounts)} comptes")
                except Exception as e:
                    logger.warning(f"[respoof autocomplete] fallback failed: {e}")

            # Filtre par texte tapé
            cur_lower = (current or "").strip().lower().lstrip("@")
            if cur_lower:
                filtered = [
                    a for a in accounts
                    if cur_lower in str(a.get("username", "")).lower()
                ]
            else:
                filtered = accounts

            # Discord limite à 25 choix max
            choices = []
            for a in filtered[:25]:
                try:
                    username = a.get("username", "")
                    if not username:
                        continue
                    device = str(a.get("device_choice", "") or "no-device").replace("_", " ")[:18]
                    city = str(a.get("gps_city", "") or "")[:15]
                    label = f"@{username}"
                    if device or city:
                        label += f" ({device}"
                        if city:
                            label += f" · {city}"
                        label += ")"
                    label = label[:100]  # Discord limit 100 chars
                    choices.append(app_commands.Choice(name=label, value=username))
                except Exception as e:
                    logger.warning(f"[respoof autocomplete] entry skip: {e}")
                    continue
            logger.info(f"[respoof autocomplete] returning {len(choices)} choices")
            return choices
        except Exception as e:
            logger.error(f"[respoof autocomplete] outer failure: {e}", exc_info=True)
            return []

    logger.info("Slash commands ClipFusion installés (/request, /respoof, /models, /status)")
