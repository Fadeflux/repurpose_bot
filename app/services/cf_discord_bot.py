"""
ClipFusion Discord Bot — slash command `/request quantite:N modele:X`
Permet aux VAs de demander un batch automatique sur Discord.

Architecture :
  - Greffe sur le bot Discord existant (discord_bot.py de Repurpose)
  - File d'attente FIFO : 1 mix à la fois pour pas saturer Railway
  - Réactions emoji pour feedback visuel (⏳ → ⚙️ → ✅ ou ❌)
  - DM final au VA avec lien Drive

Variables d'env :
  CF_REQUEST_CHANNEL_IDS : IDs de canaux Discord (CSV) où /request est dispo
  CF_DEFAULT_TEAM        : équipe par défaut (geelark | instagram), default geelark
  CF_REQUEST_MAX_VIDEOS  : limite max par demande (default 200)
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
                        await req.interaction.followup.send(
                            f"❌ Erreur pendant le mix : {e}",
                            ephemeral=False,
                        )
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
            if t == "progress":
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

    # 5. Notif finale
    if drive_url:
        msg = f"✅ Mix terminé : **{progress_count}** vidéos prêtes !\n📁 <{drive_url}>"
    else:
        msg = f"✅ Mix terminé : **{progress_count}** vidéos générées (Drive non configuré)."
    await _say(req, msg)

    # 6. DM au VA avec le lien direct
    if drive_url and req.interaction:
        try:
            user = await req.interaction.client.fetch_user(req.user_id)
            await user.send(
                f"🎬 Ton batch ClipFusion est prêt !\n"
                f"**{progress_count}** vidéos · modèle **{model_label}** · équipe **{req.team}**\n"
                f"📁 {drive_url}"
            )
        except Exception as e:
            logger.warning(f"DM VA échoué: {e}")


async def _say(req: CFRequest, content: str):
    """Envoie un follow-up sur l'interaction (visible à tous dans le canal)."""
    try:
        if req.interaction:
            await req.interaction.followup.send(content, ephemeral=False)
    except Exception as e:
        logger.warning(f"Reply Discord échoué: {e}")


# ============================================================================
# INSTALLATION DU SLASH COMMAND SUR LE BOT EXISTANT
# ============================================================================
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

        # 2. Validation quantité
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

        # 3. Validation modèle
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

        # 4. Construit la requête + ajoute dans la queue
        _ensure_queue()
        team = _default_team()
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

        # 5. Réponse initiale + lance worker si pas déjà
        position = len(_pending) + (1 if _current else 0)
        if position <= 1 and _current is None:
            ack = (
                f"⚙️ Demande acceptée : **{quantite}** vidéos pour modèle **{model['label']}**\n"
                f"Lancement immédiat..."
            )
        else:
            ack = (
                f"⏳ Demande acceptée : **{quantite}** vidéos pour modèle **{model['label']}**\n"
                f"Position dans la file : **{position}**"
            )
        await interaction.response.send_message(ack, ephemeral=False)

        # Démarre le worker s'il ne tourne pas
        global _worker_task
        if _worker_task is None or _worker_task.done():
            _worker_task = asyncio.create_task(_worker_loop())

    @bot.tree.command(name="models", description="Liste les modèles disponibles")
    async def models_cmd(interaction: "discord.Interaction"):
        allowed = _get_request_channel_ids()
        if allowed and interaction.channel_id not in allowed:
            await interaction.response.send_message(
                f"❌ Cette commande n'est pas dispo dans ce canal.",
                ephemeral=True,
            )
            return
        models = cf_storage.list_models()
        if not models:
            await interaction.response.send_message(
                "Aucun modèle configuré. Demande à l'admin d'en créer.",
                ephemeral=True,
            )
            return
        lines = []
        for m in models:
            n_videos = len(cf_storage.list_videos(model_id=m["id"]))
            lines.append(f"• **ID {m['id']}** — {m['label']} ({n_videos} vidéos)")
        msg = "**📋 Modèles disponibles :**\n" + "\n".join(lines)
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
