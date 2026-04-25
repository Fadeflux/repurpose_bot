"""
Bot Discord WebSocket (Gateway) pour l'onboarding des VA.

Flow :
1. VA poste son email dans le canal #email-drive
2. Bot détecte le message → extrait l'email
3. Bot stocke {discord_id, email} dans le cache VA
4. Bot répond "✅ OK email enregistré"
5. Bot supprime le message du VA + sa propre réponse après 5s
6. Bot envoie un DM privé au VA pour confirmation

Le bot tourne en tâche de fond, en parallèle de FastAPI.

Variables d'environnement :
  DISCORD_BOT_TOKEN : Token du bot (même que pour la sync VA)
  DISCORD_ONBOARDING_CHANNEL_ID : ID du canal #email-drive

Permissions Discord requises :
  - View Channels
  - Send Messages
  - Manage Messages (pour supprimer)
  - Read Message History
  - Message Content Intent (à activer dans le portail Discord)
"""
import asyncio
import io
import os
import random
import re
from typing import List, Optional

import discord
from discord.ext import commands

from app.services.discord_va_sync import (
    set_va_email,
    find_va_by_discord_id,
    sync_va_list,
)
from app.utils.logger import get_logger

logger = get_logger("discord_bot")


# Regex simple pour détecter un email dans un message
EMAIL_REGEX = re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b")


def _get_bot_token() -> Optional[str]:
    return os.getenv("DISCORD_BOT_TOKEN")


def _get_onboarding_channel_ids() -> List[int]:
    """Retourne la liste des IDs de canaux onboarding (toutes équipes)."""
    channel_ids = []
    for env_var in ("DISCORD_ONBOARDING_CHANNEL_ID", "DISCORD_ONBOARDING_CHANNEL_ID_INSTAGRAM"):
        val = os.getenv(env_var, "").strip()
        if val:
            try:
                channel_ids.append(int(val))
            except ValueError:
                pass
    return channel_ids


def _get_spoof_channel_ids() -> List[int]:
    """Retourne la liste des IDs de canaux spoof-photos (toutes équipes)."""
    channel_ids = []
    for env_var in ("DISCORD_SPOOF_CHANNEL_ID", "DISCORD_SPOOF_CHANNEL_ID_INSTAGRAM"):
        val = os.getenv(env_var, "").strip()
        if val:
            try:
                channel_ids.append(int(val))
            except ValueError:
                pass
    return channel_ids


def is_bot_enabled() -> bool:
    return bool(_get_bot_token() and (_get_onboarding_channel_ids() or _get_spoof_channel_ids()))


# =============================================================================
# Handler : spoof photo dans les canaux spoof-photos
# =============================================================================
SPOOF_DELETE_AFTER_SECONDS = 120  # 2 minutes
SUPPORTED_IMAGE_EXTS = (".jpg", ".jpeg", ".png", ".webp", ".heic", ".heif")


async def _handle_spoof_message(message: "discord.Message"):
    """
    Un VA a posté un message dans un canal spoof-photos.
    Télécharge chaque photo, applique le spoof, upload sur Drive (qui préserve les EXIF),
    envoie le lien Drive dans le canal (Discord supprimerait les EXIF si on uploadait direct).
    Tout (message Discord + fichier Drive) est auto-supprimé après 2 minutes.
    """
    import aiohttp
    from app.services.photo_spoof import spoof_image
    from app.services.drive_service import (
        upload_bytes, delete_file, get_or_create_subfolder, is_drive_enabled,
    )

    # Filtre les attachments image
    image_atts = [
        a for a in message.attachments
        if a.filename.lower().endswith(SUPPORTED_IMAGE_EXTS)
    ]

    if not image_atts:
        try:
            await message.delete()
        except Exception:
            pass
        try:
            await message.author.send(
                "❌ Envoie une photo dans le canal **#spoof-photos** "
                "(formats acceptés : JPG, PNG, HEIC)"
            )
        except Exception:
            pass
        return

    # Drive doit être activé
    if not is_drive_enabled():
        logger.error("Drive non activé, impossible de spoof")
        try:
            await message.delete()
        except Exception:
            pass
        try:
            await message.author.send("⚠️ Service temporairement indisponible. Contacte l'admin.")
        except Exception:
            pass
        return

    # Récupère (ou crée) le sous-dossier "spoof-photos-temp" dans le Drive parent
    parent_id = os.getenv("GOOGLE_DRIVE_PARENT_ID")
    if not parent_id:
        logger.error("GOOGLE_DRIVE_PARENT_ID non défini")
        try:
            await message.delete()
        except Exception:
            pass
        return

    loop = asyncio.get_event_loop()
    spoof_folder_id = await loop.run_in_executor(
        None, get_or_create_subfolder, parent_id, "spoof-photos-temp"
    )
    if not spoof_folder_id:
        logger.error("Impossible de créer le sous-dossier spoof-photos-temp")
        return

    # Télécharge, spoof, upload chaque photo
    uploaded_results = []  # liste de {drive_id, download_url, device, filename}
    try:
        async with aiohttp.ClientSession() as session:
            for att in image_atts:
                try:
                    async with session.get(att.url, timeout=30) as r:
                        if r.status != 200:
                            logger.warning(f"Impossible de dl {att.filename}: {r.status}")
                            continue
                        raw_bytes = await r.read()

                    # Spoof
                    spoofed_bytes, info = spoof_image(raw_bytes, att.filename)

                    # Nom unique
                    base_name = att.filename.rsplit(".", 1)[0]
                    new_name = f"{base_name}_IMG_{random.randint(1000, 9999)}.jpg"

                    # Upload sur Drive (public link)
                    upload_result = await loop.run_in_executor(
                        None,
                        upload_bytes,
                        spoofed_bytes,
                        new_name,
                        spoof_folder_id,
                        "image/jpeg",
                        True,  # make_public
                    )
                    if not upload_result:
                        logger.warning(f"Upload Drive échoué pour {new_name}")
                        continue

                    # Construit le lien de download direct
                    file_id = upload_result.get("id")
                    # Lien de download direct (force le DL plutôt que l'aperçu)
                    download_url = f"https://drive.google.com/uc?export=download&id={file_id}"
                    view_url = upload_result.get("webViewLink", "")

                    uploaded_results.append({
                        "drive_id": file_id,
                        "download_url": download_url,
                        "view_url": view_url,
                        "device": info["device_model"],
                        "filename": new_name,
                    })
                except Exception as e:
                    logger.warning(f"Erreur spoof {att.filename}: {e}")
    except Exception as e:
        logger.exception(f"Erreur traitement spoof: {e}")

    # Supprime le message original direct
    try:
        await message.delete()
    except Exception as e:
        logger.warning(f"Erreur suppression original: {e}")

    if not uploaded_results:
        try:
            await message.author.send("⚠️ Impossible de traiter ta photo. Réessaie.")
        except Exception:
            pass
        return

    # Construit le message de réponse avec les liens Drive
    lines = [f"📸 {message.author.mention} — Photo(s) spoofée(s) :"]
    for idx, res in enumerate(uploaded_results, start=1):
        lines.append(
            f"  `{idx}.` **{res['device']}** → "
            f"[📥 Télécharger]({res['download_url']}) · "
            f"[👁️ Voir]({res['view_url']})"
        )
    lines.append("*⏱️ Auto-supprimée dans 2 minutes*")
    content = "\n".join(lines)

    try:
        sent_msg = await message.channel.send(content=content)
    except Exception as e:
        logger.exception(f"Erreur envoi réponse: {e}")
        # Si le message échoue, supprime quand même les fichiers Drive
        for res in uploaded_results:
            await loop.run_in_executor(None, delete_file, res["drive_id"])
        return

    # Planifie la suppression dans 2 minutes (message + fichiers Drive)
    async def _delete_later():
        await asyncio.sleep(SPOOF_DELETE_AFTER_SECONDS)
        # Supprime le message Discord
        try:
            await sent_msg.delete()
        except Exception:
            pass
        # Supprime les fichiers Drive
        for res in uploaded_results:
            try:
                await loop.run_in_executor(None, delete_file, res["drive_id"])
            except Exception:
                pass

    asyncio.create_task(_delete_later())



_intents = discord.Intents.default()
_intents.message_content = True  # requiert MESSAGE CONTENT INTENT activé
_intents.members = True           # requiert SERVER MEMBERS INTENT activé

_bot: Optional[commands.Bot] = None
_bot_task: Optional[asyncio.Task] = None


def _build_bot() -> commands.Bot:
    bot = commands.Bot(command_prefix="!", intents=_intents)

    @bot.event
    async def on_ready():
        logger.info(f"Bot Discord connecté : {bot.user} (ID={bot.user.id})")
        for cid in _get_onboarding_channel_ids():
            ch = bot.get_channel(cid)
            if ch:
                logger.info(f"Canal onboarding trouvé : #{ch.name} (guild={ch.guild.name})")
            else:
                logger.warning(f"Canal onboarding ID={cid} introuvable")
        for cid in _get_spoof_channel_ids():
            ch = bot.get_channel(cid)
            if ch:
                logger.info(f"Canal spoof-photos trouvé : #{ch.name} (guild={ch.guild.name})")
            else:
                logger.warning(f"Canal spoof-photos ID={cid} introuvable")
        # Première sync au démarrage
        try:
            await sync_va_list()
        except Exception as e:
            logger.warning(f"Sync au démarrage échouée: {e}")

    @bot.event
    async def on_message(message: discord.Message):
        # Ignore les messages du bot lui-même
        if message.author.bot:
            return

        onboarding_ids = _get_onboarding_channel_ids()
        spoof_ids = _get_spoof_channel_ids()

        # Route vers le bon handler selon le canal
        if message.channel.id in spoof_ids:
            await _handle_spoof_message(message)
            return

        if message.channel.id not in onboarding_ids:
            return

        # -- Canal onboarding : cherche un email --
        match = EMAIL_REGEX.search(message.content)
        if not match:
            # Pas d'email détecté : supprime le message et explique en DM
            try:
                await message.delete()
            except discord.Forbidden:
                logger.warning("Pas la permission de supprimer le message")
            except Exception as e:
                logger.warning(f"Erreur suppression message: {e}")
            try:
                await message.author.send(
                    "❌ Je n'ai pas détecté d'email dans ton message.\n"
                    "Envoie juste ton adresse Gmail dans le canal **#email-drive** (ex: `tonprenom@gmail.com`)"
                )
            except discord.Forbidden:
                logger.info(f"Impossible d'envoyer un DM à {message.author} (DM fermés)")
            return

        email = match.group(0).lower()
        discord_id = str(message.author.id)
        display_name = message.author.display_name

        # Détermine l'équipe selon le serveur Discord où le message a été posté
        from app.services.discord_va_sync import get_teams_config
        teams = get_teams_config()
        guild_id_str = str(message.guild.id) if message.guild else ""
        current_team = None
        for t in teams:
            if str(t.get("guild_id", "")) == guild_id_str:
                current_team = t
                break

        role_label = current_team.get("role_name", "VA") if current_team else "VA"
        team_label = current_team.get("label", "ton équipe") if current_team else "ton équipe"

        # Vérifie que l'auteur est bien un VA (via son discord_id dans le cache)
        va_info = find_va_by_discord_id(discord_id)
        if not va_info:
            # Pas dans la liste des VA : supprime + prévient
            try:
                await message.delete()
            except Exception:
                pass
            try:
                await message.author.send(
                    f"❌ Tu n'es pas enregistré comme VA {team_label} dans notre système.\n"
                    f"Contacte l'admin pour obtenir le rôle **{role_label}**, puis réessaie."
                )
            except Exception:
                pass
            logger.info(f"Email ignoré (non-VA): {display_name} ({discord_id})")
            return

        # Enregistre l'email
        ok = set_va_email(discord_id, email)
        if not ok:
            try:
                await message.author.send(
                    "⚠️ Une erreur est survenue lors de l'enregistrement. Réessaie dans quelques minutes."
                )
            except Exception:
                pass
            return

        # Réponse publique (visible 5s), puis suppression
        try:
            confirm_msg = await message.channel.send(
                f"✅ Email enregistré pour {message.author.mention} !"
            )
        except Exception as e:
            logger.warning(f"Erreur envoi confirmation: {e}")
            confirm_msg = None

        # Supprime le message original du VA
        try:
            await message.delete()
        except Exception as e:
            logger.warning(f"Erreur suppression message VA: {e}")

        # Attend 5s puis supprime la confirmation
        if confirm_msg:
            await asyncio.sleep(5)
            try:
                await confirm_msg.delete()
            except Exception:
                pass

        # Envoie un DM privé de confirmation
        try:
            await message.author.send(
                f"✅ **Email enregistré !**\n\n"
                f"Ton email `{email}` a bien été enregistré. "
                f"À partir de maintenant, tu recevras automatiquement les partages Drive "
                f"quand ton manager générera tes Drive avec les vidéos pour toi.\n\n"
                f"Tu verras apparaître les dossiers dans **Partagé avec moi** sur ton Drive "
                f"+ Ton Drive par message ici."
            )
            logger.info(f"Email enregistré pour {display_name} ({discord_id}): {email}")
        except discord.Forbidden:
            logger.info(f"Impossible d'envoyer DM à {display_name} (DM fermés)")
        except Exception as e:
            logger.warning(f"Erreur envoi DM: {e}")

    return bot


async def notify_va_drive_ready(discord_id: str, folder_url: str = "") -> bool:
    """
    Envoie un DM privé à un VA pour lui dire que son Drive est à jour.
    Retourne True si envoyé avec succès.
    """
    global _bot
    if _bot is None or not _bot.is_ready():
        logger.warning("Bot Discord pas prêt, DM non envoyé")
        return False
    try:
        user = await _bot.fetch_user(int(discord_id))
        if not user:
            return False
        if folder_url:
            msg = (
                f"✅ **Ton Drive est mis à jour**, va voir en appuyant sur "
                f"**[Ouvrir le dossier Drive]({folder_url})**"
            )
        else:
            msg = (
                "✅ **Ton Drive est mis à jour**, va voir dans ton application "
                "**Google Drive** (dossier dans **Partagé avec moi**)."
            )
        await user.send(msg)
        logger.info(f"DM Drive envoyé à {discord_id}")
        return True
    except discord.Forbidden:
        logger.info(f"DM fermés pour user {discord_id}")
        return False
    except Exception as e:
        logger.warning(f"Erreur DM Drive à {discord_id}: {e}")
        return False


# =============================================================================
# Démarrage du bot en arrière-plan
# =============================================================================
async def _run_bot():
    """Lance le bot en boucle (reconnexion auto en cas de déco)."""
    global _bot
    token = _get_bot_token()
    if not token:
        logger.warning("Bot Discord non démarré (token manquant)")
        return

    _bot = _build_bot()
    try:
        await _bot.start(token)
    except Exception as e:
        logger.exception(f"Bot Discord crashed: {e}")


def start_discord_bot():
    """Démarre le bot Discord en tâche de fond (à appeler au startup)."""
    global _bot_task
    if not is_bot_enabled():
        logger.info(
            "Bot Discord Gateway non démarré "
            "(DISCORD_BOT_TOKEN ou DISCORD_ONBOARDING_CHANNEL_ID manquant)"
        )
        return
    if _bot_task is None or _bot_task.done():
        _bot_task = asyncio.create_task(_run_bot())
        logger.info("Bot Discord Gateway démarré en arrière-plan")
