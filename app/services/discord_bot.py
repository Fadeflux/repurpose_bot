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
    for env_var in (
        "DISCORD_ONBOARDING_CHANNEL_ID",
        "DISCORD_ONBOARDING_CHANNEL_ID_INSTAGRAM",
        "DISCORD_ONBOARDING_CHANNEL_ID_THREADS",
    ):
        val = os.getenv(env_var, "").strip()
        if val:
            try:
                channel_ids.append(int(val))
            except ValueError:
                pass
    # Hardcoded fallback : canal gmail-drive du serveur Threads
    # (Geelark et Instagram sont via env vars Railway)
    threads_gmail_drive = 1504618896131358832
    if threads_gmail_drive not in channel_ids:
        channel_ids.append(threads_gmail_drive)
    return channel_ids


def _get_spoof_channel_ids() -> List[int]:
    """Retourne la liste des IDs de canaux spoof-photos (toutes équipes)."""
    channel_ids = []
    for env_var in (
        "DISCORD_SPOOF_CHANNEL_ID",
        "DISCORD_SPOOF_CHANNEL_ID_INSTAGRAM",
        "DISCORD_SPOOF_CHANNEL_ID_THREADS",
    ):
        val = os.getenv(env_var, "").strip()
        if val:
            try:
                channel_ids.append(int(val))
            except ValueError:
                pass
    # Hardcoded fallback : canal spoof-photos du serveur Threads
    # (Geelark et Instagram sont via env vars Railway)
    threads_spoof = 1502037917160177926
    if threads_spoof not in channel_ids:
        channel_ids.append(threads_spoof)
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
    Télécharge chaque photo, applique le spoof, l'envoie comme fichier .heic
    directement dans Discord (Discord ne strip pas les EXIF des .heic car
    pas reconnu comme image inline).

    Tout est auto-supprimé après 2 minutes.
    """
    import aiohttp
    from app.services.photo_spoof import spoof_image

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

    # Télécharge + spoof chaque photo en mémoire
    spoofed_files = []  # liste de {discord_file, device, filename}
    try:
        async with aiohttp.ClientSession() as session:
            for att in image_atts:
                try:
                    async with session.get(att.url, timeout=30) as r:
                        if r.status != 200:
                            logger.warning(f"Impossible de dl {att.filename}: {r.status}")
                            continue
                        raw_bytes = await r.read()

                    # Spoof (renvoie bytes JPEG avec EXIF iPhone)
                    spoofed_bytes, info = spoof_image(raw_bytes, att.filename)

                    # Nom .heic pour bypass le strip EXIF de Discord
                    # Le contenu reste un JPEG valide, juste extension .heic
                    base_name = att.filename.rsplit(".", 1)[0]
                    new_name = f"{base_name}_IMG_{random.randint(1000, 9999)}.heic"

                    # Crée un discord.File depuis les bytes en mémoire
                    discord_file = discord.File(
                        fp=io.BytesIO(spoofed_bytes),
                        filename=new_name,
                    )
                    spoofed_files.append({
                        "file": discord_file,
                        "device": info["device_model"],
                        "filename": new_name,
                    })
                except Exception as e:
                    logger.warning(f"Erreur spoof {att.filename}: {e}")
    except Exception as e:
        logger.exception(f"Erreur traitement spoof: {e}")

    # Supprime le message original direct (avant d'envoyer la réponse)
    try:
        await message.delete()
    except Exception as e:
        logger.warning(f"Erreur suppression original: {e}")

    if not spoofed_files:
        try:
            await message.author.send("⚠️ Impossible de traiter ta photo. Réessaie.")
        except Exception:
            pass
        return

    # Construit le message + envoie les fichiers attachés
    lines = [f"📸 {message.author.mention} — Photo(s) spoofée(s) :"]
    for idx, sf in enumerate(spoofed_files, start=1):
        lines.append(f"  `{idx}.` **{sf['device']}**")
    lines.append("*⏱️ Auto-supprimée dans 2 minutes*")
    content = "\n".join(lines)

    try:
        # Envoie tous les fichiers attachés en une seule fois (Discord limite à 10 par message)
        files_to_send = [sf["file"] for sf in spoofed_files[:10]]
        sent_msg = await message.channel.send(content=content, files=files_to_send)
    except Exception as e:
        logger.exception(f"Erreur envoi photo spoofée: {e}")
        try:
            await message.author.send("⚠️ Erreur lors de l'envoi de la photo spoofée. Réessaie.")
        except Exception:
            pass
        return

    # Planifie la suppression dans 2 minutes
    async def _delete_later():
        await asyncio.sleep(SPOOF_DELETE_AFTER_SECONDS)
        try:
            await sent_msg.delete()
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

        # Sync les slash commands Discord (/request, /models, /status)
        # Si CF_GUILD_ID est défini → sync immédiate sur ce serveur uniquement
        # Sinon → sync globale (peut prendre 1h pour propager sur Discord)
        try:
            guild_id = os.environ.get("CF_GUILD_ID", "").strip()
            if guild_id and guild_id.isdigit():
                guild = discord.Object(id=int(guild_id))
                bot.tree.copy_global_to(guild=guild)
                synced = await bot.tree.sync(guild=guild)
                logger.info(f"Slash commands synced sur guild {guild_id} : {len(synced)} commandes")
            else:
                synced = await bot.tree.sync()
                logger.info(f"Slash commands synced (global, peut prendre 1h) : {len(synced)} commandes")
        except Exception as e:
            logger.warning(f"Tree sync échoué: {e}")

        # Démarre la mise à jour automatique du statut Discord (toutes les 30s)
        # Affiche dans le member list : "Mixing · 3 en attente" etc.
        try:
            asyncio.create_task(_presence_updater_loop(bot))
            logger.info("✅ Bot presence updater démarré (refresh 30s)")
        except Exception as e:
            logger.warning(f"Presence updater n'a pas pu démarrer: {e}")

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

    # Branche les slash commands ClipFusion (/request, /models, /status)
    try:
        from app.services.cf_discord_bot import install_clipfusion_commands
        install_clipfusion_commands(bot)
    except Exception as e:
        logger.warning(f"Install ClipFusion slash commands échoué: {e}")

    return bot


def _get_batch_channel_ids() -> dict:
    """
    Retourne les IDs de canaux de notifs batch par équipe.
    Variables :
      DISCORD_BATCH_CHANNEL_ID_GEELARK    : canal notifs équipe Geelark
      DISCORD_BATCH_CHANNEL_ID_INSTAGRAM  : canal notifs équipe Instagram
      DISCORD_BATCH_CHANNEL_ID            : fallback générique
    """
    result = {}
    for team_key, env_name in [
        ("geelark", "DISCORD_BATCH_CHANNEL_ID_GEELARK"),
        ("instagram", "DISCORD_BATCH_CHANNEL_ID_INSTAGRAM"),
        ("default", "DISCORD_BATCH_CHANNEL_ID"),
    ]:
        val = os.getenv(env_name, "").strip()
        if val:
            try:
                result[team_key] = int(val)
            except ValueError:
                pass
    return result


async def send_batch_notification_via_bot(
    *,
    team: str,
    va_name: str,
    va_discord_id: str,
    batch_name: str,
    total_requested: int,
    succeeded: int,
    failed: int,
    drive_uploaded: int,
    retries_used: int,
    duration_seconds: float,
    device_choice: str,
    drive_folder_url: str = "",
) -> bool:
    """
    Envoie la notif de fin de batch via le bot Discord (au lieu d'un webhook).
    Le canal est choisi selon l'équipe :
      - team=geelark   → DISCORD_BATCH_CHANNEL_ID_GEELARK
      - team=instagram → DISCORD_BATCH_CHANNEL_ID_INSTAGRAM
      - autre/vide     → DISCORD_BATCH_CHANNEL_ID
    """
    global _bot
    if _bot is None or not _bot.is_ready():
        logger.warning("Bot Discord pas prêt, notif batch non envoyée")
        return False

    # Choisit le canal selon l'équipe
    channel_ids = _get_batch_channel_ids()
    team_key = (team or "").lower().strip()
    channel_id = channel_ids.get(team_key) or channel_ids.get("default")

    if not channel_id:
        logger.warning(f"Aucun canal de notif batch configuré pour team={team}")
        return False

    channel = _bot.get_channel(channel_id)
    if not channel:
        logger.warning(f"Canal {channel_id} introuvable pour les notifs batch")
        return False

    # Format durée
    mins = int(duration_seconds // 60)
    secs = int(duration_seconds % 60)
    duration_str = f"{mins}m {secs}s" if mins else f"{secs}s"

    # Couleur + emoji selon résultat
    if failed == 0:
        color = 0x22c55e
        status_emoji = "✅"
        status_text = "Batch terminé"
    elif succeeded > 0:
        color = 0xf59e0b
        status_emoji = "⚠️"
        status_text = "Batch terminé (avec erreurs)"
    else:
        color = 0xef4444
        status_emoji = "❌"
        status_text = "Batch échoué"

    # Champ device lisible
    device_label = device_choice.replace("_", " ").title()
    if device_choice == "smart_mix":
        device_label = "🎯 Smart Mix"
    elif device_choice == "mix_random":
        device_label = "🎲 Mix iPhone + Android"

    # Build embed
    embed = discord.Embed(
        title=f"{status_emoji} {status_text} : {batch_name}",
        color=color,
    )

    if drive_folder_url:
        embed.description = f"📂 [Ouvrir le dossier Drive]({drive_folder_url})"

    embed.add_field(
        name="📊 Résultat",
        value=f"{succeeded}/{total_requested} copies",
        inline=True,
    )
    if drive_uploaded > 0:
        embed.add_field(
            name="📁 Google Drive",
            value=f"{drive_uploaded} uploads",
            inline=True,
        )
    embed.add_field(
        name="⏱️ Durée",
        value=duration_str,
        inline=True,
    )

    # VA mention
    va_field_value = f"<@{va_discord_id}>" if va_discord_id else (va_name or "—")
    embed.add_field(
        name="👤 VA",
        value=va_field_value,
        inline=True,
    )
    embed.add_field(
        name="📱 Device",
        value=device_label,
        inline=True,
    )

    if retries_used > 0:
        embed.add_field(
            name="🔄 Retries",
            value=str(retries_used),
            inline=True,
        )

    # Content avec mention pour push notif
    content = f"<@{va_discord_id}>" if va_discord_id else None
    allowed_mentions = discord.AllowedMentions(users=True) if va_discord_id else None

    try:
        await channel.send(
            content=content,
            embed=embed,
            allowed_mentions=allowed_mentions,
        )
        logger.info(f"Notif batch envoyée via bot dans canal {channel.name} (team={team})")
        return True
    except Exception as e:
        logger.exception(f"Erreur envoi notif batch via bot: {e}")
        return False


async def _presence_updater_loop(bot: "discord.Client") -> None:
    """
    Met à jour le statut "Activity" du bot Discord toutes les 30s selon
    l'état de la queue ClipFusion. Visible dans la member list + sur le
    profil du bot. Donne une vision live de la charge sans avoir à
    taper /status.
    """
    import asyncio as _asyncio
    while True:
        try:
            # Import lazy : évite la dépendance circulaire au boot du module
            from app.services.cf_discord_bot import _current, _pending  # type: ignore
            in_queue = len(_pending)
            if _current is not None:
                if in_queue > 0:
                    text = f"🎬 Mixing · {in_queue} en attente"
                else:
                    text = "🎬 Mixing 1 batch"
                status = discord.Status.online
            elif in_queue > 0:
                text = f"⏳ {in_queue} batchs en attente"
                status = discord.Status.online
            else:
                text = "💤 Idle · /request pour démarrer"
                status = discord.Status.idle
            activity = discord.Activity(
                type=discord.ActivityType.watching,
                name=text,
            )
            await bot.change_presence(activity=activity, status=status)
        except Exception as e:
            logger.warning(f"Presence update échoué: {e}")
        await _asyncio.sleep(30)


async def notify_va_drive_ready(
    discord_id: str,
    folder_url: str = "",
    fallback_channel_id: Optional[int] = None,
) -> bool:
    """
    Envoie un DM privé à un VA pour lui dire que son Drive est à jour.

    Si le DM échoue (DMs fermés = Forbidden), fallback : post le message
    dans `fallback_channel_id` en mentionnant le VA. Comme ça il rate
    jamais sa notif même avec les DMs désactivés.

    Retourne True si AU MOINS UN canal de notif a marché (DM ou fallback).
    """
    global _bot
    if _bot is None or not _bot.is_ready():
        logger.warning("Bot Discord pas prêt, DM non envoyé")
        return False

    # Rich embed plutôt que plain text — plus visible, plus pro
    embed = discord.Embed(
        title="✅ Ton Drive est prêt !",
        color=0x2ECC71,  # vert succès
    )
    if folder_url:
        embed.description = (
            f"Toutes tes vidéos sont uploadées dans le dossier Drive ci-dessous :"
        )
        embed.add_field(
            name="📁 Lien direct",
            value=f"[Ouvrir le dossier Drive]({folder_url})",
            inline=False,
        )
    else:
        embed.description = (
            "Toutes tes vidéos sont uploadées. Va voir dans ton application "
            "**Google Drive** (dossier dans **Partagé avec moi**)."
        )
    embed.set_footer(text="ClipFusion · prêt à poster 🚀")

    # Fallback text version pour le channel si DMs fermés
    fallback_text = (
        f"✅ **Ton Drive est prêt** — "
        f"[Ouvrir le dossier]({folder_url})" if folder_url else
        "✅ **Ton Drive est prêt** — check Google Drive (Partagé avec moi)"
    )

    # 1. Tentative DM (chemin nominal, embed riche)
    dm_failed = False
    try:
        user = await _bot.fetch_user(int(discord_id))
        if user:
            await user.send(embed=embed)
            logger.info(f"DM Drive envoyé à {discord_id}")
            return True
        dm_failed = True
    except discord.Forbidden:
        logger.info(f"DMs fermés pour user {discord_id}, fallback vers canal")
        dm_failed = True
    except Exception as e:
        logger.warning(f"Erreur DM Drive à {discord_id}: {e}, fallback vers canal")
        dm_failed = True

    # 2. Fallback : post dans le canal d'origine si fourni (embed + mention)
    if dm_failed and fallback_channel_id:
        try:
            channel = _bot.get_channel(int(fallback_channel_id))
            if channel is None:
                channel = await _bot.fetch_channel(int(fallback_channel_id))
            if channel:
                await channel.send(content=f"<@{discord_id}>", embed=embed)
                logger.info(
                    f"Fallback notif Drive dans canal {fallback_channel_id} pour user {discord_id}"
                )
                return True
        except Exception as e:
            logger.warning(f"Fallback canal Drive échoué pour {discord_id}: {e}")

    return False


# =============================================================================
# Démarrage du bot en arrière-plan
# =============================================================================
async def _run_bot():
    """
    Lance le bot avec auto-reconnect ROBUSTE.
    Si le bot crash ou perd la connexion Gateway, il redémarre automatiquement
    avec backoff exponentiel (5s, 10s, 20s, max 60s entre tentatives).
    """
    global _bot
    token = _get_bot_token()
    if not token:
        logger.warning("Bot Discord non démarré (token manquant)")
        return

    backoff = 5  # secondes d'attente entre reconnexions
    max_backoff = 60
    consecutive_failures = 0

    while True:
        try:
            # Recrée le bot à chaque tentative (cleanup propre)
            _bot = _build_bot()
            logger.info(f"🚀 Bot Discord démarrage (tentative #{consecutive_failures + 1})")
            await _bot.start(token)
            # Si on arrive ici sans exception, c'est qu'on a stop proprement (rare)
            logger.info("Bot Discord arrêté proprement (sortie de la boucle)")
            break
        except Exception as e:
            consecutive_failures += 1
            logger.exception(
                f"⚠️ Bot Discord crashed (tentative #{consecutive_failures}): {e}. "
                f"Reconnexion dans {backoff}s..."
            )
            # Reset le _bot pour évit de garder une instance morte
            try:
                if _bot:
                    await _bot.close()
            except Exception:
                pass
            _bot = None
            # Attend avec backoff exponentiel avant retry
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, max_backoff)
            # Si 10 échecs consécutifs, log alerte critique
            if consecutive_failures % 10 == 0:
                logger.error(
                    f"🚨 ALERT: {consecutive_failures} échecs consécutifs du bot Discord. "
                    "Vérifie le token et les permissions."
                )


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
