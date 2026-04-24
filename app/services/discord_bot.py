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
import os
import re
from typing import Optional

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


def _get_onboarding_channel_id() -> Optional[int]:
    val = os.getenv("DISCORD_ONBOARDING_CHANNEL_ID", "").strip()
    if not val:
        return None
    try:
        return int(val)
    except ValueError:
        return None


def is_bot_enabled() -> bool:
    return bool(_get_bot_token() and _get_onboarding_channel_id())


# =============================================================================
# Bot instance
# =============================================================================
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
        channel_id = _get_onboarding_channel_id()
        if channel_id:
            ch = bot.get_channel(channel_id)
            if ch:
                logger.info(f"Canal onboarding trouvé : #{ch.name}")
            else:
                logger.warning(f"Canal onboarding ID={channel_id} introuvable")
        # Première sync au démarrage pour s'assurer qu'on a la liste
        try:
            await sync_va_list()
        except Exception as e:
            logger.warning(f"Sync au démarrage échouée: {e}")

    @bot.event
    async def on_message(message: discord.Message):
        # Ignore les messages du bot lui-même
        if message.author.bot:
            return

        # Écoute uniquement le canal onboarding
        channel_id = _get_onboarding_channel_id()
        if not channel_id or message.channel.id != channel_id:
            return

        # Cherche un email dans le message
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
                    "❌ Tu n'es pas enregistré comme VA dans notre système.\n"
                    "Contacte l'admin pour obtenir le rôle **VA Geelark**, puis réessaie."
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
                f"Ton email `{email}` a bien été enregistré.\n"
                f"À partir de maintenant, tu recevras automatiquement les partages Drive "
                f"quand ton manager générera un batch vidéo pour toi.\n\n"
                f"Tu verras apparaître les dossiers dans **Partagé avec moi** sur ton Drive."
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
        msg = (
            "✅ **Ton Drive est mis à jour !**\n\n"
            "Je te laisse regarder dans ton application **Google Drive**.\n"
            "Les nouvelles vidéos sont dans **Partagé avec moi**."
        )
        if folder_url:
            msg += f"\n\n📂 [Ouvrir le dossier directement]({folder_url})"
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
