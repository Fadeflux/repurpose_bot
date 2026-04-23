"""
Service pour envoyer des notifications Discord via webhook.

Variables d'environnement :
  DISCORD_WEBHOOK_URL : URL du webhook Discord
"""
import asyncio
import os
from typing import List, Optional

import aiohttp

from app.utils.logger import get_logger

logger = get_logger("discord")


def _get_webhook_url() -> Optional[str]:
    """Retourne l'URL du webhook si configurée."""
    return os.getenv("DISCORD_WEBHOOK_URL")


def is_discord_enabled() -> bool:
    """Retourne True si Discord webhook est configuré."""
    return bool(_get_webhook_url())


async def send_batch_notification(
    *,
    va_name: str = "",
    batch_name: str = "",
    total_requested: int,
    succeeded: int,
    failed: int,
    drive_uploaded: int,
    retries_used: int,
    duration_seconds: float,
    device_choice: str,
    drive_folder_url: Optional[str] = None,
) -> bool:
    """
    Envoie une notification Discord après un batch terminé.
    Non-bloquant, silencieux en cas d'erreur.
    Retourne True si envoyé avec succès.
    """
    url = _get_webhook_url()
    if not url:
        return False

    # Format de durée
    mins = int(duration_seconds // 60)
    secs = int(duration_seconds % 60)
    duration_str = f"{mins}m {secs}s" if mins else f"{secs}s"

    # Couleur de l'embed selon le résultat
    if failed == 0:
        color = 0x22c55e  # vert
        emoji = "✅"
    elif succeeded > 0:
        color = 0xf59e0b  # orange
        emoji = "⚠️"
    else:
        color = 0xef4444  # rouge
        emoji = "❌"

    # Construction de l'embed
    fields: List[dict] = [
        {
            "name": "📊 Résultat",
            "value": f"**{succeeded}**/{total_requested} vidéos générées"
                     + (f"\n❌ {failed} échouée(s)" if failed else ""),
            "inline": True,
        },
        {
            "name": "📁 Google Drive",
            "value": f"**{drive_uploaded}** uploadée(s)" if drive_uploaded else "Pas d'upload",
            "inline": True,
        },
        {
            "name": "⏱️ Durée",
            "value": duration_str,
            "inline": True,
        },
    ]

    if va_name:
        fields.append({"name": "👤 VA", "value": va_name, "inline": True})

    fields.append({"name": "📱 Device", "value": device_choice, "inline": True})

    if retries_used > 0:
        fields.append({
            "name": "🔄 Auto-retry",
            "value": f"{retries_used} tentative(s) r\u00e9ussie(s)",
            "inline": True,
        })

    embed = {
        "title": f"{emoji} Batch terminé : {batch_name or 'sans nom'}",
        "color": color,
        "fields": fields,
    }
    if drive_folder_url:
        embed["url"] = drive_folder_url
        embed["description"] = f"📂 [Ouvrir le dossier Drive]({drive_folder_url})"

    payload = {
        "username": "Repurpose Bot",
        "embeds": [embed],
    }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, timeout=10) as resp:
                if resp.status < 300:
                    logger.info(f"Discord notif envoyée : {batch_name}")
                    return True
                else:
                    body = await resp.text()
                    logger.warning(f"Discord webhook erreur {resp.status}: {body[:200]}")
                    return False
    except asyncio.TimeoutError:
        logger.warning("Discord webhook timeout")
        return False
    except Exception as e:
        logger.warning(f"Discord webhook échec: {type(e).__name__}: {e}")
        return False


async def send_simple_message(content: str) -> bool:
    """Envoie un message texte simple (utilisé pour erreurs critiques)."""
    url = _get_webhook_url()
    if not url:
        return False
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                url,
                json={"username": "Repurpose Bot", "content": content[:2000]},
                timeout=5,
            ) as resp:
                return resp.status < 300
    except Exception:
        return False
