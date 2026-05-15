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


# Labels lisibles pour les device choices
DEVICE_LABELS = {
    "mix_random":        "🎲 Mix iPhone + Android",
    "iphone_random":     "📱 iPhone aléatoire (16/17)",
    "android_random":    "🅰️ Android aléatoire",
    "samsung_random":    "🅰️ Samsung aléatoire",
    "pixel_random":      "🅰️ Google Pixel aléatoire",
    "iphone_17_pro_max": "📱 iPhone 17 Pro Max",
    "iphone_17_pro":     "📱 iPhone 17 Pro",
    "iphone_17_air":     "📱 iPhone 17 Air",
    "iphone_17":         "📱 iPhone 17",
    "iphone_16_pro_max": "📱 iPhone 16 Pro Max",
    "iphone_16_pro":     "📱 iPhone 16 Pro",
    "iphone_16_plus":    "📱 iPhone 16 Plus",
    "iphone_16":         "📱 iPhone 16",
    "iphone_16e":        "📱 iPhone 16e",
    "samsung_s25_ultra": "🅰️ Galaxy S25 Ultra",
    "samsung_s25_plus":  "🅰️ Galaxy S25+",
    "samsung_s25":       "🅰️ Galaxy S25",
    "samsung_s24_ultra": "🅰️ Galaxy S24 Ultra",
    "samsung_s24_plus":  "🅰️ Galaxy S24+",
    "samsung_s24":       "🅰️ Galaxy S24",
    "samsung_s23_ultra": "🅰️ Galaxy S23 Ultra",
    "samsung_s23_plus":  "🅰️ Galaxy S23+",
    "samsung_s23":       "🅰️ Galaxy S23",
    "pixel_9_pro_xl":    "🅰️ Pixel 9 Pro XL",
    "pixel_9_pro":       "🅰️ Pixel 9 Pro",
    "pixel_9":           "🅰️ Pixel 9",
    "pixel_8_pro":       "🅰️ Pixel 8 Pro",
    "pixel_8":           "🅰️ Pixel 8",
    "xiaomi_15_ultra":   "🅰️ Xiaomi 15 Ultra",
    "xiaomi_15_pro":     "🅰️ Xiaomi 15 Pro",
    "xiaomi_15":         "🅰️ Xiaomi 15",
}


def _format_device(device_choice: str) -> str:
    """Retourne le label lisible d'un device_choice."""
    return DEVICE_LABELS.get(device_choice, device_choice)


async def send_batch_notification(
    *,
    va_name: str = "",
    va_discord_id: str = "",
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
    Si va_discord_id est fourni, mentionne le VA pour qu'il reçoive une notif push.
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
        color = 0x22c55e
        emoji = "✅"
    elif succeeded > 0:
        color = 0xf59e0b
        emoji = "⚠️"
    else:
        color = 0xef4444
        emoji = "❌"

    # Champ VA : mention si on a l'ID Discord, sinon juste le nom
    va_field_value = f"<@{va_discord_id}>" if va_discord_id else (va_name or "—")

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
        {"name": "👤 VA", "value": va_field_value, "inline": True},
        {"name": "📱 Device", "value": _format_device(device_choice), "inline": True},
    ]

    if retries_used > 0:
        fields.append({
            "name": "🔄 Auto-retry",
            "value": f"{retries_used} tentative(s) réussie(s)",
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

    # Mention dans le content pour déclencher une vraie notif push chez le VA
    content = f"<@{va_discord_id}>" if va_discord_id else None

    payload = {
        "username": "Repurpose Bot",
        "embeds": [embed],
        # allowed_mentions : autorise uniquement les users mentionnés (évite @everyone)
        "allowed_mentions": {"parse": ["users"]},
    }
    if content:
        payload["content"] = content

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, timeout=10) as resp:
                body = await resp.text()
                if resp.status < 300:
                    logger.info(f"Discord notif envoyée : {batch_name} (status={resp.status})")
                    return True
                else:
                    logger.error(
                        f"Discord webhook ERREUR {resp.status} pour batch '{batch_name}': "
                        f"body={body[:500]} | "
                        f"payload_keys={list(payload.keys())} | "
                        f"has_content={bool(content)}"
                    )
                    return False
    except asyncio.TimeoutError:
        logger.warning(f"Discord webhook TIMEOUT pour batch '{batch_name}'")
        return False
    except Exception as e:
        logger.exception(f"Discord webhook EXCEPTION pour batch '{batch_name}': {type(e).__name__}: {e}")
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


# ============================================================
# Alertes admin (webhook séparé pour signaler les batches problématiques)
# ============================================================

def _get_admin_webhook_for_team(team: Optional[str] = None) -> Optional[str]:
    """
    Retourne l'URL du webhook admin selon la team :
    - team="geelark" → DISCORD_ADMIN_WEBHOOK_URL_GEELARK si défini
    - team="instagram" → DISCORD_ADMIN_WEBHOOK_URL_INSTAGRAM si défini
    - team="threads" → DISCORD_ADMIN_WEBHOOK_URL_THREADS si défini
    - Sinon ou si team-spécifique pas défini → fallback DISCORD_ADMIN_WEBHOOK_URL

    Permet de router les alertes par équipe (Geelark alerts → canal Geelark
    admin, Insta alerts → canal Insta admin) tout en gardant un fallback
    global pour les alertes qui n'ont pas de team (ex: Drive quota).
    """
    if team:
        t = team.lower().strip()
        team_var = f"DISCORD_ADMIN_WEBHOOK_URL_{t.upper()}"
        team_url = os.getenv(team_var, "").strip()
        if team_url:
            return team_url
    return os.getenv("DISCORD_ADMIN_WEBHOOK_URL", "").strip() or None


def is_admin_webhook_enabled(team: Optional[str] = None) -> bool:
    """True si un webhook admin (team-specific ou global) est configuré."""
    return bool(_get_admin_webhook_for_team(team))


async def send_admin_alert(
    title: str,
    message: str,
    level: str = "info",
    team: Optional[str] = None,
) -> bool:
    """
    Envoie une alerte admin via webhook dédié.
    level : 'info' (bleu), 'warning' (orange), 'error' (rouge)
    team  : 'geelark' / 'instagram' / 'threads' → route vers le webhook
            de cette équipe si défini, sinon fallback sur le webhook global.
    """
    webhook_url = _get_admin_webhook_for_team(team)
    if not webhook_url:
        logger.debug(f"Admin webhook non configuré (team={team}), alerte ignorée")
        return False

    color_map = {
        "info": 0x3498DB,      # bleu
        "warning": 0xE67E22,   # orange
        "error": 0xE74C3C,     # rouge
    }
    color = color_map.get(level, 0x95A5A6)

    icon = "ℹ️" if level == "info" else "⚠️"
    payload = {
        "embeds": [
            {
                "title": f"{icon} {title}",
                "description": message,
                "color": color,
            }
        ]
    }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(webhook_url, json=payload, timeout=10) as resp:
                if resp.status in (200, 204):
                    logger.info(f"Admin alert envoyée : {title}")
                    return True
                logger.warning(f"Admin alert HTTP {resp.status}")
                return False
    except Exception as e:
        logger.warning(f"Admin alert exception: {e}")
        return False
