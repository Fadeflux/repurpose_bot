"""
Service pour synchroniser la liste des VA depuis Discord.

Utilise le bot Discord (pas le webhook) pour scraper tous les membres
ayant un rôle spécifique (ex: "VA Geelark").

Variables d'environnement :
  DISCORD_BOT_TOKEN  : Token du bot Discord
  DISCORD_GUILD_ID   : ID du serveur Discord
  DISCORD_VA_ROLE_NAME : Nom exact du rôle à scraper (défaut: "VA Geelark")

Permissions requises du bot :
  - View Server Members
  - Server Members Intent activé dans le portail Discord
"""
import asyncio
import json
import os
from datetime import datetime
from pathlib import Path
from typing import List, Optional

import aiohttp

from app.utils.logger import get_logger

logger = get_logger("discord_va_sync")


# Fichier de cache des VA (persistant sur disque)
VA_CACHE_FILE = Path("/home/claude/repurpose_bot/va_cache.json")
# En prod Railway, stocke à la racine de l'app
if os.path.exists("/app"):
    VA_CACHE_FILE = Path("/app/va_cache.json")


def _get_bot_token() -> Optional[str]:
    return os.getenv("DISCORD_BOT_TOKEN")


def _get_guild_id() -> Optional[str]:
    return os.getenv("DISCORD_GUILD_ID")


def _get_va_role_id() -> Optional[str]:
    """ID du rôle VA (prioritaire si défini)."""
    return os.getenv("DISCORD_VA_ROLE_ID")


def _get_va_role_name() -> str:
    """Nom du rôle (fallback si pas d'ID)."""
    return os.getenv("DISCORD_VA_ROLE_NAME", "VA Geelark")


def is_va_sync_enabled() -> bool:
    """Retourne True si la config est complète pour la sync."""
    return bool(_get_bot_token() and _get_guild_id())


async def _fetch_guild_roles(session: aiohttp.ClientSession, token: str, guild_id: str) -> List[dict]:
    """Récupère tous les rôles du serveur."""
    url = f"https://discord.com/api/v10/guilds/{guild_id}/roles"
    headers = {"Authorization": f"Bot {token}"}
    async with session.get(url, headers=headers, timeout=15) as resp:
        if resp.status != 200:
            body = await resp.text()
            raise RuntimeError(f"Discord API /roles failed: {resp.status} - {body[:200]}")
        return await resp.json()


async def _fetch_guild_members(
    session: aiohttp.ClientSession,
    token: str,
    guild_id: str,
) -> List[dict]:
    """
    Récupère tous les membres du serveur (pagination par batch de 1000).
    Nécessite que "Server Members Intent" soit activé sur le bot.
    """
    headers = {"Authorization": f"Bot {token}"}
    all_members = []
    after = "0"
    while True:
        url = f"https://discord.com/api/v10/guilds/{guild_id}/members?limit=1000&after={after}"
        async with session.get(url, headers=headers, timeout=20) as resp:
            if resp.status != 200:
                body = await resp.text()
                raise RuntimeError(
                    f"Discord API /members failed: {resp.status} - {body[:200]}. "
                    "Vérifie que 'Server Members Intent' est activé dans le portail Discord."
                )
            batch = await resp.json()
            if not batch:
                break
            all_members.extend(batch)
            # Pagination : l'ID du dernier membre sert de cursor
            after = batch[-1]["user"]["id"]
            if len(batch) < 1000:
                break
    return all_members


async def fetch_va_members_from_discord() -> List[dict]:
    """
    Récupère la liste des VA depuis Discord.
    Retourne une liste de dicts {name, discord_id}, triée par nom.
    """
    token = _get_bot_token()
    guild_id = _get_guild_id()
    va_role_id_env = _get_va_role_id()
    role_name = _get_va_role_name()

    if not token or not guild_id:
        logger.warning("Discord VA sync désactivé (token ou guild_id manquant)")
        return []

    try:
        async with aiohttp.ClientSession() as session:
            va_role_id = None

            # Mode 1 : ID du rôle fourni via env var (plus fiable)
            if va_role_id_env:
                va_role_id = va_role_id_env.strip()
                logger.info(f"Utilisation de DISCORD_VA_ROLE_ID={va_role_id}")
            else:
                # Mode 2 : recherche par nom
                roles = await _fetch_guild_roles(session, token, guild_id)
                va_role = next(
                    (r for r in roles if r["name"].lower() == role_name.lower()),
                    None,
                )
                if not va_role:
                    available = [r["name"] for r in roles]
                    logger.error(
                        f"Rôle '{role_name}' introuvable dans le serveur. "
                        f"Rôles disponibles: {available}"
                    )
                    return []
                va_role_id = va_role["id"]
                logger.info(f"Rôle '{role_name}' trouvé (ID={va_role_id})")

            # Récupère tous les membres du serveur
            members = await _fetch_guild_members(session, token, guild_id)
            logger.info(f"{len(members)} membre(s) récupéré(s) depuis Discord")

            # Filtre ceux qui ont le rôle VA
            va_list = []
            for m in members:
                if va_role_id in m.get("roles", []):
                    user = m.get("user", {})
                    name = (
                        m.get("nick")
                        or user.get("global_name")
                        or user.get("username")
                        or ""
                    )
                    discord_id = user.get("id")
                    if name and discord_id:
                        va_list.append({"name": name, "discord_id": discord_id})

            va_list.sort(key=lambda v: v["name"].lower())
            logger.info(f"{len(va_list)} VA trouvé(s) avec le rôle ID={va_role_id}")
            return va_list
    except Exception as e:
        logger.exception(f"Erreur fetch VA Discord: {e}")
        return []


def load_cached_vas() -> dict:
    """Charge la liste des VA depuis le cache disque."""
    if not VA_CACHE_FILE.exists():
        return {"vas": [], "last_sync": None}
    try:
        return json.loads(VA_CACHE_FILE.read_text(encoding="utf-8"))
    except Exception as e:
        logger.warning(f"Cache VA corrompu: {e}")
        return {"vas": [], "last_sync": None}


def save_cached_vas(vas: List[str]) -> None:
    """Sauve la liste des VA sur le cache disque."""
    data = {
        "vas": vas,
        "last_sync": datetime.utcnow().isoformat() + "Z",
    }
    try:
        VA_CACHE_FILE.write_text(
            json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8"
        )
    except Exception as e:
        logger.error(f"Impossible d'écrire le cache VA: {e}")


async def sync_va_list() -> dict:
    """
    Lance une sync manuelle avec Discord.
    Préserve les emails stockés pour les VA qui restent.
    Retire automatiquement les emails des VA qui quittent.
    """
    existing = load_cached_vas()
    old_list = existing.get("vas", [])

    # Construit un map email par discord_id pour préserver les emails des VA qui restent
    old_email_by_id = {}
    old_names = set()
    for v in old_list:
        if isinstance(v, str):
            old_names.add(v)
        elif isinstance(v, dict):
            old_names.add(v.get("name", ""))
            did = v.get("discord_id")
            email = v.get("email")
            if did and email:
                old_email_by_id[did] = email

    new_vas = await fetch_va_members_from_discord()
    if not new_vas and old_names:
        logger.warning("Discord n'a rien retourné, garde le cache existant")
        return {
            "vas": old_list,
            "last_sync": existing.get("last_sync"),
            "added": [],
            "removed": [],
            "from_cache": True,
        }

    # Ré-injecte les emails des VA qui sont toujours présents
    for v in new_vas:
        did = v.get("discord_id")
        if did and did in old_email_by_id:
            v["email"] = old_email_by_id[did]

    new_names = {v["name"] for v in new_vas}
    added = sorted(new_names - old_names)
    removed = sorted(old_names - new_names)
    # Logs utiles : quels emails ont été supprimés car VA parti
    dropped_emails = []
    for v in old_list:
        if isinstance(v, dict) and v.get("email"):
            did = v.get("discord_id")
            still_here = any(n.get("discord_id") == did for n in new_vas)
            if not still_here:
                dropped_emails.append(v.get("email"))
    if dropped_emails:
        logger.info(f"Emails supprimés (VA partis) : {dropped_emails}")

    save_cached_vas(new_vas)
    logger.info(f"VA sync: {len(new_vas)} total, +{len(added)} ajoutés, -{len(removed)} supprimés")

    return {
        "vas": new_vas,
        "last_sync": datetime.utcnow().isoformat() + "Z",
        "added": added,
        "removed": removed,
        "dropped_emails": dropped_emails,
        "from_cache": False,
    }


def find_va_discord_id(name: str) -> Optional[str]:
    """Retourne l'ID Discord d'un VA à partir de son nom (via le cache)."""
    if not name:
        return None
    data = load_cached_vas()
    for v in data.get("vas", []):
        if isinstance(v, dict) and v.get("name", "").lower() == name.lower():
            return v.get("discord_id")
    return None


def find_va_by_discord_id(discord_id: str) -> Optional[dict]:
    """Retourne un VA complet par son Discord ID."""
    if not discord_id:
        return None
    data = load_cached_vas()
    for v in data.get("vas", []):
        if isinstance(v, dict) and str(v.get("discord_id")) == str(discord_id):
            return v
    return None


def get_all_va_emails() -> List[str]:
    """Retourne la liste des emails de tous les VA enregistrés."""
    data = load_cached_vas()
    emails = []
    for v in data.get("vas", []):
        if isinstance(v, dict) and v.get("email"):
            emails.append(v["email"])
    return emails


def set_va_email(discord_id: str, email: str) -> bool:
    """Met à jour l'email d'un VA identifié par son Discord ID."""
    if not discord_id or not email:
        return False
    data = load_cached_vas()
    vas = data.get("vas", [])
    updated = False
    for v in vas:
        if isinstance(v, dict) and str(v.get("discord_id")) == str(discord_id):
            v["email"] = email.lower().strip()
            updated = True
            break
    if updated:
        # Sauve avec le nouveau format
        data["vas"] = vas
        try:
            VA_CACHE_FILE.write_text(
                json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8"
            )
            logger.info(f"Email VA enregistré pour discord_id={discord_id}")
            return True
        except Exception as e:
            logger.error(f"Impossible de sauver email VA: {e}")
    return False


def get_all_va_emails() -> List[str]:
    """Retourne tous les emails des VA actuellement dans le cache."""
    data = load_cached_vas()
    emails = []
    for v in data.get("vas", []):
        if isinstance(v, dict) and v.get("email"):
            emails.append(v["email"])
    return emails


def update_va_email(discord_id: str, email: str) -> bool:
    """Met à jour l'email d'un VA identifié par son discord_id. Retourne True si OK."""
    if not discord_id:
        return False
    data = load_cached_vas()
    vas = data.get("vas", [])
    updated = False
    for v in vas:
        if isinstance(v, dict) and v.get("discord_id") == discord_id:
            email_clean = (email or "").strip().lower()
            if email_clean:
                v["email"] = email_clean
            else:
                v.pop("email", None)
            updated = True
            break
    if updated:
        save_cached_vas(vas)
    return updated


# ---------------------------------------------------------------------------
# Tâche planifiée : sync toutes les N heures en arrière-plan
# ---------------------------------------------------------------------------
_sync_task: Optional[asyncio.Task] = None
SYNC_INTERVAL_SECONDS = int(os.getenv("DISCORD_VA_SYNC_INTERVAL_SECONDS", "10800"))  # 3h par défaut


async def _periodic_sync_loop():
    """Boucle qui sync la liste toutes les 3h en arrière-plan."""
    # Première sync après 10 secondes (laisse l'app démarrer)
    await asyncio.sleep(10)
    while True:
        try:
            if is_va_sync_enabled():
                await sync_va_list()
        except Exception as e:
            logger.exception(f"Sync périodique échouée: {e}")
        # Attend avant la prochaine sync
        await asyncio.sleep(SYNC_INTERVAL_SECONDS)


def start_periodic_sync():
    """Démarre la tâche de sync en arrière-plan (à appeler au startup)."""
    global _sync_task
    if _sync_task is None or _sync_task.done():
        if is_va_sync_enabled():
            _sync_task = asyncio.create_task(_periodic_sync_loop())
            logger.info(
                f"Sync VA Discord démarrée (toutes les {SYNC_INTERVAL_SECONDS // 3600}h)"
            )
        else:
            logger.info("Sync VA Discord non activée (variables d'env manquantes)")
