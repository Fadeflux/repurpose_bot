"""
ClipFusion — VA listing + email management routes.
Réutilise discord_va_sync + va_emails_db de Repurpose.
"""
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Form

from app.utils.logger import get_logger

logger = get_logger("cf_vas")

router = APIRouter(prefix="/api/clipfusion", tags=["clipfusion-vas"])


@router.get("/list-vas")
async def list_vas() -> Dict[str, Any]:
    """
    Retourne la liste des VAs groupés par équipe (geelark / instagram).
    """
    try:
        from app.services.discord_va_sync import load_cached_vas
        cache = load_cached_vas()
        vas = cache.get("vas") or []

        by_team: Dict[str, List[Dict[str, str]]] = {"geelark": [], "instagram": []}
        for v in vas:
            if not isinstance(v, dict):
                continue
            name = v.get("name", "")
            did = v.get("discord_id", "")
            teams = v.get("teams") or [v.get("team", "")]
            for t in teams:
                if t in by_team:
                    by_team[t].append({"name": name, "discord_id": did})

        for t in by_team:
            by_team[t].sort(key=lambda x: x["name"].lower())

        return {
            "teams": by_team,
            "last_sync": cache.get("last_sync"),
            "total": sum(len(v) for v in by_team.values()),
        }
    except Exception as e:
        logger.error(f"list_vas failed: {e}")
        return {"teams": {"geelark": [], "instagram": []}, "last_sync": None, "total": 0}


@router.get("/list-vas-admin")
async def list_vas_admin() -> Dict[str, Any]:
    """
    Liste TOUS les VAs avec leurs emails actuels (panneau admin).
    Retourne : { vas: [{name, discord_id, team, email}], last_sync }
    """
    try:
        from app.services.discord_va_sync import load_cached_vas
        cache = load_cached_vas()
        vas_raw = cache.get("vas") or []

        # Récupère les emails depuis Postgres
        emails_map: Dict[str, str] = {}
        try:
            from app.services.va_emails_db import load_all_emails, is_db_enabled
            if is_db_enabled():
                emails_map = load_all_emails() or {}
        except Exception as e:
            logger.warning(f"Email load failed: {e}")

        # Construit la liste enrichie (un VA peut être dans plusieurs équipes,
        # on duplique alors la ligne pour qu'il apparaisse dans chaque tab)
        rows = []
        for v in vas_raw:
            if not isinstance(v, dict):
                continue
            name = v.get("name", "")
            did = v.get("discord_id", "")
            teams = v.get("teams") or [v.get("team", "")]
            email = emails_map.get(str(did), "") or ""
            for t in teams:
                if not t:
                    continue
                rows.append({
                    "name": name,
                    "discord_id": did,
                    "team": t,
                    "email": email,
                })

        # Tri par équipe puis par nom
        rows.sort(key=lambda x: (x["team"], x["name"].lower()))
        return {
            "vas": rows,
            "last_sync": cache.get("last_sync"),
            "total": len(rows),
        }
    except Exception as e:
        logger.error(f"list_vas_admin failed: {e}")
        return {"vas": [], "last_sync": None, "total": 0}


@router.post("/save-va-email")
async def save_va_email(
    discord_id: str = Form(...),
    email: str = Form(""),
) -> Dict[str, Any]:
    """
    Enregistre / met à jour l'email Gmail d'un VA dans Postgres.
    Email vide = suppression de l'entrée.
    """
    if not discord_id:
        return {"ok": False, "error": "discord_id manquant"}
    try:
        from app.services.va_emails_db import save_email, delete_email, is_db_enabled
        if not is_db_enabled():
            return {"ok": False, "error": "DATABASE_URL non configuré"}

        email = (email or "").strip().lower()
        if email:
            ok = save_email(discord_id, email)
        else:
            ok = delete_email(discord_id)
        return {"ok": bool(ok), "discord_id": discord_id, "email": email}
    except Exception as e:
        logger.error(f"save_va_email failed: {e}")
        return {"ok": False, "error": str(e)}


@router.post("/resync-vas")
async def resync_vas() -> Dict[str, Any]:
    """Force une resync des VAs depuis Discord."""
    try:
        from app.services.discord_va_sync import sync_va_list
        result = await sync_va_list()
        return {"ok": True, **result}
    except Exception as e:
        logger.error(f"resync_vas failed: {e}")
        return {"ok": False, "error": str(e)}

