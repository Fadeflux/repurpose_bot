"""
ClipFusion — VA listing routes.
Réutilise discord_va_sync de Repurpose pour fournir la liste des VAs
au frontend ClipFusion (sélecteur dans étape 5).
"""
from typing import Any, Dict, List

from fastapi import APIRouter

from app.utils.logger import get_logger

logger = get_logger("cf_vas")

router = APIRouter(prefix="/api/clipfusion", tags=["clipfusion-vas"])


@router.get("/list-vas")
async def list_vas() -> Dict[str, Any]:
    """
    Retourne la liste des VAs groupés par équipe (geelark / instagram).
    Utilise le cache disque de discord_va_sync (alimenté par sync périodique).
    """
    try:
        from app.services.discord_va_sync import load_cached_vas
        cache = load_cached_vas()
        vas = cache.get("vas") or []

        # Group par team
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

        # Tri alphabétique dans chaque équipe
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


@router.post("/resync-vas")
async def resync_vas() -> Dict[str, Any]:
    """Force une resync des VAs depuis Discord (réutilise sync_va_list de Repurpose)."""
    try:
        from app.services.discord_va_sync import sync_va_list
        result = await sync_va_list()
        return {"ok": True, **result}
    except Exception as e:
        logger.error(f"resync_vas failed: {e}")
        return {"ok": False, "error": str(e)}
