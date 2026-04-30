"""
Routes de test Geelark — à utiliser pour valider la connexion avant
d'intégrer Geelark dans le flow de batch.
"""
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from app.services import geelark_service
from app.utils.logger import get_logger

logger = get_logger("geelark_routes")

router = APIRouter(prefix="/api/geelark", tags=["geelark"])


@router.get("/test")
async def test_connection():
    """
    Test la connexion à l'API Geelark.
    Liste les groupes existants pour valider auth + accès.
    """
    if not geelark_service.is_geelark_enabled():
        raise HTTPException(
            status_code=400,
            detail="Geelark non configuré. Définis GEELARK_APP_ID et GEELARK_API_KEY.",
        )
    try:
        groups = await geelark_service.list_groups()
        return {"ok": True, "groups": groups, "count": len(groups)}
    except Exception as e:
        logger.exception("Geelark test failed")
        raise HTTPException(status_code=500, detail=f"Geelark test failed: {e}") from e


@router.get("/phones")
async def list_phones(group: str = Query(..., description="Nom du groupe Geelark")):
    """
    Liste les phones d'un groupe précis.
    Utile pour vérifier que les noms de groupes Discord matchent côté Geelark.
    """
    if not geelark_service.is_geelark_enabled():
        raise HTTPException(status_code=400, detail="Geelark non configuré.")
    try:
        phones = await geelark_service.list_phones_by_group(group)
        # Renvoie une vue simplifiée pour le debug
        simplified = [
            {
                "id": p.get("id"),
                "serialName": p.get("serialName"),
                "status": p.get("status"),
                "group": (p.get("group") or {}).get("name"),
            }
            for p in phones
        ]
        return {"ok": True, "group": group, "count": len(simplified), "phones": simplified}
    except Exception as e:
        logger.exception("Geelark list_phones failed")
        raise HTTPException(status_code=500, detail=str(e)) from e
