"""
ClipFusion — Historique des mix (panneau étape 6).
Endpoints :
  - GET  /api/clipfusion/history           : liste des batches avec filtres
  - GET  /api/clipfusion/history/stats     : stats globales (total, aujourd'hui, etc.)
  - DELETE /api/clipfusion/history/{id}    : supprime un batch de l'historique
"""
from typing import Any, Dict, Optional

from fastapi import APIRouter, HTTPException, Query

from app.services import cf_storage
from app.utils.logger import get_logger

logger = get_logger("cf_history")

router = APIRouter(prefix="/api/clipfusion/history", tags=["clipfusion-history"])


@router.get("/")
async def list_history(
    period: str = Query("all", description="Preset : today | yesterday | 7d | 15d | 30d | all | custom"),
    start_date: Optional[str] = Query(None, description="ISO date YYYY-MM-DD (si period=custom)"),
    end_date: Optional[str] = Query(None, description="ISO date YYYY-MM-DD"),
    va_name: Optional[str] = Query(None),
    team: Optional[str] = Query(None),
    limit: int = Query(200),
) -> Dict[str, Any]:
    """Retourne la liste des batches selon les filtres période + VA + équipe."""
    period_days: Optional[int] = None
    sd = start_date
    ed = end_date

    p = (period or "all").lower()
    if p == "today":
        period_days = 0  # géré côté SQL via CURRENT_DATE
        # Trick: NOW() - INTERVAL '0 days' = aujourd'hui min nuit, on filtre côté SQL
        # Mais cf_storage.list_batches fait NOW() - INTERVAL n. Pour today on force start_date.
        from datetime import date
        sd = date.today().isoformat()
        ed = sd
    elif p == "yesterday":
        from datetime import date, timedelta
        y = (date.today() - timedelta(days=1)).isoformat()
        sd = y
        ed = y
    elif p == "7d":
        period_days = 7
    elif p == "15d":
        period_days = 15
    elif p == "30d":
        period_days = 30
    elif p == "custom":
        # start_date et end_date utilisés tels quels
        pass
    elif p == "all":
        # Pas de filtre date
        period_days = None

    # Si va_name est "" string vide, traiter comme None
    va_name = va_name or None
    team = team or None

    batches = cf_storage.list_batches(
        period_days=period_days,
        start_date=sd,
        end_date=ed,
        va_name=va_name,
        team=team,
        limit=limit,
    )

    return {
        "batches": batches,
        "count": len(batches),
        "filters": {
            "period": p,
            "start_date": sd,
            "end_date": ed,
            "va_name": va_name,
            "team": team,
        },
    }


@router.get("/stats")
async def history_stats() -> Dict[str, Any]:
    """Stats globales pour le header du panneau historique."""
    return cf_storage.get_batches_stats()


@router.delete("/{batch_id}")
async def delete_history(batch_id: str) -> Dict[str, Any]:
    """Supprime un batch de l'historique (n'efface PAS le dossier Drive lui-même)."""
    ok = cf_storage.delete_batch(batch_id)
    if not ok:
        raise HTTPException(404, "Batch introuvable")
    return {"ok": True}
