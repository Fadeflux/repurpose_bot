"""
ClipFusion — Gestion des modèles (créatrices).
Endpoints :
  - GET    /api/clipfusion/models           : liste
  - POST   /api/clipfusion/models           : créer
  - PATCH  /api/clipfusion/models/{id}      : renommer
  - DELETE /api/clipfusion/models/{id}      : supprimer
"""
from typing import Any, Dict

from fastapi import APIRouter, Form, HTTPException

from app.services import cf_storage
from app.utils.logger import get_logger

logger = get_logger("cf_models")

router = APIRouter(prefix="/api/clipfusion/models", tags=["clipfusion-models"])


@router.get("/")
async def list_models() -> Dict[str, Any]:
    """Retourne la liste des modèles enregistrés."""
    models = cf_storage.list_models()
    return {"models": models, "total": len(models)}


@router.post("/")
async def create_model(label: str = Form("")) -> Dict[str, Any]:
    """
    Crée un nouveau modèle.
    Si label vide, l'ID auto-généré sera utilisé comme label par défaut ("Modele 1", "Modele 2", etc.)
    """
    model = cf_storage.add_model(label.strip())
    if not model:
        raise HTTPException(500, "Création échouée (DATABASE_URL ?)")
    return {"ok": True, "model": model}


@router.patch("/{model_id}")
async def rename_model(model_id: int, label: str = Form(...)) -> Dict[str, Any]:
    """Renomme un modèle existant (change son label)."""
    clean = (label or "").strip()
    if not clean:
        raise HTTPException(400, "Le nouveau nom ne peut pas être vide")
    ok = cf_storage.rename_model(model_id, clean)
    if not ok:
        raise HTTPException(404, "Modèle introuvable")
    return {"ok": True, "label": clean}


@router.delete("/{model_id}")
async def delete_model(model_id: int) -> Dict[str, Any]:
    """Supprime un modèle par ID."""
    ok = cf_storage.delete_model(model_id)
    if not ok:
        raise HTTPException(404, "Modèle introuvable")
    return {"ok": True}
