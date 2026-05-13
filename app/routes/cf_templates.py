"""
ClipFusion — Templates : list, delete, clear, export, import, edit, custom create.
Routes montées sous /api/clipfusion/templates/...
"""
import uuid
from pathlib import Path
from typing import Any, Dict

from fastapi import APIRouter, Body, File, Form, HTTPException, UploadFile

from app.services import cf_storage as storage
from app.utils.logger import get_logger
from app.utils.storage_paths import TEMPLATE_DIR as UPLOAD_DIR
from app.utils.upload_helper import save_upload_streaming

logger = get_logger("cf_templates")

router = APIRouter(prefix="/api/clipfusion/templates", tags=["clipfusion-templates"])

# Stockage thumbnails templates : volume persistant /data si dispo
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)


@router.get("/")
async def list_all():
    return storage.list_templates()


@router.delete("/{tpl_id}")
async def delete_one(tpl_id: str):
    ok = storage.delete_template(tpl_id)
    if not ok:
        raise HTTPException(404, "Template not found")
    return {"ok": True}


@router.delete("/")
async def clear_all():
    count = storage.clear_templates()
    return {"ok": True, "deleted": count}


@router.patch("/{tpl_id}")
async def edit_template(tpl_id: str, payload: Dict[str, Any] = Body(...)):
    """Update fields on a template."""
    fields = {}
    if "caption" in payload:
        cap = (payload["caption"] or "").strip()
        if not cap:
            raise HTTPException(400, "Caption cannot be empty")
        fields["caption"] = cap
    if "align" in payload:
        fields["align"] = payload["align"]
    if "music_name" in payload:
        fields["music_name"] = (payload["music_name"] or "").strip()
    if "is_favorite" in payload:
        fields["is_favorite"] = bool(payload["is_favorite"])
    if "is_selected" in payload:
        fields["is_selected"] = bool(payload["is_selected"])

    if not fields:
        raise HTTPException(400, "Aucun champ à modifier")
    updated = storage.update_template(tpl_id, fields)
    if not updated:
        raise HTTPException(404, "Template not found")
    return updated


@router.post("/select-all")
async def select_all(payload: Dict[str, Any] = Body(default={})):
    """Bulk-update selection. Modes : all | none | favorites | reset."""
    mode = (payload or {}).get("mode", "all")
    if mode not in ("all", "none", "favorites", "reset"):
        raise HTTPException(400, f"Mode invalide : {mode}")
    affected = storage.select_templates_bulk(mode)
    return {"ok": True, "mode": mode, "affected": affected}


@router.post("/custom")
async def create_custom(
    caption: str = Form(...),
    music_name: str = Form(""),
    align: str = Form("center"),
    image: UploadFile = File(None),
):
    """Create a custom template with optional image upload."""
    if not caption.strip():
        raise HTTPException(400, "Caption is empty")

    # Sauvegarde l'image d'abord pour avoir le path
    thumbnail_path = None
    if image and image.filename:
        ext = Path(image.filename).suffix.lower() or ".png"
        if ext in {".png", ".jpg", ".jpeg", ".webp", ".gif"}:
            image_name = f"{uuid.uuid4().hex}{ext}"
            save_path = UPLOAD_DIR / image_name
            try:
                await save_upload_streaming(image, save_path)
                thumbnail_path = str(save_path)
            except Exception as e:
                logger.error(f"Image upload failed: {e}")
                thumbnail_path = None
        else:
            # Ext non supportée : ferme quand même le UploadFile
            try:
                await image.close()
            except Exception:
                pass

    tpl = storage.add_template(
        caption=caption.strip(),
        music_name=music_name.strip(),
        align=align,
        thumbnail_path=thumbnail_path,
    )
    if not tpl:
        raise HTTPException(500, "Échec création template (vérifie DATABASE_URL)")
    return tpl


@router.get("/export")
async def export_all():
    return {"templates": storage.list_templates()}


@router.post("/import")
async def import_templates(payload: Dict[str, Any] = Body(...)):
    items = payload.get("templates", [])
    count = 0
    for it in items:
        cap = it.get("caption", "")
        if cap:
            ok = storage.add_template(
                caption=cap,
                music_name=it.get("music_name", ""),
                align=it.get("align", "center"),
            )
            if ok:
                count += 1
    return {"imported": count}
