"""
ClipFusion — Content : upload + list + delete + filter raw videos.
Routes montées sous /api/clipfusion/content/...
"""
import shutil
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Body, File, Form, HTTPException, Query, UploadFile

from app.services import cf_storage as storage
from app.services import cf_video_scanner as video_scanner
from app.utils.logger import get_logger

logger = get_logger("cf_content")

router = APIRouter(prefix="/api/clipfusion/content", tags=["clipfusion-content"])

# Stockage des vidéos brutes uploadées en /tmp
VIDEO_DIR = Path("/tmp/clipfusion/videos")
VIDEO_DIR.mkdir(parents=True, exist_ok=True)

ALLOWED_EXTS = {".mp4", ".mov", ".m4v", ".webm", ".avi", ".mkv"}


@router.post("/upload")
async def upload_videos(
    files: List[UploadFile] = File(...),
    model_id: int = Form(..., description="Catégorie/modèle obligatoire"),
):
    """
    Upload de vidéos brutes. Le model_id (catégorie) est OBLIGATOIRE
    pour qu'on sache à quelle modèle ces vidéos appartiennent.
    """
    if not model_id:
        raise HTTPException(400, "model_id (catégorie) obligatoire pour upload de vidéos")

    # Vérifie que le modèle existe
    if not storage.get_model(model_id):
        raise HTTPException(400, f"Le modèle ID {model_id} n'existe pas. Crée-le d'abord.")

    saved = []
    for f in files:
        ext = Path(f.filename).suffix.lower()
        if ext not in ALLOWED_EXTS:
            continue
        save_name = f"{uuid.uuid4().hex}{ext}"
        save_path = VIDEO_DIR / save_name
        with open(save_path, "wb") as out:
            shutil.copyfileobj(f.file, out)
        try:
            size_bytes = save_path.stat().st_size
        except Exception:
            size_bytes = 0
        meta = storage.add_video(
            filename=save_name,
            path=str(save_path),
            original_name=f.filename,
            size_bytes=size_bytes,
            model_id=model_id,
        )
        if meta:
            saved.append(meta)
    return {"saved": saved}


@router.get("/")
async def list_videos(model_id: Optional[int] = Query(None)):
    """Liste les vidéos brutes. Filtre optionnel par catégorie/modèle."""
    return storage.list_videos(model_id=model_id)


@router.patch("/{vid_id}/model")
async def update_video_model(vid_id: str, model_id: Optional[int] = Form(None)):
    """Change la catégorie d'une vidéo existante (peut être vidée avec model_id=null)."""
    ok = storage.update_video_model(vid_id, model_id)
    if not ok:
        raise HTTPException(404, "Vidéo introuvable")
    return {"ok": True}


@router.delete("/{vid_id}")
async def delete_video(vid_id: str):
    ok = storage.delete_video(vid_id)
    if not ok:
        raise HTTPException(404, "Video not found")
    return {"ok": True}


@router.delete("/")
async def clear_videos():
    count = storage.clear_videos()
    return {"ok": True, "deleted": count}


@router.post("/filter")
async def filter_videos(payload: Dict[str, Any] = Body(default={})):
    """
    Applique les filtres auto sur toutes les vidéos uploadées.
    Body: { filter_horizontal, filter_talking, filter_captions, dry_run }
    """
    flag_h = bool(payload.get("filter_horizontal", True))
    flag_t = bool(payload.get("filter_talking", True))
    flag_c = bool(payload.get("filter_captions", True))
    dry_run = bool(payload.get("dry_run", False))

    videos = storage.list_videos()
    if not videos:
        return {"total": 0, "kept": 0, "dropped": 0, "deleted_ids": [], "details": []}

    details = []
    to_delete = []
    for v in videos:
        path = v["path"]
        if not Path(path).exists():
            continue

        info = video_scanner.get_video_info(path)
        item = {
            "id": v["id"],
            "original_name": v.get("original_name", v["filename"]),
            "kept": True,
            "reasons_dropped": [],
            "width": info.get("width", 0),
            "height": info.get("height", 0),
        }
        if flag_h and video_scanner.is_horizontal(path, info):
            item["kept"] = False
            item["reasons_dropped"].append("horizontale")
        if item["kept"] and flag_c and video_scanner.has_caption_overlay(path):
            item["kept"] = False
            item["reasons_dropped"].append("captions")
        if item["kept"] and flag_t and video_scanner.is_talking(path):
            item["kept"] = False
            item["reasons_dropped"].append("parle")

        details.append(item)
        if not item["kept"]:
            to_delete.append(v["id"])

    deleted_ids: List[str] = []
    if not dry_run and to_delete:
        deleted_count = storage.delete_videos_bulk(to_delete)
        deleted_ids = to_delete[:deleted_count]

    kept = sum(1 for d in details if d["kept"])
    dropped = sum(1 for d in details if not d["kept"])
    return {
        "total": len(details),
        "kept": kept,
        "dropped": dropped,
        "deleted_ids": deleted_ids,
        "dry_run": dry_run,
        "details": details,
    }
