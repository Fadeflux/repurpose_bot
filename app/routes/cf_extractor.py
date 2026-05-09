"""
ClipFusion — Extractor : créer des templates depuis screenshot OCR, manuel,
ou extraction vidéo (caption + audio).
Routes montées sous /api/clipfusion/extractor/...
"""
import shutil
import uuid
from pathlib import Path
from typing import Any, Dict

from fastapi import APIRouter, File, Form, HTTPException, UploadFile
from fastapi.responses import FileResponse

from app.services import cf_storage as storage
from app.services import cf_ocr as ocr
from app.services import cf_video_extractor as video_extractor
from app.utils.logger import get_logger
from app.utils.storage_paths import UPLOAD_DIR, MUSIC_DIR

logger = get_logger("cf_extractor")

router = APIRouter(prefix="/api/clipfusion/extractor", tags=["clipfusion-extractor"])

# Stockage uploads (screenshots/vidéos source) + musique : volume persistant
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
MUSIC_DIR.mkdir(parents=True, exist_ok=True)


# Sert les images uploadées (thumbnails templates) pour les afficher dans l'UI
@router.get("/uploads/{filename}")
async def serve_upload(filename: str):
    """Sert un thumbnail uploadé (image)."""
    safe_name = Path(filename).name  # neutralise les ../
    fpath = UPLOAD_DIR / safe_name
    if not fpath.exists():
        raise HTTPException(404, "Fichier introuvable")
    return FileResponse(fpath)


@router.post("/screenshot")
async def extract_from_screenshot(file: UploadFile = File(...)):
    """Upload image → OCR → retourne caption (sans créer de template)."""
    ext = Path(file.filename).suffix.lower() or ".png"
    save_name = f"{uuid.uuid4().hex}{ext}"
    save_path = UPLOAD_DIR / save_name
    with open(save_path, "wb") as f:
        shutil.copyfileobj(file.file, f)
    text = ocr.extract_text_from_image(str(save_path))
    return {
        "caption": text,
        "image": save_name,
        "image_url": f"/api/clipfusion/extractor/uploads/{save_name}",
    }


@router.post("/screenshot-auto")
async def screenshot_auto_template(
    file: UploadFile = File(...),
    align: str = Form("center"),
):
    """Upload screenshot + OCR + création template directe avec thumbnail."""
    ext = Path(file.filename).suffix.lower() or ".png"
    save_name = f"{uuid.uuid4().hex}{ext}"
    save_path = UPLOAD_DIR / save_name
    with open(save_path, "wb") as f:
        shutil.copyfileobj(file.file, f)

    text = ocr.extract_text_from_image(str(save_path))
    if not text:
        text = "(aucun texte détecté — édite ce template)"

    tpl = storage.add_template(
        caption=text,
        music_name="",
        align=align,
        thumbnail_path=str(save_path),
    )
    if not tpl:
        raise HTTPException(500, "Échec création template (DB ?)")
    # Pour l'UI on ajoute l'URL servie côté API
    tpl["image_url"] = f"/api/clipfusion/extractor/uploads/{save_name}"
    tpl["original_name"] = file.filename
    return tpl


@router.post("/manual")
async def create_template_manual(
    caption: str = Form(...),
    music_name: str = Form(""),
    align: str = Form("center"),
):
    """Create un template depuis une caption tapée manuellement."""
    if not caption.strip():
        raise HTTPException(400, "Caption is empty")
    tpl = storage.add_template(
        caption=caption.strip(), music_name=music_name.strip(), align=align
    )
    if not tpl:
        raise HTTPException(500, "Échec création template")
    return tpl


@router.post("/from-extracted")
async def save_extracted(
    caption: str = Form(...),
    music_name: str = Form(""),
    align: str = Form("center"),
    image: str = Form(""),  # save_name retourné par /screenshot
):
    """Save une caption OCR-extraite comme nouveau template."""
    if not caption.strip():
        raise HTTPException(400, "Caption is empty")
    thumb = None
    if image:
        candidate = UPLOAD_DIR / Path(image).name
        if candidate.exists():
            thumb = str(candidate)
    tpl = storage.add_template(
        caption=caption.strip(),
        music_name=music_name.strip(),
        align=align,
        thumbnail_path=thumb,
    )
    if not tpl:
        raise HTTPException(500, "Échec création template")
    if image:
        tpl["image_url"] = f"/api/clipfusion/extractor/uploads/{Path(image).name}"
    return tpl


@router.post("/video-extract")
async def video_extract(
    file: UploadFile = File(...),
    extract_text: str = Form("true"),
    extract_music: str = Form("true"),
    align: str = Form("center"),
):
    """
    Extrait caption (OCR sur frames) et/ou musique d'une vidéo.
    """
    do_text = str(extract_text).lower() in ("true", "1", "yes", "on")
    do_music = str(extract_music).lower() in ("true", "1", "yes", "on")
    if not do_text and not do_music:
        raise HTTPException(400, "Sélectionne au moins une option (texte ou musique)")

    ext = Path(file.filename).suffix.lower() or ".mp4"
    save_name = f"{uuid.uuid4().hex}{ext}"
    save_path = UPLOAD_DIR / save_name
    with open(save_path, "wb") as f:
        shutil.copyfileobj(file.file, f)

    result: Dict[str, Any] = {
        "filename": file.filename,
        "caption": "",
        "template": None,
        "music": None,
        "errors": [],
    }

    # Caption via OCR multi-frames
    if do_text:
        try:
            caption = video_extractor.extract_caption_from_video(str(save_path), num_frames=6)
            result["caption"] = caption
            if caption:
                tpl = storage.add_template(caption=caption, music_name="", align=align)
                if tpl:
                    tpl["original_name"] = file.filename
                    tpl["source"] = "video"
                    result["template"] = tpl
            else:
                result["errors"].append("Aucun texte détecté dans la vidéo")
        except Exception as e:
            logger.exception("OCR vidéo échec")
            result["errors"].append(f"OCR vidéo échec: {e}")

    # Audio extraction
    if do_music:
        try:
            mp3_name = f"{uuid.uuid4().hex}.mp3"
            mp3_path = MUSIC_DIR / mp3_name
            ok = video_extractor.extract_audio_to_mp3(str(save_path), str(mp3_path))
            if ok:
                size_b = mp3_path.stat().st_size if mp3_path.exists() else 0
                meta = storage.add_music(
                    filename=mp3_name,
                    path=str(mp3_path),
                    original_name=file.filename,
                    size_bytes=size_b,
                )
                result["music"] = meta
            else:
                result["errors"].append("Extraction audio échouée")
        except Exception as e:
            logger.exception("Audio extract échec")
            result["errors"].append(f"Audio extract échec: {e}")

    # Cleanup la vidéo source (on n'a besoin que des extraits)
    try:
        save_path.unlink(missing_ok=True)
    except Exception:
        pass

    return result
