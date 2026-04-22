"""Routes API pour l'upload et le traitement des vidéos."""
import json
import uuid
from pathlib import Path
from typing import Optional

import aiofiles
from fastapi import APIRouter, File, Form, HTTPException, UploadFile
from fastapi.responses import FileResponse, JSONResponse

from app.config import OUTPUT_DIR, UPLOAD_DIR, settings, PARAM_RANGES
from app.services.ffmpeg_service import process_video
from app.utils.logger import get_logger

router = APIRouter(prefix="/api", tags=["video"])
logger = get_logger("routes")


@router.get("/health")
async def health():
    return {"status": "ok", "app": settings.APP_NAME, "version": settings.VERSION}


@router.get("/params")
async def get_param_ranges():
    """Expose les bornes min/max par défaut de chaque paramètre."""
    return {
        key: {"min": lo, "max": hi}
        for key, (lo, hi) in PARAM_RANGES.items()
    }


@router.post("/process")
async def process_endpoint(
    file: UploadFile = File(..., description="Vidéo source"),
    copies: int = Form(1, ge=1, description="Nombre de copies à générer"),
    concurrency: int = Form(2, ge=1, le=4, description="Processus ffmpeg parallèles"),
    custom_ranges: Optional[str] = Form(
        None,
        description='JSON des bornes custom. Ex: {"speed":[1.02,1.05],"zoom":[1.0,1.1]}',
    ),
    enabled_filters: Optional[str] = Form(
        None,
        description='JSON de la liste des filtres actifs. Ex: ["speed","zoom","rotation"]',
    ),
):
    """
    Upload une vidéo, génère `copies` variantes randomisées et retourne
    la liste des fichiers produits dans /outputs.
    """
    # -- Validation extension -------------------------------------------------
    ext = Path(file.filename or "").suffix.lower()
    if ext not in settings.ALLOWED_EXTENSIONS:
        raise HTTPException(
            status_code=400,
            detail=f"Extension {ext!r} non supportée. Autorisées: {sorted(settings.ALLOWED_EXTENSIONS)}",
        )

    if copies > settings.MAX_COPIES_PER_REQUEST:
        raise HTTPException(
            status_code=400,
            detail=f"Maximum {settings.MAX_COPIES_PER_REQUEST} copies par requête.",
        )

    # -- Parse JSON des options ----------------------------------------------
    parsed_ranges = None
    parsed_filters = None
    try:
        if custom_ranges:
            raw = json.loads(custom_ranges)
            parsed_ranges = {k: tuple(v) for k, v in raw.items() if k in PARAM_RANGES}
        if enabled_filters:
            parsed_filters = [f for f in json.loads(enabled_filters) if f in PARAM_RANGES]
    except (json.JSONDecodeError, ValueError, TypeError) as e:
        raise HTTPException(status_code=400, detail=f"JSON invalide: {e}") from e

    # -- Sauvegarde du fichier source ----------------------------------------
    job_id = uuid.uuid4().hex[:12]
    src_path = UPLOAD_DIR / f"{job_id}{ext}"
    max_bytes = settings.MAX_UPLOAD_MB * 1024 * 1024
    written = 0

    try:
        async with aiofiles.open(src_path, "wb") as out:
            while chunk := await file.read(1024 * 1024):
                written += len(chunk)
                if written > max_bytes:
                    await out.close()
                    src_path.unlink(missing_ok=True)
                    raise HTTPException(
                        status_code=413,
                        detail=f"Fichier trop volumineux (> {settings.MAX_UPLOAD_MB} MB).",
                    )
                await out.write(chunk)
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Erreur durant l'upload")
        src_path.unlink(missing_ok=True)
        raise HTTPException(status_code=500, detail=f"Erreur upload: {e}") from e

    logger.info(f"[{job_id}] upload OK ({written/1024/1024:.1f} MB)")

    # -- Traitement ----------------------------------------------------------
    try:
        results = await process_video(
            source=src_path,
            copies=copies,
            job_id=job_id,
            concurrency=concurrency,
            custom_ranges=parsed_ranges,
            enabled_filters=parsed_filters,
        )
    except RuntimeError as e:
        logger.error(f"[{job_id}] {e}")
        raise HTTPException(status_code=500, detail=str(e)) from e
    except Exception as e:
        logger.exception(f"[{job_id}] Erreur traitement")
        raise HTTPException(status_code=500, detail=f"Erreur traitement: {e}") from e
    finally:
        src_path.unlink(missing_ok=True)

    success = [r for r in results if r.get("success")]
    failed = [r for r in results if not r.get("success")]

    return JSONResponse(
        status_code=200 if success else 500,
        content={
            "job_id": job_id,
            "requested": copies,
            "succeeded": len(success),
            "failed": len(failed),
            "results": results,
            "download_base_url": "/api/download/",
        },
    )


@router.get("/download/{filename}")
async def download(filename: str):
    """Sert un fichier généré. Blocage des path traversal."""
    if "/" in filename or "\\" in filename or ".." in filename:
        raise HTTPException(status_code=400, detail="Nom de fichier invalide.")

    path = OUTPUT_DIR / filename
    if not path.exists() or not path.is_file():
        raise HTTPException(status_code=404, detail="Fichier introuvable.")

    return FileResponse(path, media_type="video/mp4", filename=filename)


@router.get("/outputs")
async def list_outputs():
    """Liste les vidéos disponibles dans /outputs."""
    files = []
    for p in sorted(OUTPUT_DIR.glob("*.mp4"), key=lambda x: x.stat().st_mtime, reverse=True):
        files.append({
            "filename": p.name,
            "size_bytes": p.stat().st_size,
            "url": f"/api/download/{p.name}",
        })
    return {"count": len(files), "files": files}
