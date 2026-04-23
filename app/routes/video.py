"""Routes API pour l'upload et le traitement des vidéos."""
import asyncio
import json
import re
import uuid
from datetime import datetime
from pathlib import Path
from typing import List, Optional

import aiofiles
from fastapi import APIRouter, File, Form, HTTPException, UploadFile
from fastapi.responses import FileResponse, JSONResponse

from app.config import OUTPUT_DIR, UPLOAD_DIR, settings, PARAM_RANGES
from app.services.drive_service import (
    create_batch_folder,
    get_folder_link,
    is_drive_enabled,
    upload_csv,
    upload_file,
)
from app.services.ffmpeg_service import process_video
from app.utils.logger import get_logger

router = APIRouter(prefix="/api", tags=["video"])
logger = get_logger("routes")


@router.get("/health")
async def health():
    return {
        "status": "ok",
        "app": settings.APP_NAME,
        "version": settings.VERSION,
        "drive_enabled": is_drive_enabled(),
    }


@router.get("/params")
async def get_param_ranges():
    """Expose les bornes min/max par défaut de chaque paramètre."""
    return {
        key: {"min": lo, "max": hi}
        for key, (lo, hi) in PARAM_RANGES.items()
    }


def _sanitize_batch_name(name: str) -> str:
    """Nettoie un nom de batch pour qu'il soit safe dans Drive / FS."""
    name = name.strip()
    if not name:
        name = f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    # Remplace tout caractère chelou par _
    name = re.sub(r"[^\w\s\-]", "_", name, flags=re.UNICODE)
    name = re.sub(r"\s+", "_", name)
    return name[:80]


@router.post("/process")
async def process_endpoint(
    files: List[UploadFile] = File(..., description="Une ou plusieurs vidéos sources"),
    batch_name: str = Form("", description="Nom du batch (sous-dossier Drive)"),
    copies_per_video: int = Form(1, ge=1, description="Nombre de variantes par vidéo"),
    concurrency: int = Form(3, ge=1, le=6, description="Processus ffmpeg parallèles"),
    upload_to_drive: bool = Form(True, description="Envoyer sur Google Drive"),
    custom_ranges: Optional[str] = Form(None),
    enabled_filters: Optional[str] = Form(None),
):
    """
    Upload une ou plusieurs vidéos, génère des variantes randomisées,
    et (optionnellement) uploade le tout sur Google Drive dans un sous-dossier
    nommé avec `batch_name`.
    """
    # -- Validations ---------------------------------------------------------
    if not files:
        raise HTTPException(status_code=400, detail="Aucun fichier fourni.")
    if len(files) > settings.MAX_FILES_PER_REQUEST:
        raise HTTPException(
            status_code=400,
            detail=f"Max {settings.MAX_FILES_PER_REQUEST} vidéos par requête.",
        )
    if copies_per_video > settings.MAX_COPIES_PER_REQUEST:
        raise HTTPException(
            status_code=400,
            detail=f"Max {settings.MAX_COPIES_PER_REQUEST} copies par vidéo.",
        )

    for f in files:
        ext = Path(f.filename or "").suffix.lower()
        if ext not in settings.ALLOWED_EXTENSIONS:
            raise HTTPException(
                status_code=400,
                detail=f"Extension {ext!r} non supportée ({f.filename}).",
            )

    # -- Parse JSON options --------------------------------------------------
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

    # -- Préparation batch ---------------------------------------------------
    batch_slug = _sanitize_batch_name(batch_name)
    job_id = uuid.uuid4().hex[:8]
    full_batch_id = f"{batch_slug}_{job_id}"

    # Dossier Drive (optionnel)
    drive_folder_id = None
    drive_folder_link = None
    if upload_to_drive and is_drive_enabled():
        drive_folder_id = create_batch_folder(batch_slug)
        if drive_folder_id:
            drive_folder_link = get_folder_link(drive_folder_id)
            logger.info(f"[{full_batch_id}] Drive folder: {drive_folder_link}")
        else:
            logger.warning(f"[{full_batch_id}] Création dossier Drive échouée")

    # -- Sauvegarde des fichiers sources ------------------------------------
    src_paths: List[Path] = []
    max_bytes = settings.MAX_UPLOAD_MB * 1024 * 1024

    try:
        for idx, f in enumerate(files):
            ext = Path(f.filename or "").suffix.lower()
            safe_orig = re.sub(r"[^\w\-.]", "_", Path(f.filename or f"src{idx}").stem)
            src_path = UPLOAD_DIR / f"{full_batch_id}_{idx:03d}_{safe_orig}{ext}"
            written = 0
            async with aiofiles.open(src_path, "wb") as out:
                while chunk := await f.read(1024 * 1024):
                    written += len(chunk)
                    if written > max_bytes:
                        await out.close()
                        src_path.unlink(missing_ok=True)
                        raise HTTPException(
                            status_code=413,
                            detail=f"Fichier trop volumineux ({f.filename}).",
                        )
                    await out.write(chunk)
            src_paths.append(src_path)
        logger.info(f"[{full_batch_id}] {len(src_paths)} vidéo(s) uploadée(s)")
    except HTTPException:
        for p in src_paths:
            p.unlink(missing_ok=True)
        raise
    except Exception as e:
        logger.exception("Erreur upload")
        for p in src_paths:
            p.unlink(missing_ok=True)
        raise HTTPException(status_code=500, detail=f"Erreur upload: {e}") from e

    # -- Traitement ----------------------------------------------------------
    all_results = []
    try:
        for src_idx, src in enumerate(src_paths):
            source_label = files[src_idx].filename or src.name
            per_video_job_id = f"{full_batch_id}_v{src_idx:03d}"
            results = await process_video(
                source=src,
                copies=copies_per_video,
                job_id=per_video_job_id,
                concurrency=concurrency,
                custom_ranges=parsed_ranges,
                enabled_filters=parsed_filters,
            )
            # On annote chaque résultat avec la source
            for r in results:
                r["source_file"] = source_label
                r["source_index"] = src_idx + 1
            all_results.extend(results)
    except RuntimeError as e:
        logger.error(f"[{full_batch_id}] {e}")
        raise HTTPException(status_code=500, detail=str(e)) from e
    except Exception as e:
        logger.exception(f"[{full_batch_id}] Erreur traitement")
        raise HTTPException(status_code=500, detail=f"Erreur traitement: {e}") from e
    finally:
        # On purge les sources pour économiser le disque
        for p in src_paths:
            p.unlink(missing_ok=True)

    success = [r for r in all_results if r.get("success")]
    failed = [r for r in all_results if not r.get("success")]

    # -- Upload Drive --------------------------------------------------------
    drive_uploads: List[dict] = []
    if drive_folder_id and success:
        logger.info(f"[{full_batch_id}] Upload Drive de {len(success)} vidéos")

        # Upload parallélisé mais limité (3 à la fois)
        sem = asyncio.Semaphore(3)

        async def _upload(r):
            async with sem:
                loop = asyncio.get_event_loop()
                res = await loop.run_in_executor(
                    None, upload_file, Path(r["path"]), drive_folder_id
                )
                if res:
                    r["drive_url"] = res.get("webViewLink")
                    r["drive_id"] = res.get("id")
                    return {"filename": r["filename"], "drive_url": res.get("webViewLink")}
                return None

        upload_results = await asyncio.gather(*[_upload(r) for r in success])
        drive_uploads = [u for u in upload_results if u]

        # CSV de métadonnées
        csv_rows = []
        for r in all_results:
            row = {
                "source_file": r.get("source_file"),
                "copy_index": r.get("copy_index"),
                "success": r.get("success"),
                "output_filename": r.get("filename", ""),
                "drive_url": r.get("drive_url", ""),
                "size_bytes": r.get("size_bytes", ""),
                "error": (r.get("error", "") or "")[:300],
            }
            # Aplatit les params
            params = r.get("params") or {}
            for k, v in params.items():
                row[f"param_{k}"] = v if v is not None else ""
            csv_rows.append(row)

        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            None, upload_csv, drive_folder_id, csv_rows, "metadata.csv"
        )

    return JSONResponse(
        content={
            "batch_id": full_batch_id,
            "batch_name": batch_slug,
            "sources_count": len(files),
            "copies_per_video": copies_per_video,
            "total_requested": len(files) * copies_per_video,
            "succeeded": len(success),
            "failed": len(failed),
            "drive": {
                "enabled": bool(drive_folder_id),
                "folder_id": drive_folder_id,
                "folder_url": drive_folder_link,
                "uploaded": len(drive_uploads),
            },
            "results": all_results,
            "download_base_url": "/api/download/",
        },
    )


@router.get("/download/{filename}")
async def download(filename: str):
    """Sert un fichier généré."""
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
