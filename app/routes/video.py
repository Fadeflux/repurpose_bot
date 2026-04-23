"""Routes API pour l'upload et le traitement des vidéos."""
import asyncio
import json
import re
import uuid
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

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


# ---------------------------------------------------------------------------
# Tracking en mémoire du progrès des batches Drive
# {batch_id: {"total": N, "uploaded": X, "done": bool}}
# ---------------------------------------------------------------------------
_batch_progress: Dict[str, Dict] = {}
_MAX_TRACKED_BATCHES = 20  # garde en mémoire les 20 derniers batches


def _update_progress(batch_id: str, total: int = None, uploaded_delta: int = 0, done: bool = False):
    """Met à jour le progrès d'un batch en mémoire."""
    if batch_id not in _batch_progress:
        _batch_progress[batch_id] = {"total": 0, "uploaded": 0, "done": False}
    if total is not None:
        _batch_progress[batch_id]["total"] = total
    if uploaded_delta:
        _batch_progress[batch_id]["uploaded"] += uploaded_delta
    if done:
        _batch_progress[batch_id]["done"] = True
    # Nettoyage : garde seulement les N derniers
    if len(_batch_progress) > _MAX_TRACKED_BATCHES:
        oldest_keys = list(_batch_progress.keys())[:-_MAX_TRACKED_BATCHES]
        for k in oldest_keys:
            _batch_progress.pop(k, None)


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


@router.get("/progress/{batch_id}")
async def get_batch_progress(batch_id: str):
    """
    Retourne le progrès d'upload Drive pour un batch en cours.
    Utilisé par le frontend pour afficher une barre de progression.
    """
    info = _batch_progress.get(batch_id)
    if not info:
        return {"found": False, "total": 0, "uploaded": 0, "done": False}
    return {
        "found": True,
        "total": info["total"],
        "uploaded": info["uploaded"],
        "done": info["done"],
        "percent": round(100 * info["uploaded"] / max(1, info["total"]), 1),
    }


@router.get("/progress-current")
async def get_current_batch_progress():
    """
    Retourne le progrès du batch le plus récent non terminé.
    Permet au frontend de polling sans connaître l'ID à l'avance.
    """
    # Cherche le dernier batch non terminé
    for batch_id in reversed(list(_batch_progress.keys())):
        info = _batch_progress[batch_id]
        if not info["done"]:
            return {
                "found": True,
                "batch_id": batch_id,
                "total": info["total"],
                "uploaded": info["uploaded"],
                "done": False,
                "percent": round(100 * info["uploaded"] / max(1, info["total"]), 1),
            }
    # Sinon retourne le tout dernier (terminé)
    if _batch_progress:
        batch_id = list(_batch_progress.keys())[-1]
        info = _batch_progress[batch_id]
        return {
            "found": True,
            "batch_id": batch_id,
            "total": info["total"],
            "uploaded": info["uploaded"],
            "done": info["done"],
            "percent": round(100 * info["uploaded"] / max(1, info["total"]), 1),
        }
    return {"found": False, "total": 0, "uploaded": 0, "done": False}


@router.get("/drive-debug")
async def drive_debug():
    """
    Teste la configuration Drive et fait un upload de test.
    Utilisé pour diagnostiquer les problèmes d'upload.
    """
    import os
    from app.services.drive_service import get_drive_client, get_auth_mode

    oauth_raw = os.getenv("GOOGLE_OAUTH_TOKEN_JSON") or ""
    creds_raw = os.getenv("GOOGLE_CREDENTIALS_JSON") or ""

    result = {
        "env_vars": {
            "GOOGLE_OAUTH_TOKEN_JSON": {
                "present": bool(oauth_raw),
                "length": len(oauth_raw),
                "starts_with": oauth_raw[:20] if oauth_raw else None,
            },
            "GOOGLE_CREDENTIALS_JSON": {
                "present": bool(creds_raw),
                "length": len(creds_raw),
            },
            "GOOGLE_DRIVE_PARENT_ID": os.getenv("GOOGLE_DRIVE_PARENT_ID") or None,
        },
        "drive_enabled": is_drive_enabled(),
        "client_initialized": False,
        "auth_mode": None,
        "service_account_email": None,
        "test_folder_creation": None,
        "test_file_upload": None,
        "errors": [],
    }

    # Test 1 : client initialisé
    try:
        client = get_drive_client()
        if client is None:
            result["errors"].append("get_drive_client() a retourné None")
            return result
        result["client_initialized"] = True
        result["auth_mode"] = get_auth_mode()  # "oauth" ou "service_account"
    except Exception as e:
        result["errors"].append(f"Erreur init client: {type(e).__name__}: {e}")
        return result

    # Test 2 : récup email du service account
    try:
        creds_raw = os.getenv("GOOGLE_CREDENTIALS_JSON", "{}")
        creds_dict = json.loads(creds_raw) if creds_raw.startswith("{") else {}
        result["service_account_email"] = creds_dict.get("client_email")
    except Exception as e:
        result["errors"].append(f"Parse credentials: {e}")

    # Test 3 : création d'un dossier test
    try:
        test_folder_id = create_batch_folder("drive_test_diagnostic")
        if test_folder_id:
            result["test_folder_creation"] = {
                "success": True,
                "folder_id": test_folder_id,
                "folder_url": get_folder_link(test_folder_id),
            }
        else:
            result["test_folder_creation"] = {"success": False}
            result["errors"].append("create_batch_folder() a retourné None")
    except Exception as e:
        result["test_folder_creation"] = {"success": False, "error": str(e)}
        result["errors"].append(f"Création dossier: {type(e).__name__}: {e}")

    # Test 4 : upload d'un petit fichier texte test
    if result["test_folder_creation"] and result["test_folder_creation"].get("success"):
        try:
            test_file = OUTPUT_DIR / "drive_test_file.txt"
            test_file.write_text("Ceci est un fichier de test Drive")
            upload_result = upload_file(
                test_file,
                result["test_folder_creation"]["folder_id"],
                mime_type="text/plain",
            )
            if upload_result:
                result["test_file_upload"] = {
                    "success": True,
                    "file_id": upload_result.get("id"),
                    "file_url": upload_result.get("webViewLink"),
                }
            else:
                result["test_file_upload"] = {"success": False}
                result["errors"].append(
                    "upload_file() a retourné None. Cause probable : "
                    "le Service Account n'a pas de quota storage. "
                    "Solution : partager le dossier Drive parent avec l'email "
                    f"du service account ({result['service_account_email']}) "
                    "en tant qu'Editor, OU utiliser un Shared Drive."
                )
            test_file.unlink(missing_ok=True)
        except Exception as e:
            result["test_file_upload"] = {"success": False, "error": str(e)}
            result["errors"].append(f"Upload test: {type(e).__name__}: {e}")

    return result


def _sanitize_batch_name(name: str) -> str:
    """Nettoie un nom de batch pour qu'il soit safe dans Drive / FS."""
    name = name.strip()
    if not name:
        name = f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    name = re.sub(r"[^\w\s\-]", "_", name, flags=re.UNICODE)
    name = re.sub(r"\s+", "_", name)
    return name[:80]


async def _async_drive_upload(result: dict, folder_id: str, batch_id: str = None) -> None:
    """Upload un fichier vers Drive en arrière-plan. Modifie result in-place."""
    try:
        loop = asyncio.get_event_loop()
        res = await loop.run_in_executor(
            None, upload_file, Path(result["path"]), folder_id
        )
        if res:
            result["drive_url"] = res.get("webViewLink")
            result["drive_id"] = res.get("id")
            if batch_id:
                _update_progress(batch_id, uploaded_delta=1)
    except Exception as e:
        logger.warning(f"Drive upload échoué pour {result.get('filename')}: {e}")


@router.post("/process")
async def process_endpoint(
    files: List[UploadFile] = File(..., description="Une ou plusieurs vidéos sources"),
    batch_name: str = Form("", description="Nom du batch (sous-dossier Drive)"),
    copies_per_video: int = Form(1, ge=1, description="Nombre de variantes par vidéo"),
    concurrency: int = Form(4, ge=1, le=6, description="Processus ffmpeg parallèles"),
    upload_to_drive: bool = Form(True, description="Envoyer sur Google Drive"),
    device_choice: str = Form("mix_random", description="Type de device à simuler"),
    custom_ranges: Optional[str] = Form(None),
    enabled_filters: Optional[str] = Form(None),
):
    """
    Upload une ou plusieurs vidéos, génère des variantes randomisées,
    et (optionnellement) uploade le tout sur Google Drive en pipeline parallèle :
    dès qu'une vidéo est encodée, elle part sur Drive pendant que ffmpeg
    continue à traiter les suivantes.
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

    drive_folder_id = None
    drive_folder_link = None
    if upload_to_drive and is_drive_enabled():
        drive_folder_id = create_batch_folder(batch_slug)
        if drive_folder_id:
            drive_folder_link = get_folder_link(drive_folder_id)
            logger.info(f"[{full_batch_id}] Drive folder: {drive_folder_link}")

    # -- Sauvegarde des fichiers sources (EN PARALLÈLE) ----------------------
    src_paths: List[Path] = []
    max_bytes = settings.MAX_UPLOAD_MB * 1024 * 1024

    async def _save_one(idx: int, f: UploadFile) -> Path:
        """Sauvegarde un fichier source en streaming."""
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
        return src_path

    try:
        # Parallélise l'écriture sur disque des fichiers sources (gain sur gros batchs)
        src_paths = await asyncio.gather(*[_save_one(i, f) for i, f in enumerate(files)])
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

    # -- PIPELINE : ffmpeg + Drive upload en parallèle ----------------------
    # Dès qu'une vidéo est encodée, on lance son upload Drive sans attendre
    # les autres. Gros gain de temps sur les gros batchs.
    all_results: List[dict] = []
    drive_upload_tasks: List[asyncio.Task] = []

    # Initialise le tracking du progrès Drive
    if drive_folder_id:
        total_expected = len(src_paths) * copies_per_video
        _update_progress(full_batch_id, total=total_expected)

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
                device_choice=device_choice,
            )
            for r in results:
                r["source_file"] = source_label
                r["source_index"] = src_idx + 1
                # Lance l'upload Drive en background SI succès + Drive activé
                if r.get("success") and drive_folder_id:
                    task = asyncio.create_task(
                        _async_drive_upload(r, drive_folder_id, full_batch_id)
                    )
                    drive_upload_tasks.append(task)
            all_results.extend(results)
    except RuntimeError as e:
        logger.error(f"[{full_batch_id}] {e}")
        raise HTTPException(status_code=500, detail=str(e)) from e
    except Exception as e:
        logger.exception(f"[{full_batch_id}] Erreur traitement")
        raise HTTPException(status_code=500, detail=f"Erreur traitement: {e}") from e
    finally:
        for p in src_paths:
            p.unlink(missing_ok=True)

    # Attendre que TOUS les uploads Drive en cours soient terminés
    if drive_upload_tasks:
        logger.info(f"[{full_batch_id}] Finalisation {len(drive_upload_tasks)} uploads Drive")
        await asyncio.gather(*drive_upload_tasks, return_exceptions=True)

    # Marque le batch comme terminé pour le polling
    if drive_folder_id:
        _update_progress(full_batch_id, done=True)

    success = [r for r in all_results if r.get("success")]
    failed = [r for r in all_results if not r.get("success")]

    # Upload CSV de métadonnées (après tout le reste)
    drive_uploads_count = sum(1 for r in success if r.get("drive_url"))
    if drive_folder_id and success:
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
                "uploaded": drive_uploads_count,
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
