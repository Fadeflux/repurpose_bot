"""
ClipFusion — Debug routes : info sur le stockage et l'environnement.
À utiliser pour diagnostiquer les problèmes de volume Railway.
"""
import os
from pathlib import Path

from fastapi import APIRouter

from app.utils.storage_paths import (
    BASE_DIR,
    VIDEO_DIR,
    MUSIC_DIR,
    OUTPUT_DIR,
    TEMPLATE_DIR,
    UPLOAD_DIR,
    is_persistent,
)

router = APIRouter(prefix="/api/clipfusion/debug", tags=["clipfusion-debug"])


@router.get("/storage")
async def storage_info():
    """Retourne tout ce qu'il faut savoir sur où l'app stocke les fichiers."""
    data_root = Path("/data")
    tmp_root = Path("/tmp")

    # Test d'écriture sur /data
    data_writable = False
    data_write_error = None
    if data_root.exists():
        try:
            test = data_root / ".cf_debug_test"
            test.touch()
            test.unlink()
            data_writable = True
        except Exception as e:
            data_write_error = str(e)

    # Liste ce qu'il y a dans /data si ça existe
    data_contents = []
    if data_root.exists():
        try:
            data_contents = [str(p) for p in data_root.iterdir()][:20]
        except Exception as e:
            data_contents = [f"<erreur lecture: {e}>"]

    # Liste ce qu'il y a dans VIDEO_DIR si ça existe
    video_dir_exists = VIDEO_DIR.exists()
    video_dir_count = 0
    if video_dir_exists:
        try:
            video_dir_count = sum(1 for _ in VIDEO_DIR.iterdir())
        except Exception:
            video_dir_count = -1

    return {
        "BASE_DIR": str(BASE_DIR),
        "is_persistent": is_persistent(),
        "VIDEO_DIR": str(VIDEO_DIR),
        "VIDEO_DIR_exists": video_dir_exists,
        "VIDEO_DIR_file_count": video_dir_count,
        "MUSIC_DIR": str(MUSIC_DIR),
        "OUTPUT_DIR": str(OUTPUT_DIR),
        "TEMPLATE_DIR": str(TEMPLATE_DIR),
        "UPLOAD_DIR": str(UPLOAD_DIR),
        "/data_exists": data_root.exists(),
        "/data_is_dir": data_root.is_dir() if data_root.exists() else False,
        "/data_writable": data_writable,
        "/data_write_error": data_write_error,
        "/data_contents_sample": data_contents,
        "/tmp_exists": tmp_root.exists(),
        "env_CLIPFUSION_STORAGE_DIR": os.environ.get("CLIPFUSION_STORAGE_DIR", "<not set>"),
        "env_RAILWAY_VOLUME_MOUNT_PATH": os.environ.get("RAILWAY_VOLUME_MOUNT_PATH", "<not set>"),
        "env_RAILWAY_VOLUME_NAME": os.environ.get("RAILWAY_VOLUME_NAME", "<not set>"),
    }
