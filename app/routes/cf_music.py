"""
ClipFusion — Music library : upload MP3 ou extraction audio depuis vidéo.
Routes montées sous /api/clipfusion/music/...
"""
import shutil
import subprocess
import uuid
from pathlib import Path
from typing import List

from fastapi import APIRouter, File, HTTPException, UploadFile

from app.services import cf_storage as storage
from app.utils.logger import get_logger
from app.utils.storage_paths import MUSIC_DIR

logger = get_logger("cf_music")

router = APIRouter(prefix="/api/clipfusion/music", tags=["clipfusion-music"])

# Stockage musique : volume persistant /data si dispo, sinon /tmp
MUSIC_DIR.mkdir(parents=True, exist_ok=True)

AUDIO_EXTS = {".mp3", ".wav", ".m4a", ".aac", ".ogg"}
VIDEO_EXTS = {".mp4", ".mov", ".webm", ".mkv", ".avi"}


@router.post("/upload")
async def upload_music(files: List[UploadFile] = File(...)):
    saved = []
    for f in files:
        ext = Path(f.filename).suffix.lower()
        save_name = f"{uuid.uuid4().hex}{ext}"
        save_path = MUSIC_DIR / save_name

        if ext in VIDEO_EXTS:
            # Extraction audio depuis vidéo
            tmp_path = MUSIC_DIR / f"tmp_{save_name}"
            with open(tmp_path, "wb") as out:
                shutil.copyfileobj(f.file, out)
            mp3_name = f"{uuid.uuid4().hex}.mp3"
            mp3_path = MUSIC_DIR / mp3_name
            try:
                subprocess.run(
                    ["ffmpeg", "-y", "-i", str(tmp_path), "-vn",
                     "-acodec", "libmp3lame", "-b:a", "192k", str(mp3_path)],
                    capture_output=True, timeout=300,
                )
                tmp_path.unlink(missing_ok=True)
                size_b = mp3_path.stat().st_size if mp3_path.exists() else 0
                meta = storage.add_music(
                    filename=mp3_name, path=str(mp3_path),
                    original_name=f.filename, size_bytes=size_b,
                )
                if meta:
                    saved.append(meta)
            except Exception as e:
                tmp_path.unlink(missing_ok=True)
                logger.exception(f"Audio extract failed pour {f.filename}")
                raise HTTPException(500, f"Audio extract failed: {e}")
        elif ext in AUDIO_EXTS:
            with open(save_path, "wb") as out:
                shutil.copyfileobj(f.file, out)
            try:
                size_b = save_path.stat().st_size
            except Exception:
                size_b = 0
            meta = storage.add_music(
                filename=save_name, path=str(save_path),
                original_name=f.filename, size_bytes=size_b,
            )
            if meta:
                saved.append(meta)
    return {"saved": saved}


@router.get("/")
async def list_music():
    return storage.list_music()


@router.delete("/{music_id}")
async def delete_music(music_id: str):
    ok = storage.delete_music(music_id)
    if not ok:
        raise HTTPException(404, "Music not found")
    return {"ok": True}


@router.delete("/")
async def clear_music():
    count = storage.clear_music()
    return {"ok": True, "deleted": count}
