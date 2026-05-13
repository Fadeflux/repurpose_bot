"""
ClipFusion — Debug routes : info sur le stockage et l'environnement.
À utiliser pour diagnostiquer les problèmes de volume Railway.
"""
import os
import shutil
import time
from pathlib import Path
from typing import Dict, List, Any

from fastapi import APIRouter, HTTPException, Query

from app.utils.logger import get_logger
from app.utils.storage_paths import (
    BASE_DIR,
    VIDEO_DIR,
    MUSIC_DIR,
    OUTPUT_DIR,
    TEMPLATE_DIR,
    UPLOAD_DIR,
    is_persistent,
)

logger = get_logger("cf_debug")

router = APIRouter(prefix="/api/clipfusion/debug", tags=["clipfusion-debug"])


def _dir_size(path: Path) -> int:
    """Retourne la taille totale d'un dossier (en bytes), récursivement."""
    total = 0
    try:
        for entry in path.rglob("*"):
            if entry.is_file():
                try:
                    total += entry.stat().st_size
                except Exception:
                    pass
    except Exception:
        pass
    return total


def _format_size(bytes_count: int) -> str:
    """Format human-readable de la taille."""
    units = ["B", "KB", "MB", "GB", "TB"]
    n = float(bytes_count)
    i = 0
    while n >= 1024 and i < len(units) - 1:
        n /= 1024
        i += 1
    return f"{n:.2f} {units[i]}"


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


@router.get("/disk-usage")
async def disk_usage():
    """
    Analyse précise de l'espace disque utilisé par chaque dossier ClipFusion.
    SAFE : lecture seule, ne supprime RIEN.

    À appeler quand "No space left on device" → permet de voir quel dossier
    bouffe la place.
    """
    # Espace total/utilisé/libre sur le volume
    try:
        stat = shutil.disk_usage(str(BASE_DIR.parent))
        total = stat.total
        used = stat.used
        free = stat.free
        pct_used = round((used / total) * 100, 1) if total else 0
    except Exception as e:
        return {"error": f"disk_usage failed: {e}"}

    # Taille de chaque dossier ClipFusion
    dirs_info = {}
    for name, path in [
        ("videos", VIDEO_DIR),
        ("music", MUSIC_DIR),
        ("output", OUTPUT_DIR),
        ("templates", TEMPLATE_DIR),
        ("uploads", UPLOAD_DIR),
    ]:
        if path.exists():
            size = _dir_size(path)
            count = sum(1 for f in path.rglob("*") if f.is_file())
            dirs_info[name] = {
                "path": str(path),
                "size_bytes": size,
                "size_readable": _format_size(size),
                "file_count": count,
            }
        else:
            dirs_info[name] = {
                "path": str(path),
                "exists": False,
            }

    # Top 10 plus gros fichiers dans OUTPUT_DIR (candidats à nettoyer)
    biggest_outputs = []
    if OUTPUT_DIR.exists():
        try:
            files_with_size = []
            for f in OUTPUT_DIR.rglob("*"):
                if f.is_file():
                    try:
                        files_with_size.append((f, f.stat().st_size, f.stat().st_mtime))
                    except Exception:
                        pass
            files_with_size.sort(key=lambda x: x[1], reverse=True)
            for f, size, mtime in files_with_size[:10]:
                biggest_outputs.append({
                    "path": str(f),
                    "size": _format_size(size),
                    "age_hours": round((time.time() - mtime) / 3600, 1),
                })
        except Exception as e:
            biggest_outputs = [{"error": str(e)}]

    return {
        "volume": {
            "mount_point": str(BASE_DIR.parent),
            "total": _format_size(total),
            "used": _format_size(used),
            "free": _format_size(free),
            "pct_used": pct_used,
        },
        "dirs": dirs_info,
        "biggest_outputs_top10": biggest_outputs,
    }


@router.post("/cleanup-safe")
async def cleanup_safe(
    confirm: str = Query("", description="Doit valoir YES_CLEAN pour supprimer"),
    older_than_hours: int = Query(0, description="Ne supprimer que les fichiers plus vieux que N heures (0 = tous)"),
    target: str = Query("output", description="output | tmp | both"),
):
    """
    Nettoie l'espace disque SANS toucher aux vidéos brutes ni à la musique.

    PROTECTION :
    - Sans ?confirm=YES_CLEAN : dry-run (juste liste ce qui serait supprimé)
    - Ne touche JAMAIS à VIDEO_DIR (vidéos brutes des VAs)
    - Ne touche JAMAIS à MUSIC_DIR (musiques)
    - Ne touche JAMAIS à TEMPLATE_DIR (templates)
    - Supprime UNIQUEMENT OUTPUT_DIR (mixes générés, déjà sur Drive)
      et/ou les fichiers tmp_* dans UPLOAD_DIR

    Args:
        confirm: "YES_CLEAN" pour vraiment supprimer, sinon dry-run
        older_than_hours: si > 0, ne touche pas aux fichiers récents
        target: "output" (mixes), "tmp" (temp uploads), ou "both"
    """
    if target not in ("output", "tmp", "both"):
        raise HTTPException(400, "target doit être 'output', 'tmp', ou 'both'")

    dry_run = confirm != "YES_CLEAN"
    now = time.time()
    cutoff = now - (older_than_hours * 3600) if older_than_hours > 0 else None

    # Collecte des fichiers à supprimer (SANS toucher à videos/music/templates)
    candidates: List[Dict[str, Any]] = []

    if target in ("output", "both") and OUTPUT_DIR.exists():
        for f in OUTPUT_DIR.rglob("*"):
            if not f.is_file():
                continue
            try:
                stat = f.stat()
                if cutoff is not None and stat.st_mtime > cutoff:
                    continue  # trop récent, on garde
                candidates.append({
                    "path": str(f),
                    "category": "output",
                    "size_bytes": stat.st_size,
                    "age_hours": round((now - stat.st_mtime) / 3600, 1),
                })
            except Exception:
                pass

    if target in ("tmp", "both") and UPLOAD_DIR.exists():
        # Ne supprime que les fichiers commençant par "tmp_" dans uploads
        # (les screenshots/images uploadées par /extractor sont à conserver)
        for f in UPLOAD_DIR.rglob("tmp_*"):
            if not f.is_file():
                continue
            try:
                stat = f.stat()
                if cutoff is not None and stat.st_mtime > cutoff:
                    continue
                candidates.append({
                    "path": str(f),
                    "category": "tmp",
                    "size_bytes": stat.st_size,
                    "age_hours": round((now - stat.st_mtime) / 3600, 1),
                })
            except Exception:
                pass

    total_size = sum(c["size_bytes"] for c in candidates)

    deleted_count = 0
    deleted_size = 0
    errors: List[str] = []

    if not dry_run:
        logger.warning(
            f"🗑️ [cf_debug] CLEANUP-SAFE deleting {len(candidates)} files "
            f"({_format_size(total_size)}) target={target}"
        )
        for c in candidates:
            try:
                Path(c["path"]).unlink()
                deleted_count += 1
                deleted_size += c["size_bytes"]
            except Exception as e:
                errors.append(f"{c['path']}: {e}")

    # Espace disque avant/après
    try:
        stat = shutil.disk_usage(str(BASE_DIR.parent))
        disk_status = {
            "free": _format_size(stat.free),
            "used": _format_size(stat.used),
            "pct_used": round((stat.used / stat.total) * 100, 1),
        }
    except Exception:
        disk_status = {}

    return {
        "dry_run": dry_run,
        "target": target,
        "older_than_hours": older_than_hours,
        "candidates_found": len(candidates),
        "candidates_total_size": _format_size(total_size),
        "deleted_count": deleted_count,
        "deleted_size": _format_size(deleted_size),
        "errors": errors[:20],
        "disk_status_after": disk_status,
        "next_step": (
            "Tu vois ce qui serait supprimé. Pour vraiment supprimer, ajoute ?confirm=YES_CLEAN à l'URL."
            if dry_run else
            f"✅ {deleted_count} fichiers supprimés ({_format_size(deleted_size)} libérés)"
        ),
        "samples": candidates[:10],  # premiers 10 pour visualiser
    }
