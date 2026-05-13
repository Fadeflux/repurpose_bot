"""
Helper centralisé pour upload streaming sans saturer la RAM.

Problème résolu : `shutil.copyfileobj(f.file, out)` et `await f.read()`
chargent la vidéo entière en RAM. Avec 5-10 VAs qui upload des vidéos
de 50-100 MB en parallèle, Railway atteint la limite 5GB RAM → OOM kill
→ HTTP 500 → bot crashé.

Cette fonction lit par chunks de 1 MB et écrit direct sur disque.
RAM constante, peu importe la taille du fichier.
"""
import gc
from pathlib import Path
from typing import Optional

from fastapi import UploadFile

# Chunk size : 1 MB. Compromis idéal entre nombre de syscalls et RAM utilisée.
CHUNK_SIZE = 1024 * 1024


async def save_upload_streaming(
    upload: UploadFile,
    dest_path: Path,
    max_bytes: Optional[int] = None,
) -> int:
    """
    Sauvegarde un UploadFile vers `dest_path` en streaming par chunks de 1 MB.
    Garantit que `upload` est fermé après usage (libère la RAM).

    Args:
        upload: le UploadFile FastAPI
        dest_path: chemin de destination sur disque
        max_bytes: si > 0, lève ValueError si dépassé (cleanup auto du fichier partiel)

    Returns:
        nombre d'octets écrits

    Raises:
        ValueError: si max_bytes est dépassé
        Exception: toute erreur d'écriture (le fichier partiel est nettoyé)
    """
    written = 0
    try:
        with open(dest_path, "wb") as out:
            while True:
                chunk = await upload.read(CHUNK_SIZE)
                if not chunk:
                    break
                written += len(chunk)
                if max_bytes is not None and written > max_bytes:
                    raise ValueError(
                        f"Fichier trop volumineux: {written} > {max_bytes} bytes"
                    )
                out.write(chunk)
                # Libère immédiatement la référence
                del chunk
    except Exception:
        # Cleanup du fichier partiellement écrit
        try:
            if dest_path.exists():
                dest_path.unlink()
        except Exception:
            pass
        raise
    finally:
        # TOUJOURS fermer le UploadFile pour libérer la RAM (SpooledTemporaryFile)
        try:
            await upload.close()
        except Exception:
            pass

    return written


async def save_upload_streaming_and_gc(
    upload: UploadFile,
    dest_path: Path,
    max_bytes: Optional[int] = None,
) -> int:
    """Comme save_upload_streaming mais force gc.collect() à la fin.
    À utiliser après le dernier upload d'un batch."""
    written = await save_upload_streaming(upload, dest_path, max_bytes)
    gc.collect()
    return written
