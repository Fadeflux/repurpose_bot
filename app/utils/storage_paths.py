"""
Centralise les chemins de stockage de ClipFusion.

Sur Railway avec un volume persistant monté sur /data :
  - Les fichiers sont stockés sur /data/clipfusion/... (persistants entre redeploys)
  - Si /data n'existe pas (dev local sans volume), fallback sur /tmp/clipfusion (éphémère)

Tous les modules ClipFusion doivent importer depuis ici au lieu de hardcoder /tmp.
"""
import os
from pathlib import Path


def _resolve_base_dir() -> Path:
    """
    Décide où stocker les fichiers ClipFusion :
      1. Variable d'env CLIPFUSION_STORAGE_DIR si définie (override manuel)
      2. /data si le volume Railway existe (persistant)
      3. /tmp/clipfusion en dernier recours (éphémère, pour dev local)
    """
    override = os.environ.get("CLIPFUSION_STORAGE_DIR", "").strip()
    if override:
        return Path(override)

    # Si /data existe et est inscriptible, on l'utilise (volume Railway)
    data_root = Path("/data")
    if data_root.exists() and data_root.is_dir():
        try:
            # Test d'écriture rapide pour vérifier les permissions
            test_file = data_root / ".clipfusion_write_test"
            test_file.touch(exist_ok=True)
            test_file.unlink(missing_ok=True)
            return data_root / "clipfusion"
        except Exception:
            pass

    # Fallback : /tmp (éphémère)
    return Path("/tmp/clipfusion")


BASE_DIR: Path = _resolve_base_dir()

# Sous-dossiers standards
VIDEO_DIR: Path = BASE_DIR / "videos"
MUSIC_DIR: Path = BASE_DIR / "music"
TEMPLATE_DIR: Path = BASE_DIR / "templates"
OUTPUT_DIR: Path = BASE_DIR / "output"
UPLOAD_DIR: Path = BASE_DIR / "uploads"

# Dossier séparé pour Geelark (hors ClipFusion)
GEELARK_UPLOAD_DIR: Path = (
    Path("/data/geelark_uploads")
    if Path("/data").exists() and Path("/data").is_dir()
    else Path("/tmp/geelark_uploads")
)


def ensure_dirs() -> None:
    """À appeler au démarrage pour créer tous les dossiers nécessaires."""
    for d in (VIDEO_DIR, MUSIC_DIR, TEMPLATE_DIR, OUTPUT_DIR, UPLOAD_DIR, GEELARK_UPLOAD_DIR):
        try:
            d.mkdir(parents=True, exist_ok=True)
        except Exception:
            pass


def is_persistent() -> bool:
    """True si on stocke sur volume Railway, False si /tmp éphémère."""
    return str(BASE_DIR).startswith("/data")
