"""
Configuration centrale de l'application.
Toutes les bornes min/max des paramètres de randomisation sont ici.
"""
import os
from pathlib import Path
from pydantic_settings import BaseSettings


# ============================================================================
# CHEMINS DE STOCKAGE
# ============================================================================
# IMPORTANT : sur Railway, le filesystem du container est LIMITÉ (~5-10 GB
# selon le plan) et il sert à FAIRE TOURNER l'app. Stocker des vidéos dedans
# = crash garanti.
#
# Le volume Railway monté sur /data est de 100 GB (plan Pro) et est PERSISTANT
# entre redéploiements. C'est LÀ qu'on doit stocker les outputs/uploads.
#
# Fallback : si /data n'existe pas (dev local), on utilise un dossier dans
# le code source comme avant.

_PROJECT_DIR = Path(__file__).resolve().parent.parent  # racine du repo


def _resolve_storage_root() -> Path:
    """Détermine où stocker uploads/outputs."""
    # 1) Override explicite via env var
    override = os.environ.get("REPURPOSE_STORAGE_DIR", "").strip()
    if override:
        return Path(override)

    # 2) Volume Railway /data si dispo
    data_root = Path("/data")
    if data_root.exists() and data_root.is_dir():
        try:
            test = data_root / ".repurpose_write_test"
            test.touch(exist_ok=True)
            test.unlink(missing_ok=True)
            return data_root / "repurpose"
        except Exception:
            pass

    # 3) Fallback : dossier dans le repo (dev local uniquement)
    return _PROJECT_DIR


BASE_DIR = _resolve_storage_root()
UPLOAD_DIR = BASE_DIR / "uploads"
OUTPUT_DIR = BASE_DIR / "outputs"
LOG_DIR = _PROJECT_DIR / "logs"  # logs restent dans le code, c'est éphémère c'est OK

for d in (UPLOAD_DIR, OUTPUT_DIR, LOG_DIR):
    try:
        d.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass


class Settings(BaseSettings):
    APP_NAME: str = "Repurpose Bot API"
    VERSION: str = "2.1.0"

    # Limites upload
    MAX_UPLOAD_MB: int = 500
    MAX_FILES_PER_REQUEST: int = 200
    ALLOWED_EXTENSIONS: set = {".mp4", ".mov", ".mkv", ".avi", ".webm"}

    # Nombre max de copies par vidéo source
    MAX_COPIES_PER_REQUEST: int = 100

    # Framerate cible fixe (60 fps = qualité max TikTok, pas de valeurs suspectes)
    TARGET_FPS: int = 60

    # Encodeur : veryfast permet le High profile H.264 (ultrafast force Baseline)
    # Bon compromis vitesse/qualité
    VIDEO_ENCODER: str = "libx264"
    PRESET: str = "veryfast"       # High profile OK, qualité nettement meilleure qu'ultrafast
    TUNE: str = ""                 # pas de tune (fastdecode dégradait la qualité)
    VIDEO_PROFILE: str = "high"
    AUDIO_CODEC: str = "aac"
    AUDIO_BITRATE: str = "192k"

    # Format cible TikTok vertical
    TARGET_WIDTH: int = 1080
    TARGET_HEIGHT: int = 1920


settings = Settings()


# ---------------------------------------------------------------------------
# Bornes des paramètres de randomisation (min, max)
# ---------------------------------------------------------------------------
# Note: framerate retiré — on le force à 60 fps systématiquement pour la qualité
PARAM_RANGES = {
    "bitrate":    (8000, 12000),         # kbps (bien plus haut qu'avant, proche des sources TikTok)
    "brightness": (-0.05, 0.05),
    "contrast":   (0.95, 1.10),
    "saturation": (0.95, 1.15),
    "gamma":      (0.95, 1.05),
    "speed":      (1.03, 1.04),
    "zoom":       (1.03, 1.06),
    "noise":      (5, 15),
    "vignette":   (0.20, 0.40),
    "rotation":   (-0.5, 0.5),
    "cut_start":  (0.1, 0.15),
    "cut_end":    (0.1, 0.15),
}
