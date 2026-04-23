"""
Configuration centrale de l'application.
Toutes les bornes min/max des paramètres de randomisation sont ici.
"""
from pathlib import Path
from pydantic_settings import BaseSettings


BASE_DIR = Path(__file__).resolve().parent.parent
UPLOAD_DIR = BASE_DIR / "uploads"
OUTPUT_DIR = BASE_DIR / "outputs"
LOG_DIR = BASE_DIR / "logs"

for d in (UPLOAD_DIR, OUTPUT_DIR, LOG_DIR):
    d.mkdir(parents=True, exist_ok=True)


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
