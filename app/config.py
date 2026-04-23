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
    VERSION: str = "2.0.0"

    # Limites upload
    MAX_UPLOAD_MB: int = 500               # par fichier
    MAX_FILES_PER_REQUEST: int = 200       # nb max de vidéos sources
    ALLOWED_EXTENSIONS: set = {".mp4", ".mov", ".mkv", ".avi", ".webm"}

    # Nombre max de copies par vidéo source
    MAX_COPIES_PER_REQUEST: int = 100

    # Encodeur par défaut (libx264 = CPU, compatible partout)
    VIDEO_ENCODER: str = "libx264"
    PRESET: str = "veryfast"
    AUDIO_CODEC: str = "aac"
    AUDIO_BITRATE: str = "128k"

    # Format cible TikTok vertical
    TARGET_WIDTH: int = 1080
    TARGET_HEIGHT: int = 1920


settings = Settings()


# ---------------------------------------------------------------------------
# Bornes des paramètres de randomisation (min, max)
# ---------------------------------------------------------------------------
PARAM_RANGES = {
    "framerate":  (30, 60),
    "bitrate":    (5000, 6000),
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
