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
    VERSION: str = "1.0.0"

    # Limites upload
    MAX_UPLOAD_MB: int = 500
    ALLOWED_EXTENSIONS: set = {".mp4", ".mov", ".mkv", ".avi", ".webm"}

    # Nombre max de copies par requête (protection)
    MAX_COPIES_PER_REQUEST: int = 20

    # Encodeur par défaut (libx264 = CPU, compatible partout)
    # Pour GPU NVIDIA : "h264_nvenc" | pour Apple Silicon : "h264_videotoolbox"
    VIDEO_ENCODER: str = "libx264"
    PRESET: str = "veryfast"   # trade-off vitesse / qualité
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
    "framerate":  (30, 60),              # fps
    "bitrate":    (5000, 6000),          # kbps
    "brightness": (-0.05, 0.05),         # -1.0 à 1.0 (FFmpeg eq)
    "contrast":   (0.95, 1.10),          # 0 à 2 (neutre = 1)
    "saturation": (0.95, 1.15),          # 0 à 3 (neutre = 1)
    "gamma":      (0.95, 1.05),          # 0.1 à 10 (neutre = 1)
    "speed":      (1.03, 1.04),          # multiplicateur vitesse
    "zoom":       (1.03, 1.06),          # multiplicateur zoom (crop + scale)
    "noise":      (5, 15),               # intensité bruit (0-100)
    "vignette":   (0.20, 0.40),          # angle vignette (radians ~ PI/5)
    "rotation":   (-0.5, 0.5),           # rotation en degrés
    "cut_start":  (0.1, 0.15),           # secondes coupées au début
    "cut_end":    (0.1, 0.15),           # secondes coupées à la fin
}
