"""
Service pour spoofer les métadonnées d'une image (EXIF + pixels).

Applique :
- Metadata EXIF : iPhone 16/17 aléatoire (Make/Model/Software/Lens)
- Date/heure randomisée (récente)
- Micro-bruit imperceptible sur les pixels (change le hash)
- Brightness +/- 1% aléatoire

Usage :
    spoofed_bytes, info = spoof_image(input_bytes, filename="photo.jpg")
"""
import io
import random
from datetime import datetime, timedelta
from typing import Tuple, Dict

from PIL import Image, ImageEnhance
import piexif

from app.utils.logger import get_logger

logger = get_logger("photo_spoof")


# Catalogue iPhone 16/17 (Make, Model, Software, LensModel)
IPHONE_DEVICES = [
    # iPhone 17 series
    {"model": "iPhone 17 Pro Max", "software": "18.1.1", "lens": "iPhone 17 Pro Max back triple camera 5.96mm f/1.78"},
    {"model": "iPhone 17 Pro",     "software": "18.1.1", "lens": "iPhone 17 Pro back triple camera 5.96mm f/1.78"},
    {"model": "iPhone 17 Air",     "software": "18.1",   "lens": "iPhone 17 Air back camera 5.96mm f/1.8"},
    {"model": "iPhone 17",         "software": "18.1",   "lens": "iPhone 17 back dual camera 5.1mm f/1.6"},
    # iPhone 16 series
    {"model": "iPhone 16 Pro Max", "software": "18.0.1", "lens": "iPhone 16 Pro Max back triple camera 6.765mm f/1.78"},
    {"model": "iPhone 16 Pro",     "software": "18.0.1", "lens": "iPhone 16 Pro back triple camera 6.765mm f/1.78"},
    {"model": "iPhone 16 Plus",    "software": "18.0",   "lens": "iPhone 16 Plus back dual camera 5.1mm f/1.6"},
    {"model": "iPhone 16",         "software": "18.0",   "lens": "iPhone 16 back dual camera 5.1mm f/1.6"},
    {"model": "iPhone 16e",        "software": "18.0",   "lens": "iPhone 16e back camera 5.1mm f/1.6"},
]


def _random_recent_datetime() -> str:
    """Retourne une date récente (random dans les 7 derniers jours) au format EXIF."""
    now = datetime.now()
    delta_days = random.uniform(0, 7)
    delta_seconds = random.uniform(0, 86400)
    rand_date = now - timedelta(days=delta_days, seconds=delta_seconds)
    return rand_date.strftime("%Y:%m:%d %H:%M:%S")


def _build_exif(device: dict) -> bytes:
    """Construit un bloc EXIF complet pour un device iPhone."""
    dt = _random_recent_datetime()

    zeroth_ifd = {
        piexif.ImageIFD.Make: b"Apple",
        piexif.ImageIFD.Model: device["model"].encode("utf-8"),
        piexif.ImageIFD.Software: device["software"].encode("utf-8"),
        piexif.ImageIFD.DateTime: dt.encode("utf-8"),
        piexif.ImageIFD.Orientation: 1,
        piexif.ImageIFD.XResolution: (72, 1),
        piexif.ImageIFD.YResolution: (72, 1),
        piexif.ImageIFD.ResolutionUnit: 2,
        piexif.ImageIFD.YCbCrPositioning: 1,
    }

    exif_ifd = {
        piexif.ExifIFD.DateTimeOriginal: dt.encode("utf-8"),
        piexif.ExifIFD.DateTimeDigitized: dt.encode("utf-8"),
        piexif.ExifIFD.LensMake: b"Apple",
        piexif.ExifIFD.LensModel: device["lens"].encode("utf-8"),
        piexif.ExifIFD.ExifVersion: b"0232",
        piexif.ExifIFD.ColorSpace: 1,
        piexif.ExifIFD.ExposureTime: (1, random.choice([60, 100, 125, 250, 500])),
        piexif.ExifIFD.FNumber: (178, 100),  # f/1.78
        piexif.ExifIFD.ISOSpeedRatings: random.choice([50, 64, 100, 125, 200, 400]),
        piexif.ExifIFD.FocalLength: (596, 100),
        piexif.ExifIFD.FocalLengthIn35mmFilm: 24,
        piexif.ExifIFD.Flash: 16,  # Flash did not fire, compulsory flash mode
        piexif.ExifIFD.WhiteBalance: 0,
        piexif.ExifIFD.MeteringMode: 5,  # Pattern
        piexif.ExifIFD.ExposureProgram: 2,  # Normal program
        piexif.ExifIFD.SceneCaptureType: 0,  # Standard
    }

    exif_dict = {
        "0th": zeroth_ifd,
        "Exif": exif_ifd,
        "GPS": {},   # pas de GPS pour la privacy
        "1st": {},
        "thumbnail": None,
    }

    try:
        return piexif.dump(exif_dict)
    except Exception as e:
        logger.warning(f"Erreur construction EXIF: {e}, fallback minimal")
        # Fallback minimal si un champ pose problème
        return piexif.dump({
            "0th": {
                piexif.ImageIFD.Make: b"Apple",
                piexif.ImageIFD.Model: device["model"].encode("utf-8"),
                piexif.ImageIFD.DateTime: dt.encode("utf-8"),
            },
            "Exif": {},
            "GPS": {},
            "1st": {},
            "thumbnail": None,
        })


def _apply_pixel_tweaks(img: Image.Image) -> Image.Image:
    """Applique des modifications imperceptibles aux pixels pour changer le hash."""
    # 1. Micro-ajustement brightness (+/- 1%)
    brightness_factor = random.uniform(0.99, 1.01)
    img = ImageEnhance.Brightness(img).enhance(brightness_factor)

    # 2. Micro-ajustement contrast (+/- 1%)
    contrast_factor = random.uniform(0.99, 1.01)
    img = ImageEnhance.Contrast(img).enhance(contrast_factor)

    # 3. Micro-ajustement saturation (+/- 2%)
    sat_factor = random.uniform(0.98, 1.02)
    img = ImageEnhance.Color(img).enhance(sat_factor)

    return img


def spoof_image(input_bytes: bytes, filename: str = "photo.jpg") -> Tuple[bytes, Dict]:
    """
    Spoofe une image : EXIF iPhone 16/17 + micro-tweaks pixels.
    Retourne (bytes_spoofés, info_dict).
    """
    # Choisit un device aléatoire
    device = random.choice(IPHONE_DEVICES)

    # Ouvre l'image
    img = Image.open(io.BytesIO(input_bytes))

    # Convertit en RGB si nécessaire (certains PNG ont RGBA, pas compatible JPEG)
    if img.mode not in ("RGB", "L"):
        img = img.convert("RGB")

    # Applique les micro-tweaks pixels
    img = _apply_pixel_tweaks(img)

    # Génère le bloc EXIF
    exif_bytes = _build_exif(device)

    # Sauvegarde en JPEG avec l'EXIF (toutes les photos iPhone sont en JPEG/HEIC)
    out = io.BytesIO()
    img.save(
        out,
        format="JPEG",
        quality=random.randint(88, 95),  # qualité iPhone typique
        exif=exif_bytes,
        optimize=False,
    )
    out.seek(0)

    info = {
        "device_model": device["model"],
        "software": device["software"],
        "original_size": len(input_bytes),
        "spoofed_size": len(out.getvalue()),
        "filename": filename,
    }
    logger.info(f"Photo spoofée : {filename} -> {device['model']}")

    return out.getvalue(), info
