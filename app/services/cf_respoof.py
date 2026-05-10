"""
ClipFusion Respoof Service.

Pipeline pour respoofer une photo ou une vidéo existante :
- Photo (JPG/PNG/HEIC) : EXIF iPhone + pixel tweaks via photo_spoof.py
- Vidéo (MP4/MOV) : metadata iPhone + audio randomization + transcode (casse hash)

Différences avec mix_batch_stream (le mode /request normal) :
- Pas de caption ajoutée (la vidéo a déjà sa caption hardcoded)
- Pas de scaling forcé 1080x1920 (garde la résolution source)
- 1 fichier in → 1 fichier out (pas N variantes)
- Pas de musique remplacée (garde l'audio source + re-encode pour spoofer hash)
"""
import os
import io
import shutil
import logging
import random
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional, Tuple, Any

from app.utils import metadata_randomizer
from app.utils.storage_paths import VIDEO_DIR

logger = logging.getLogger(__name__)


# Extensions reconnues
PHOTO_EXTENSIONS = {".jpg", ".jpeg", ".png", ".heic", ".heif", ".webp"}
VIDEO_EXTENSIONS = {".mp4", ".mov", ".m4v", ".avi", ".webm", ".mkv"}


def detect_file_type(filename: str) -> str:
    """
    Retourne 'photo', 'video' ou 'unknown' selon l'extension du fichier.
    """
    if not filename:
        return "unknown"
    ext = Path(filename).suffix.lower()
    if ext in PHOTO_EXTENSIONS:
        return "photo"
    if ext in VIDEO_EXTENSIONS:
        return "video"
    return "unknown"


def respoof_photo(
    input_bytes: bytes,
    filename: str,
    account: Dict[str, Any],
    target_hour: int = 12,
    tz_name: str = "benin",
) -> Tuple[bytes, Dict[str, Any]]:
    """
    Respoof une photo en utilisant le device + GPS lockés du compte.
    Returns: (output_bytes, info_dict)
    """
    from app.services import photo_spoof, cf_storage
    from PIL import Image, ImageEnhance
    import piexif

    # Récupère device + GPS + iOS lockés du compte
    device_choice = account.get("device_choice", "")
    gps_lat = float(account.get("gps_lat", 0.0))
    gps_lng = float(account.get("gps_lng", 0.0))
    gps_alt = cf_storage.get_city_altitude(account.get("gps_city", ""))

    # Lookup le modèle iPhone à partir de device_choice
    model_name = metadata_randomizer._IPHONE_MAP.get(device_choice, "iPhone 16 Pro Max")

    # Lookup iOS du compte (drift)
    current_ios = account.get("ios_version", "") or ""
    ios_set_at = account.get("ios_set_at")
    picked_ios, did_upgrade = metadata_randomizer.pick_ios_for_account(
        device_choice=device_choice,
        current_ios=current_ios,
        ios_set_at_iso=ios_set_at,
    )
    if did_upgrade and account.get("id"):
        try:
            cf_storage.update_account_ios(int(account["id"]), picked_ios)
            account["ios_version"] = picked_ios
        except Exception as e:
            logger.warning(f"Failed to persist iOS update: {e}")

    # Build le device dict comme attendu par photo_spoof
    device = {
        "model": model_name,
        "software": picked_ios,
        "lens": _build_lens_string(model_name),
    }

    # Date dans la fenêtre cible
    tz_offset = metadata_randomizer.TIMEZONE_OFFSETS.get(tz_name.lower(), 1)
    dt = metadata_randomizer._datetime_for_window(target_hour, tz_offset)
    # Adjust à une heure dans la fenêtre (1h avant target_hour)
    dt_str = dt.strftime("%Y:%m:%d %H:%M:%S")

    # Construit le bloc EXIF custom avec GPS + iOS du compte
    exif_bytes = _build_exif_with_account(device, dt_str, gps_lat, gps_lng, gps_alt)

    # Ouvre l'image
    img = Image.open(io.BytesIO(input_bytes))
    if img.mode not in ("RGB", "L"):
        img = img.convert("RGB")

    # Pixel tweaks (changement de hash imperceptible)
    img = _apply_pixel_tweaks(img)

    # Sauvegarde en JPEG avec EXIF custom
    out = io.BytesIO()
    img.save(
        out,
        format="JPEG",
        quality=random.randint(88, 95),
        exif=exif_bytes,
        optimize=False,
    )
    out.seek(0)

    info = {
        "type": "photo",
        "device_model": model_name,
        "software": picked_ios,
        "gps_city": account.get("gps_city", ""),
        "filename": filename,
        "size_in": len(input_bytes),
        "size_out": len(out.getvalue()),
    }
    return out.getvalue(), info


def respoof_video(
    input_path: str,
    output_path: str,
    account: Dict[str, Any],
    target_hour: int = 12,
    tz_name: str = "benin",
) -> Dict[str, Any]:
    """
    Respoof une vidéo en utilisant le device + GPS lockés du compte.
    Garde la résolution source et l'audio original (re-encodé pour casser le hash).
    Returns: info_dict
    """
    from app.services import cf_storage

    device_choice = account.get("device_choice", "")
    gps_lat = float(account.get("gps_lat", 0.0))
    gps_lng = float(account.get("gps_lng", 0.0))
    gps_alt = cf_storage.get_city_altitude(account.get("gps_city", ""))

    # Lookup iOS via drift
    current_ios = account.get("ios_version", "") or ""
    ios_set_at = account.get("ios_set_at")
    picked_ios, did_upgrade = metadata_randomizer.pick_ios_for_account(
        device_choice=device_choice,
        current_ios=current_ios,
        ios_set_at_iso=ios_set_at,
    )
    if did_upgrade and account.get("id"):
        try:
            cf_storage.update_account_ios(int(account["id"]), picked_ios)
            account["ios_version"] = picked_ios
        except Exception as e:
            logger.warning(f"Failed to persist iOS update: {e}")

    # Construit metadata via la fonction existante du randomizer
    spoof_meta = metadata_randomizer.iphone_metadata_for_account(
        device_choice=device_choice,
        gps_lat=gps_lat,
        gps_lng=gps_lng,
        target_hour=target_hour,
        tz_name=tz_name,
        gps_alt=gps_alt,
        drift_step=0,
        ios_version=picked_ios,
    )

    # Récupère les specs device (bitrate)
    device_specs = metadata_randomizer.get_device_specs(spoof_meta.get("model", ""))
    bitrate_kbps = random.randint(*device_specs["bitrate_kbps"])

    # Audio spoof params (pitch ±1.5% pour casser le hash audio)
    audio_pitch = random.uniform(0.985, 1.015)

    # Construit la commande FFmpeg minimaliste
    # Pas de scale (garde résolution source), pas de caption, garde audio source
    cmd = _build_respoof_ffmpeg_cmd(
        input_path=input_path,
        output_path=output_path,
        metadata=spoof_meta,
        bitrate_kbps=bitrate_kbps,
        audio_pitch=audio_pitch,
    )

    logger.info(f"[respoof] FFmpeg cmd: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
    if result.returncode != 0:
        logger.error(f"[respoof] FFmpeg failed: {result.stderr[-500:]}")
        raise RuntimeError(f"FFmpeg respoof failed: {result.stderr[-300:]}")

    out_size = Path(output_path).stat().st_size if Path(output_path).exists() else 0

    info = {
        "type": "video",
        "device_model": spoof_meta.get("model", "?"),
        "software": picked_ios,
        "gps_city": account.get("gps_city", ""),
        "bitrate_kbps": bitrate_kbps,
        "size_out": out_size,
    }
    return info


def _build_respoof_ffmpeg_cmd(
    input_path: str,
    output_path: str,
    metadata: Dict[str, str],
    bitrate_kbps: int,
    audio_pitch: float,
) -> list:
    """
    Construit la commande FFmpeg pour respoofer une vidéo.
    - Garde résolution source
    - Re-encode video (casse hash) avec bitrate du device
    - Re-encode audio avec pitch ±1.5% (casse hash audio)
    - Applique metadata Apple/iPhone
    """
    # Filtre audio : pitch shift léger pour casser hash audio
    afilter = (
        f"asetrate=44100*{audio_pitch:.6f},"
        f"aresample=44100,"
        f"atempo={1.0/audio_pitch:.6f}"
    )

    cmd = [
        "ffmpeg", "-y",
        "-i", input_path,
        # Video : re-encode avec bitrate cohérent device
        "-c:v", "libx264",
        "-preset", "veryfast",
        "-crf", "23",
        "-b:v", f"{bitrate_kbps}k",
        # Audio : pitch shift léger
        "-c:a", "aac",
        "-b:a", "128k",
        "-af", afilter,
        # Strip original metadata
        "-map_metadata", "-1",
        # Bitexact pour wipe le tag Encoder FFmpeg
        "-fflags", "+bitexact",
        "-flags:v", "+bitexact",
        "-flags:a", "+bitexact",
        # Brand qt comme un vrai iPhone
        "-brand", "qt  ",
        # Mov flags
        "-movflags", "+faststart+use_metadata_tags",
    ]

    # Inject metadata Apple
    cmd += metadata_randomizer.metadata_to_ffmpeg_args(metadata)

    cmd += [output_path]
    return cmd


def _build_lens_string(model_name: str) -> str:
    """Génère le LensModel EXIF cohérent avec le device iPhone."""
    lenses = {
        "iPhone 17 Pro Max": "iPhone 17 Pro Max back triple camera 5.96mm f/1.78",
        "iPhone 17 Pro":     "iPhone 17 Pro back triple camera 5.96mm f/1.78",
        "iPhone 17 Air":     "iPhone 17 Air back camera 5.96mm f/1.8",
        "iPhone 17":         "iPhone 17 back dual camera 5.1mm f/1.6",
        "iPhone 16 Pro Max": "iPhone 16 Pro Max back triple camera 6.765mm f/1.78",
        "iPhone 16 Pro":     "iPhone 16 Pro back triple camera 6.765mm f/1.78",
        "iPhone 16 Plus":    "iPhone 16 Plus back dual camera 5.1mm f/1.6",
        "iPhone 16":         "iPhone 16 back dual camera 5.1mm f/1.6",
    }
    return lenses.get(model_name, "iPhone back camera 5.1mm f/1.6")


def _build_exif_with_account(
    device: dict,
    dt_str: str,
    gps_lat: float,
    gps_lng: float,
    gps_alt: int,
) -> bytes:
    """
    Construit un bloc EXIF avec le device + GPS locké du compte.
    Inclut GPS lat/lng/alt pour cohérence avec l'identité spoofée.
    """
    import piexif

    # Convertit lat/lng en format EXIF DMS (rationals)
    def _to_dms_rational(deg: float):
        deg_abs = abs(deg)
        d = int(deg_abs)
        m_float = (deg_abs - d) * 60
        m = int(m_float)
        s_float = (m_float - m) * 60
        return ((d, 1), (m, 1), (int(s_float * 100), 100))

    lat_ref = b"N" if gps_lat >= 0 else b"S"
    lng_ref = b"E" if gps_lng >= 0 else b"W"
    alt_ref = 0 if gps_alt >= 0 else 1

    zeroth = {
        piexif.ImageIFD.Make: b"Apple",
        piexif.ImageIFD.Model: device["model"].encode("utf-8"),
        piexif.ImageIFD.Software: device["software"].encode("utf-8"),
        piexif.ImageIFD.DateTime: dt_str.encode("utf-8"),
        piexif.ImageIFD.Orientation: 1,
        piexif.ImageIFD.XResolution: (72, 1),
        piexif.ImageIFD.YResolution: (72, 1),
        piexif.ImageIFD.ResolutionUnit: 2,
        piexif.ImageIFD.YCbCrPositioning: 1,
    }

    exif_ifd = {
        piexif.ExifIFD.DateTimeOriginal: dt_str.encode("utf-8"),
        piexif.ExifIFD.DateTimeDigitized: dt_str.encode("utf-8"),
        piexif.ExifIFD.LensMake: b"Apple",
        piexif.ExifIFD.LensModel: device["lens"].encode("utf-8"),
        piexif.ExifIFD.ExifVersion: b"0232",
        piexif.ExifIFD.ColorSpace: 1,
        piexif.ExifIFD.ExposureTime: (1, random.choice([60, 100, 125, 250, 500])),
        piexif.ExifIFD.FNumber: (178, 100),
        piexif.ExifIFD.ISOSpeedRatings: random.choice([50, 64, 100, 125, 200, 400]),
        piexif.ExifIFD.FocalLength: (596, 100),
        piexif.ExifIFD.FocalLengthIn35mmFilm: 24,
        piexif.ExifIFD.Flash: 16,
        piexif.ExifIFD.WhiteBalance: 0,
        piexif.ExifIFD.MeteringMode: 5,
        piexif.ExifIFD.ExposureProgram: 2,
        piexif.ExifIFD.SceneCaptureType: 0,
    }

    gps_ifd = {
        piexif.GPSIFD.GPSVersionID: (2, 3, 0, 0),
        piexif.GPSIFD.GPSLatitudeRef: lat_ref,
        piexif.GPSIFD.GPSLatitude: _to_dms_rational(gps_lat),
        piexif.GPSIFD.GPSLongitudeRef: lng_ref,
        piexif.GPSIFD.GPSLongitude: _to_dms_rational(gps_lng),
        piexif.GPSIFD.GPSAltitudeRef: alt_ref,
        piexif.GPSIFD.GPSAltitude: (abs(gps_alt) * 100, 100),
        piexif.GPSIFD.GPSDateStamp: dt_str.split()[0].encode("utf-8"),
    }

    exif_dict = {
        "0th": zeroth,
        "Exif": exif_ifd,
        "GPS": gps_ifd,
        "1st": {},
        "thumbnail": None,
    }

    try:
        return piexif.dump(exif_dict)
    except Exception as e:
        logger.warning(f"EXIF dump failed: {e}, fallback minimal")
        return piexif.dump({
            "0th": {
                piexif.ImageIFD.Make: b"Apple",
                piexif.ImageIFD.Model: device["model"].encode("utf-8"),
                piexif.ImageIFD.DateTime: dt_str.encode("utf-8"),
            },
            "Exif": {},
            "GPS": {},
            "1st": {},
            "thumbnail": None,
        })


def _apply_pixel_tweaks(img):
    """Pixel tweaks imperceptibles (brightness/contrast/saturation ±1-2%)."""
    from PIL import ImageEnhance

    img = ImageEnhance.Brightness(img).enhance(random.uniform(0.99, 1.01))
    img = ImageEnhance.Contrast(img).enhance(random.uniform(0.99, 1.01))
    img = ImageEnhance.Color(img).enhance(random.uniform(0.98, 1.02))
    return img
