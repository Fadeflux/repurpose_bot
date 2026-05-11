"""
ClipFusion Respoof Service.

Pipeline pour respoofer une photo ou une vidéo existante :
- Photo (JPG/PNG/HEIC) : EXIF iPhone + pixel tweaks
- Vidéo (MP4/MOV) : metadata iPhone + audio randomization + transcode (casse hash)
"""
import io
import logging
import random
import re
import subprocess
from pathlib import Path
from typing import Dict, Tuple, Any

from app.utils import metadata_randomizer

logger = logging.getLogger(__name__)


PHOTO_EXTENSIONS = {".jpg", ".jpeg", ".png", ".heic", ".heif", ".webp"}
VIDEO_EXTENSIONS = {".mp4", ".mov", ".m4v", ".avi", ".webm", ".mkv"}


_DEVICE_BITRATE_KBPS = {
    "iPhone 17 Pro Max": (12000, 18000),
    "iPhone 17 Pro":     (12000, 18000),
    "iPhone 17 Air":     (10000, 15000),
    "iPhone 17":         (10000, 15000),
    "iPhone 16 Pro Max": (12000, 18000),
    "iPhone 16 Pro":     (12000, 18000),
    "iPhone 16 Plus":    (10000, 15000),
    "iPhone 16":         (10000, 15000),
}


def detect_file_type(filename: str) -> str:
    if not filename:
        return "unknown"
    ext = Path(filename).suffix.lower()
    if ext in PHOTO_EXTENSIONS:
        return "photo"
    if ext in VIDEO_EXTENSIONS:
        return "video"
    return "unknown"


def _get_iphone_model_from_device_choice(device_choice: str) -> str:
    """Convertit 'iphone_16_pro_max' → 'iPhone 16 Pro Max'."""
    return metadata_randomizer._IPHONE_MAP.get(device_choice, "iPhone 16 Pro Max")


def _override_metadata_with_account_gps(
    meta: Dict[str, str],
    gps_lat: float,
    gps_lng: float,
    gps_alt: float,
) -> Dict[str, str]:
    """Remplace les coordonnées random par celles du compte."""
    location_str = metadata_randomizer._format_iso6709(gps_lat, gps_lng, float(gps_alt))
    meta = dict(meta)
    meta["location"] = location_str
    meta["location-eng"] = location_str
    meta["com.apple.quicktime.location.ISO6709"] = location_str
    return meta


def respoof_photo(
    input_bytes: bytes,
    filename: str,
    account: Dict[str, Any],
    target_hour: int = 12,
    tz_name: str = "benin",
) -> Tuple[bytes, Dict[str, Any]]:
    from app.services import cf_storage
    from PIL import Image

    device_choice = account.get("device_choice", "")
    gps_lat = float(account.get("gps_lat", 0.0))
    gps_lng = float(account.get("gps_lng", 0.0))
    gps_alt = cf_storage.get_city_altitude(account.get("gps_city", ""))

    model_name = _get_iphone_model_from_device_choice(device_choice)
    spoof_meta = metadata_randomizer._iphone_metadata_fixed(model_name)
    ios_version = spoof_meta.get("com.apple.quicktime.software", "26.4")

    creation_time_iso = spoof_meta.get("creation_time", "")
    dt_str = _iso_to_exif_dt(creation_time_iso)

    lens = _build_lens_string(model_name)
    exif_bytes = _build_exif(
        model_name, ios_version, lens, dt_str, gps_lat, gps_lng, int(gps_alt)
    )

    img = Image.open(io.BytesIO(input_bytes))
    if img.mode not in ("RGB", "L"):
        img = img.convert("RGB")

    img = _apply_pixel_tweaks(img)

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
        "software": ios_version,
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
    from app.services import cf_storage

    device_choice = account.get("device_choice", "")
    gps_lat = float(account.get("gps_lat", 0.0))
    gps_lng = float(account.get("gps_lng", 0.0))
    gps_alt = float(cf_storage.get_city_altitude(account.get("gps_city", "")))

    model_name = _get_iphone_model_from_device_choice(device_choice)
    spoof_meta = metadata_randomizer._iphone_metadata_fixed(model_name)
    ios_version = spoof_meta.get("com.apple.quicktime.software", "26.4")

    spoof_meta = _override_metadata_with_account_gps(spoof_meta, gps_lat, gps_lng, gps_alt)

    bitrate_range = _DEVICE_BITRATE_KBPS.get(model_name, (10000, 15000))
    bitrate_kbps = random.randint(*bitrate_range)
    audio_pitch = random.uniform(0.985, 1.015)

    cmd = _build_respoof_ffmpeg_cmd(
        input_path=input_path,
        output_path=output_path,
        metadata=spoof_meta,
        bitrate_kbps=bitrate_kbps,
        audio_pitch=audio_pitch,
    )

    logger.info(f"[respoof] FFmpeg cmd: {' '.join(cmd[:25])}...")
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
    if result.returncode != 0:
        logger.error(f"[respoof] FFmpeg failed: {result.stderr[-500:]}")
        raise RuntimeError(f"FFmpeg respoof failed: {result.stderr[-300:]}")

    out_size = Path(output_path).stat().st_size if Path(output_path).exists() else 0

    return {
        "type": "video",
        "device_model": model_name,
        "software": ios_version,
        "gps_city": account.get("gps_city", ""),
        "bitrate_kbps": bitrate_kbps,
        "size_out": out_size,
    }


def _build_respoof_ffmpeg_cmd(
    input_path: str,
    output_path: str,
    metadata: Dict[str, str],
    bitrate_kbps: int,
    audio_pitch: float,
) -> list:
    afilter = (
        f"asetrate=44100*{audio_pitch:.6f},"
        f"aresample=44100,"
        f"atempo={1.0/audio_pitch:.6f}"
    )
    cmd = [
        "ffmpeg", "-y",
        "-i", input_path,
        "-c:v", "libx264",
        "-preset", "veryfast",
        "-crf", "23",
        "-b:v", f"{bitrate_kbps}k",
        "-c:a", "aac",
        "-b:a", "128k",
        "-af", afilter,
        "-map_metadata", "-1",
        "-fflags", "+bitexact",
        "-flags:v", "+bitexact",
        "-flags:a", "+bitexact",
        "-brand", "qt  ",
        "-movflags", "+faststart+use_metadata_tags",
    ]
    cmd += metadata_randomizer.metadata_to_ffmpeg_args(metadata)
    cmd += [output_path]
    return cmd


def _iso_to_exif_dt(iso: str) -> str:
    if not iso:
        from datetime import datetime
        return datetime.now().strftime("%Y:%m:%d %H:%M:%S")
    m = re.match(r"(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})", iso)
    if m:
        return f"{m.group(1)}:{m.group(2)}:{m.group(3)} {m.group(4)}:{m.group(5)}:{m.group(6)}"
    return iso


def _build_lens_string(model_name: str) -> str:
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


def _build_exif(
    model: str,
    software: str,
    lens: str,
    dt_str: str,
    gps_lat: float,
    gps_lng: float,
    gps_alt: int,
) -> bytes:
    import piexif

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
        piexif.ImageIFD.Model: model.encode("utf-8"),
        piexif.ImageIFD.Software: software.encode("utf-8"),
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
        piexif.ExifIFD.LensModel: lens.encode("utf-8"),
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
    try:
        return piexif.dump({
            "0th": zeroth,
            "Exif": exif_ifd,
            "GPS": gps_ifd,
            "1st": {},
            "thumbnail": None,
        })
    except Exception as e:
        logger.warning(f"EXIF dump failed: {e}, fallback minimal")
        return piexif.dump({
            "0th": {
                piexif.ImageIFD.Make: b"Apple",
                piexif.ImageIFD.Model: model.encode("utf-8"),
                piexif.ImageIFD.DateTime: dt_str.encode("utf-8"),
            },
            "Exif": {},
            "GPS": {},
            "1st": {},
            "thumbnail": None,
        })


def _apply_pixel_tweaks(img):
    from PIL import ImageEnhance
    img = ImageEnhance.Brightness(img).enhance(random.uniform(0.99, 1.01))
    img = ImageEnhance.Contrast(img).enhance(random.uniform(0.99, 1.01))
    img = ImageEnhance.Color(img).enhance(random.uniform(0.98, 1.02))
    return img
