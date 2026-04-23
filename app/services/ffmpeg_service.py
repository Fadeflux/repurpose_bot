"""
Service de traitement vidéo via FFmpeg.

Améliorations v2.1 :
- Framerate forcé à 60 fps (qualité max TikTok, pas de valeurs suspectes)
- Bitrate plus élevé (8000-12000 kbps) pour matcher la qualité des sources TikTok
- Stripping systématique des métadonnées suspectes (comment vid:xxx TikTok)
- Injection de métadonnées aléatoires crédibles (géoloc, device, date)
- Aspect ratio forcé à 1:1 (plus de ratio bizarre 680:681)
- Preset "medium" pour meilleure qualité d'encodage
"""
import asyncio
import json
import shutil
import uuid
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from app.config import OUTPUT_DIR, settings
from app.utils.logger import get_logger
from app.utils.metadata_randomizer import metadata_to_ffmpeg_args, random_metadata
from app.utils.randomizer import random_params

logger = get_logger("ffmpeg_service")


# ---------------------------------------------------------------------------
# Sondage du fichier source (ffprobe)
# ---------------------------------------------------------------------------
async def probe_duration(path: Path) -> Optional[float]:
    """Retourne la durée du média en secondes via ffprobe, ou None si échec."""
    cmd = [
        "ffprobe", "-v", "error",
        "-show_entries", "format=duration",
        "-of", "json",
        str(path),
    ]
    proc = await asyncio.create_subprocess_exec(
        *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    stdout, stderr = await proc.communicate()
    if proc.returncode != 0:
        logger.warning(f"ffprobe a échoué: {stderr.decode(errors='ignore')}")
        return None
    try:
        data = json.loads(stdout.decode())
        return float(data["format"]["duration"])
    except (KeyError, ValueError, json.JSONDecodeError):
        return None


# ---------------------------------------------------------------------------
# Construction de la chaîne de filtres
# ---------------------------------------------------------------------------
def build_filter_complex(params: Dict[str, Optional[float]]) -> str:
    """
    Assemble le filter_complex FFmpeg en une seule chaîne vidéo.
    Les paramètres à None sont ignorés (filtre non appliqué).
    """
    W, H = settings.TARGET_WIDTH, settings.TARGET_HEIGHT
    filters: List[str] = []

    # Framerate forcé à 60 fps (qualité max, pas de valeur suspecte)
    filters.append(f"fps={settings.TARGET_FPS}")

    # Scale + crop pour garantir le canvas 1080x1920
    filters.append(f"scale={W}:{H}:force_original_aspect_ratio=increase")
    filters.append(f"crop={W}:{H}")

    # Force le sample aspect ratio à 1:1 (évite le 680:681 bizarre)
    filters.append("setsar=1:1")

    # Zoom (optionnel)
    zoom = params.get("zoom")
    if zoom is not None and zoom > 1.0:
        crop_w = int(W / zoom)
        crop_h = int(H / zoom)
        # Assure que les dimensions restent paires (requis par libx264)
        crop_w -= crop_w % 2
        crop_h -= crop_h % 2
        filters.append(f"crop={crop_w}:{crop_h}")
        filters.append(f"scale={W}:{H}:flags=lanczos")
        filters.append("setsar=1:1")

    # Rotation (optionnelle) — appliquée AVANT le crop final pour ne pas casser l'aspect
    rotation = params.get("rotation")
    if rotation is not None and abs(rotation) > 0.001:
        rot_rad = rotation * 3.141592653589793 / 180.0
        # On rogne ce qui dépasse au lieu de remplir de noir (plus propre)
        filters.append(f"rotate={rot_rad}:ow=rotw({rot_rad}):oh=roth({rot_rad})")
        # Re-crop au format attendu
        filters.append(f"crop={W}:{H}")
        filters.append("setsar=1:1")

    # Correction colorimétrique (eq)
    eq_parts = []
    if params.get("brightness") is not None:
        eq_parts.append(f"brightness={params['brightness']}")
    if params.get("contrast") is not None:
        eq_parts.append(f"contrast={params['contrast']}")
    if params.get("saturation") is not None:
        eq_parts.append(f"saturation={params['saturation']}")
    if params.get("gamma") is not None:
        eq_parts.append(f"gamma={params['gamma']}")
    if eq_parts:
        filters.append("eq=" + ":".join(eq_parts))

    # Noise (optionnel)
    noise = params.get("noise")
    if noise is not None and noise > 0:
        filters.append(f"noise=alls={int(noise)}:allf=t")

    # Vignette (optionnelle)
    vignette = params.get("vignette")
    if vignette is not None and vignette > 0:
        filters.append(f"vignette=angle={vignette}")

    # Speed (optionnelle)
    speed = params.get("speed")
    if speed is not None and abs(speed - 1.0) > 0.001:
        pts_factor = round(1.0 / speed, 6)
        filters.append(f"setpts={pts_factor}*PTS")

    return ",".join(filters)


def build_audio_filter(params: Dict[str, Optional[float]]) -> Optional[str]:
    """Ajuste la vitesse audio de la même manière que la vidéo."""
    speed = params.get("speed")
    if speed is None or abs(speed - 1.0) < 0.001:
        return None
    return f"atempo={speed}"


# ---------------------------------------------------------------------------
# Traitement d'une copie
# ---------------------------------------------------------------------------
async def process_one(
    source: Path,
    duration: Optional[float],
    params: Dict[str, Optional[float]],
    job_id: str,
    copy_index: int,
) -> Dict:
    """Génère une copie avec les paramètres donnés."""
    out_name = f"{job_id}_copy{copy_index:02d}_{uuid.uuid4().hex[:8]}.mp4"
    out_path = OUTPUT_DIR / out_name

    # Cuts (peuvent être None)
    cut_start = params.get("cut_start") or 0.0
    cut_end = params.get("cut_end") or 0.0

    input_args: List[str] = []
    if cut_start > 0:
        input_args += ["-ss", f"{cut_start:.3f}"]
    if duration is not None and duration > (cut_start + cut_end + 0.1):
        clip_duration = duration - cut_start - cut_end
        input_args += ["-t", f"{clip_duration:.3f}"]

    vf = build_filter_complex(params)
    af = build_audio_filter(params)

    # Bitrate : par défaut 10000 si désactivé (qualité élevée)
    bitrate = int(params.get("bitrate") or 10000)

    # Métadonnées aléatoires + stripping des métadonnées source
    meta = random_metadata()
    meta_args = metadata_to_ffmpeg_args(meta)

    cmd: List[str] = [
        "ffmpeg", "-y",
        "-hide_banner", "-loglevel", "error",
        *input_args,
        "-i", str(source),
        # -map_metadata -1 : supprime TOUTES les métadonnées de la source
        # (y compris le fameux comment: vid:v24044gl... de TikTok)
        "-map_metadata", "-1",
        "-vf", vf,
    ]
    if af:
        cmd += ["-af", af]

    cmd += [
        # Encodage vidéo
        "-c:v", settings.VIDEO_ENCODER,
        "-preset", settings.PRESET,
        "-profile:v", settings.VIDEO_PROFILE,
        "-level:v", "4.2",
        "-b:v", f"{bitrate}k",
        "-maxrate", f"{int(bitrate * 1.3)}k",
        "-bufsize", f"{int(bitrate * 2)}k",
        "-pix_fmt", "yuv420p",
        "-color_primaries", "bt709",
        "-color_trc", "bt709",
        "-colorspace", "bt709",
        # Force framerate de sortie (en plus du filtre fps)
        "-r", str(settings.TARGET_FPS),
        # Audio
        "-c:a", settings.AUDIO_CODEC,
        "-b:a", settings.AUDIO_BITRATE,
        "-ar", "44100",
        "-ac", "2",
        # Conteneur
        "-movflags", "+faststart",
        "-shortest",
        # Métadonnées aléatoires (appliquées APRÈS -map_metadata -1)
        *meta_args,
        str(out_path),
    ]

    logger.info(f"[{job_id}] copy {copy_index} -> {out_name}")

    proc = await asyncio.create_subprocess_exec(
        *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    _, stderr = await proc.communicate()

    if proc.returncode != 0:
        err = stderr.decode(errors="ignore")[-500:]
        logger.error(f"[{job_id}] ffmpeg a échoué (copy {copy_index}): {err}")
        if out_path.exists():
            out_path.unlink(missing_ok=True)
        return {
            "copy_index": copy_index,
            "success": False,
            "error": err,
            "params": params,
            "metadata": meta,
        }

    return {
        "copy_index": copy_index,
        "success": True,
        "filename": out_name,
        "path": str(out_path),
        "size_bytes": out_path.stat().st_size,
        "params": params,
        "metadata": meta,
    }


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------
async def process_video(
    source: Path,
    copies: int,
    job_id: str,
    concurrency: int = 2,
    custom_ranges: Optional[Dict[str, Tuple[float, float]]] = None,
    enabled_filters: Optional[List[str]] = None,
) -> List[Dict]:
    """Génère `copies` variantes randomisées de la vidéo source."""
    if not shutil.which("ffmpeg"):
        raise RuntimeError("ffmpeg introuvable dans le PATH.")

    duration = await probe_duration(source)
    logger.info(
        f"[{job_id}] source={source.name} duration={duration}s copies={copies} "
        f"filters={enabled_filters or 'all'}"
    )

    sem = asyncio.Semaphore(concurrency)

    async def _run(idx: int):
        async with sem:
            params = random_params(
                custom_ranges=custom_ranges,
                enabled_filters=enabled_filters,
            )
            return await process_one(source, duration, params, job_id, idx)

    tasks = [_run(i + 1) for i in range(copies)]
    return await asyncio.gather(*tasks)
