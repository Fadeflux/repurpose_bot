"""
Service de traitement vidéo via FFmpeg.

Construit une chaîne de filtres unique (single pass) pour maximiser
les performances tout en appliquant toutes les transformations demandées.
"""
import asyncio
import json
import shutil
import uuid
from pathlib import Path
from typing import Dict, List, Optional

from app.config import OUTPUT_DIR, settings
from app.utils.logger import get_logger
from app.utils.randomizer import random_params

logger = get_logger("ffmpeg_service")


# ---------------------------------------------------------------------------
# Sondage du fichier source (ffprobe) pour connaître la durée
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
def build_filter_complex(params: Dict[str, float]) -> str:
    """
    Assemble le filter_complex FFmpeg en une seule chaîne vidéo.

    Ordre des filtres choisi pour la qualité :
    1. fps      -> normalise le framerate d'entrée
    2. scale    -> met au format 1080x1920 avec préservation d'aspect
    3. crop     -> zoom (crop central puis rescale)
    4. rotate   -> micro-rotation (change les hash perceptuels)
    5. eq       -> brightness / contrast / saturation / gamma
    6. noise    -> bruit léger
    7. vignette -> assombrissement des bords
    8. setpts   -> variation de vitesse
    """
    W, H = settings.TARGET_WIDTH, settings.TARGET_HEIGHT
    zoom = params["zoom"]
    crop_w = int(W / zoom)
    crop_h = int(H / zoom)
    # rotation en degrés -> radians pour le filtre rotate
    rot_rad = params["rotation"] * 3.141592653589793 / 180.0
    # speed -> setpts = PTS / speed (accélère si >1)
    pts_factor = round(1.0 / params["speed"], 6)

    filters = [
        f"fps={params['framerate']}",
        # Scale puis pad pour garantir le canvas 1080x1920 même si ratio différent
        f"scale={W}:{H}:force_original_aspect_ratio=increase",
        f"crop={W}:{H}",
        # Zoom : crop central puis upscale
        f"crop={crop_w}:{crop_h}",
        f"scale={W}:{H}:flags=lanczos",
        # Rotation (bilinéaire, on garde le canvas)
        f"rotate={rot_rad}:ow={W}:oh={H}:c=black@0",
        # Correction colorimétrique
        (f"eq=brightness={params['brightness']}"
         f":contrast={params['contrast']}"
         f":saturation={params['saturation']}"
         f":gamma={params['gamma']}"),
        # Bruit temporel léger
        f"noise=alls={params['noise']}:allf=t",
        # Vignette
        f"vignette=angle={params['vignette']}",
        # Vitesse
        f"setpts={pts_factor}*PTS",
    ]
    return ",".join(filters)


def build_audio_filter(params: Dict[str, float]) -> str:
    """Ajuste la vitesse audio de la même manière que la vidéo."""
    # atempo accepte 0.5 à 2.0 ; ici on est dans 1.03-1.04 donc OK direct
    return f"atempo={params['speed']}"


# ---------------------------------------------------------------------------
# Traitement d'une copie
# ---------------------------------------------------------------------------
async def process_one(
    source: Path,
    duration: Optional[float],
    params: Dict[str, float],
    job_id: str,
    copy_index: int,
) -> Dict:
    """
    Génère une copie avec les paramètres donnés.
    Retourne un dict décrivant le résultat (succès ou erreur).
    """
    out_name = f"{job_id}_copy{copy_index:02d}_{uuid.uuid4().hex[:8]}.mp4"
    out_path = OUTPUT_DIR / out_name

    # Gestion des cuts : -ss en entrée (seek rapide), -to calculé
    cut_start = params["cut_start"]
    cut_end = params["cut_end"]

    input_args: List[str] = ["-ss", f"{cut_start:.3f}"]
    if duration is not None and duration > (cut_start + cut_end + 0.1):
        # On coupe aussi la fin : -to est un timestamp absolu par rapport au -ss
        clip_duration = duration - cut_start - cut_end
        input_args += ["-t", f"{clip_duration:.3f}"]

    vf = build_filter_complex(params)
    af = build_audio_filter(params)

    cmd: List[str] = [
        "ffmpeg", "-y",
        "-hide_banner", "-loglevel", "error",
        *input_args,
        "-i", str(source),
        "-vf", vf,
        "-af", af,
        "-c:v", settings.VIDEO_ENCODER,
        "-preset", settings.PRESET,
        "-b:v", f"{int(params['bitrate'])}k",
        "-maxrate", f"{int(params['bitrate'] * 1.2)}k",
        "-bufsize", f"{int(params['bitrate'] * 2)}k",
        "-pix_fmt", "yuv420p",
        "-movflags", "+faststart",  # meilleure lecture web/TikTok
        "-c:a", settings.AUDIO_CODEC,
        "-b:a", settings.AUDIO_BITRATE,
        "-ar", "44100",
        "-shortest",
        str(out_path),
    ]

    logger.info(f"[{job_id}] copy {copy_index} -> {out_name}")
    logger.debug(f"[{job_id}] cmd: {' '.join(cmd)}")

    proc = await asyncio.create_subprocess_exec(
        *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    _, stderr = await proc.communicate()

    if proc.returncode != 0:
        err = stderr.decode(errors="ignore")[-500:]
        logger.error(f"[{job_id}] ffmpeg a échoué (copy {copy_index}): {err}")
        # On ne lève pas, on renvoie une erreur structurée
        if out_path.exists():
            out_path.unlink(missing_ok=True)
        return {
            "copy_index": copy_index,
            "success": False,
            "error": err,
            "params": params,
        }

    return {
        "copy_index": copy_index,
        "success": True,
        "filename": out_name,
        "path": str(out_path),
        "size_bytes": out_path.stat().st_size,
        "params": params,
    }


# ---------------------------------------------------------------------------
# Orchestration : N copies en parallèle contrôlé
# ---------------------------------------------------------------------------
async def process_video(
    source: Path,
    copies: int,
    job_id: str,
    concurrency: int = 2,
) -> List[Dict]:
    """
    Génère `copies` variantes randomisées de la vidéo source.
    Concurrency limite le nb de ffmpeg en parallèle (FFmpeg est déjà multi-thread).
    """
    if not shutil.which("ffmpeg"):
        raise RuntimeError("ffmpeg introuvable dans le PATH.")

    duration = await probe_duration(source)
    logger.info(f"[{job_id}] source={source.name} duration={duration}s copies={copies}")

    sem = asyncio.Semaphore(concurrency)

    async def _run(idx: int):
        async with sem:
            params = random_params()
            return await process_one(source, duration, params, job_id, idx)

    tasks = [_run(i + 1) for i in range(copies)]
    return await asyncio.gather(*tasks)
