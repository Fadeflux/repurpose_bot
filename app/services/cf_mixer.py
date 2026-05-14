"""
Mixer ClipFusion — adapté pour intégration Repurpose Bot.

Combine vidéos brutes + captions + musique via FFmpeg, génère du contenu
1080×1920 (TikTok/Reels) avec metadata spoofées (réutilise les modules
metadata_randomizer + mp4_patcher déjà présents dans Repurpose).
"""
import subprocess
import os
import shlex
import random
import time
import gc
import threading
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple, Generator

from app.utils import metadata_randomizer
from app.utils.mp4_patcher import patch_mp4_creation_times, parse_iso_datetime
from app.utils.logger import get_logger

logger = get_logger("cf_mixer")

# Output dir : volume persistant /data si dispo, sinon /tmp.
# Les outputs sont de toute façon uploadés vers Drive après mix.
from app.utils.storage_paths import BASE_DIR, OUTPUT_DIR
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def _startup_cleanup() -> None:
    """
    Cleanup au démarrage du module : supprime TOUS les fichiers du dossier output.

    Justification : le dossier OUTPUT_DIR ne sert qu'à stocker temporairement
    les mixes le temps de les uploader sur Drive. Une fois sur Drive, ils ne
    servent plus à rien sur le volume Railway (volume limité, crash si plein).

    Tout ce qui traîne dans OUTPUT_DIR au démarrage est forcément un reliquat
    d'un batch précédent (crash, redéploiement, upload Drive échoué). On peut
    tout supprimer SAFE — les vidéos sources (VIDEO_DIR), musique (MUSIC_DIR)
    et templates (TEMPLATE_DIR) ne sont PAS dans OUTPUT_DIR.
    """
    try:
        cleaned_count = 0
        cleaned_bytes = 0
        if OUTPUT_DIR.exists():
            for f in OUTPUT_DIR.iterdir():
                if not f.is_file():
                    continue
                try:
                    cleaned_bytes += f.stat().st_size
                    f.unlink()
                    cleaned_count += 1
                except Exception:
                    pass
        if cleaned_count > 0:
            mb = cleaned_bytes / (1024 * 1024)
            try:
                import logging as _logging
                _logger = _logging.getLogger(__name__)
                _logger.info(f"🧹 [cf_mixer startup] Cleanup: {cleaned_count} anciens fichiers supprimés ({mb:.1f} MB libérés)")
            except Exception:
                pass
    except Exception:
        pass


def _cleanup_output_dir_aggressive() -> tuple:
    """
    Cleanup AGRESSIF du dossier OUTPUT_DIR : supprime TOUS les fichiers.

    À appeler après chaque batch ou périodiquement. Le dossier ne sert qu'à
    stocker temporairement les mixes avant upload Drive. Si Drive a échoué,
    les fichiers sont perdus de toute façon (pas de retry, et le batch est
    déjà loggé en historique).

    Returns: (count, bytes_freed)
    """
    count = 0
    total_bytes = 0
    try:
        if OUTPUT_DIR.exists():
            for f in OUTPUT_DIR.iterdir():
                if not f.is_file():
                    continue
                try:
                    total_bytes += f.stat().st_size
                    f.unlink()
                    count += 1
                except Exception:
                    pass
    except Exception:
        pass
    return count, total_bytes


def _start_periodic_output_cleanup() -> None:
    """
    Lance un thread daemon qui vide OUTPUT_DIR toutes les 30 minutes.

    Sécurité multi-couche :
    1. Le cleanup post-batch (dans mix_videos_stream) supprime déjà après upload
    2. Le startup cleanup nettoie au boot
    3. Ce cleanup périodique attrape les orphelins (crashes silencieux, Drive
       upload échoué, etc.) pour éviter que le volume Railway se remplisse.
    """
    import threading
    import time as _time

    def _loop():
        # Attend 30min avant le premier cleanup (laisse le temps aux batchs actifs)
        _time.sleep(30 * 60)
        while True:
            try:
                count, bytes_freed = _cleanup_output_dir_aggressive()
                if count > 0:
                    mb = bytes_freed / (1024 * 1024)
                    try:
                        import logging as _logging
                        _logging.getLogger(__name__).info(
                            f"🧹 [cf_mixer periodic] Cleanup OUTPUT_DIR: "
                            f"{count} fichiers supprimés ({mb:.1f} MB libérés)"
                        )
                    except Exception:
                        pass
            except Exception:
                pass
            _time.sleep(30 * 60)  # 30 minutes

    try:
        t = threading.Thread(target=_loop, daemon=True, name="cf-output-cleanup")
        t.start()
    except Exception:
        pass


# Cleanup auto au démarrage du module (= démarrage du bot Railway)
_startup_cleanup()
# Lance le cleanup périodique en background
_start_periodic_output_cleanup()


# ===== CONCURRENCE LOCK =====
# Limite à 1 SEUL mix en parallèle (évite OOM kill quand 2+ VAs lancent /request ensemble).
# Les autres mix attendent leur tour dans la queue.
_MIX_LOCK = threading.Semaphore(1)


def _release_memory() -> None:
    """Force le garbage collector à libérer la RAM (utile après gros mix)."""
    try:
        gc.collect()
        gc.collect()  # 2e passe pour les objets cycliques
    except Exception:
        pass


# TikTok / Instagram Reels target dims
TARGET_W = 1080
TARGET_H = 1920


# ============================================================================
# SPOOF VIDEO PARAMS (alignés sur Repurpose Bot — bornes par défaut min/max)
# ============================================================================
# Ces plages sont identiques à PARAM_RANGES de Repurpose. On les redéfinit ici
# pour pas créer de couplage dur entre les 2 modules (ClipFusion peut évoluer
# indépendamment si besoin).
SPOOF_RANGES: Dict[str, Tuple[float, float]] = {
    "bitrate":    (8000, 12000),
    "brightness": (-0.05, 0.05),
    "contrast":   (0.95, 1.10),
    "saturation": (0.95, 1.15),
    "gamma":      (0.95, 1.05),
    "speed":      (1.03, 1.04),
    "zoom":       (1.03, 1.06),
    # Noise désactivé (0, 0) : rendu propre sans grain visible.
    # Les autres techniques (zoom/rotation/eq/vignette/speed/pitch/metadata)
    # cassent déjà efficacement le perceptual hash Insta/TikTok.
    "noise":      (0, 0),
    "vignette":   (0.20, 0.40),
    "rotation":   (-0.5, 0.5),
    "cut_start":  (0.1, 0.15),
    "cut_end":    (0.1, 0.15),
    # Audio randomization — change le pitch de l'audio sans changer la durée
    # (compensation atempo). Casse le hash audio Insta/TikTok.
    # Plage ±1.5% : imperceptible pour l'oreille humaine.
    "audio_pitch": (0.985, 1.015),
}
SPOOF_INT_KEYS = {"bitrate", "noise"}


def _sanitize_folder_part(s: str) -> str:
    """Nettoie une chaîne pour la mettre dans un nom de dossier Drive.
    Garde lettres, chiffres, tirets ; remplace espaces par '_' ; vire reste."""
    import re
    s = (s or "").strip()
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"[^A-Za-z0-9_\-]", "", s)
    return s[:40] or "x"


def _random_spoof_params(
    enabled_filters: Optional[List[str]] = None,
    custom_ranges: Optional[Dict[str, Tuple[float, float]]] = None,
) -> Dict[str, Optional[float]]:
    """
    Tire un set de paramètres aléatoires dans les bornes (similaire à Repurpose).

    enabled_filters : liste de clés activées (les autres → None = filtre skip)
    custom_ranges : surcharge des bornes par défaut, ex {"speed": (1.05, 1.10)}
    """
    out: Dict[str, Optional[float]] = {}
    for key, default_range in SPOOF_RANGES.items():
        if enabled_filters is not None and key not in enabled_filters:
            out[key] = None
            continue
        lo, hi = default_range
        if custom_ranges and key in custom_ranges:
            lo, hi = custom_ranges[key]
            if lo > hi:
                lo, hi = hi, lo
        if key in SPOOF_INT_KEYS:
            out[key] = random.randint(int(lo), int(hi))
        else:
            out[key] = round(random.uniform(lo, hi), 4)
    return out


def _build_video_spoof_chain(params: Dict[str, Optional[float]]) -> List[str]:
    """
    Construit la liste de filtres FFmpeg appliqués à [0:v] pour spoof la vidéo
    (indépendant de la caption overlay et du scale initial).
    Retourne une liste de filtres à concaténer dans le filter_complex.
    """
    chain: List[str] = []

    # Zoom (crop puis rescale)
    zoom = params.get("zoom")
    if zoom is not None and zoom > 1.0:
        crop_w = int(TARGET_W / zoom)
        crop_h = int(TARGET_H / zoom)
        crop_w -= crop_w % 2
        crop_h -= crop_h % 2
        chain.append(f"crop={crop_w}:{crop_h}")
        chain.append(f"scale={TARGET_W}:{TARGET_H}:flags=lanczos")
        chain.append("setsar=1:1")

    # Rotation (très subtile, ±0.5 degrés)
    rotation = params.get("rotation")
    if rotation is not None and abs(rotation) > 0.001:
        rot_rad = rotation * 3.141592653589793 / 180.0
        chain.append(f"rotate={rot_rad}:ow=rotw({rot_rad}):oh=roth({rot_rad}):c=black")
        chain.append(f"crop={TARGET_W}:{TARGET_H}")
        chain.append("setsar=1:1")

    # eq : brightness, contrast, saturation, gamma
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
        chain.append("eq=" + ":".join(eq_parts))

    # Noise
    noise = params.get("noise")
    if noise is not None and noise > 0:
        chain.append(f"noise=alls={int(noise)}:allf=t")

    # Vignette
    vignette = params.get("vignette")
    if vignette is not None and vignette > 0:
        chain.append(f"vignette=angle={vignette}")

    # Speed (PTS)
    speed = params.get("speed")
    if speed is not None and abs(speed - 1.0) > 0.001:
        pts_factor = round(1.0 / speed, 6)
        chain.append(f"setpts={pts_factor}*PTS")

    return chain


def _get_audio_sample_rate(path: str) -> int:
    """
    Retourne le sample rate (Hz) du 1er stream audio via ffprobe.
    Default 44100 si pas d'audio ou probe échoue.

    Important : sans ça, asetrate=44100*pitch assume une source 44.1 kHz, alors
    que 90% des vidéos modernes (iPhone, TikTok, Insta) sont en 48 kHz. Résultat :
    le pitch shift ±1.5% prévu devient ±7% réel (audible) et désynchro tempo.
    """
    try:
        result = subprocess.run(
            [
                "ffprobe", "-v", "error",
                "-select_streams", "a:0",
                "-show_entries", "stream=sample_rate",
                "-of", "default=noprint_wrappers=1:nokey=1",
                path,
            ],
            capture_output=True, text=True, timeout=10,
        )
        val = result.stdout.strip()
        if val and val.isdigit():
            sr = int(val)
            if sr > 0:
                return sr
    except Exception:
        pass
    return 44100


def _build_audio_spoof_filter(
    params: Dict[str, Optional[float]],
    source_sample_rate: int = 44100,
) -> Optional[str]:
    """
    Filtre audio pour spoof : changement de pitch (anti-fingerprint Insta/TikTok)
    + compensation tempo pour garder la durée correcte.

    Stratégie :
    - asetrate=source_sr*pitch → change la fréquence d'échantillonnage = pitch + tempo
    - aresample=source_sr → re-sample pour rester au sample rate d'origine
    - atempo=speed/pitch → compense le changement de tempo pour atteindre la vitesse voulue

    Résultat : audio avec pitch décalé de ±1.5% (imperceptible humain) qui casse
    les hash audio Insta/TikTok, tout en gardant la durée correcte.

    IMPORTANT : asetrate doit utiliser le sample rate RÉEL de la source (probe
    via ffprobe). Sinon le pitch effectif est faussé proportionnellement.
    """
    speed = params.get("speed") or 1.0
    pitch = params.get("audio_pitch") or 1.0

    # Si rien à faire, pas de filtre (économie CPU)
    if abs(speed - 1.0) < 0.001 and abs(pitch - 1.0) < 0.001:
        return None

    # Si pas de pitch shift mais speed change → comportement legacy (juste atempo)
    if abs(pitch - 1.0) < 0.001:
        return f"atempo={speed}"

    # Pitch shift via asetrate basé sur le sample rate RÉEL de la source.
    new_rate = int(source_sample_rate * pitch)
    # tempo final = speed / pitch (car asetrate a déjà appliqué un facteur de pitch)
    tempo_compensation = speed / pitch
    # atempo n'accepte que [0.5, 2.0] par étape, mais notre range est très petit donc OK
    return f"asetrate={new_rate},aresample={source_sample_rate},atempo={tempo_compensation:.6f}"


def _escape_drawtext(text: str) -> str:
    """Escape text for FFmpeg drawtext filter."""
    if not text:
        return ""
    text = text.replace("\\", "\\\\")
    text = text.replace(":", "\\:")
    text = text.replace("'", "\u2019")
    text = text.replace("%", "\\%")
    text = text.replace(",", "\\,")
    return text


def _y_position_for_align(align: str, font_size: int = 52, lines: int = 1) -> str:
    align = (align or "center").lower()
    if align == "top":
        return "h*0.12"
    if align == "tiktok":
        return "h*0.70"
    if align == "bottom":
        return "h*0.85-text_h"
    return "(h-text_h)/2"


def _font_size_for_size(size_label: str) -> int:
    mapping = {"S": 38, "M": 46, "L": 56, "XL": 72}
    return mapping.get((size_label or "L").upper(), 56)


def _get_video_duration(path: str) -> float:
    try:
        result = subprocess.run(
            [
                "ffprobe", "-v", "error",
                "-show_entries", "format=duration",
                "-of", "default=noprint_wrappers=1:nokey=1",
                path,
            ],
            capture_output=True, text=True, timeout=30,
        )
        return float(result.stdout.strip())
    except Exception:
        return 0.0


def _font_path() -> str:
    """Police principale pour le texte des captions. Priorité : Inter Bold (Insta-look)."""
    candidates = [
        "/usr/share/fonts/truetype/inter/Inter-Bold.ttf",
        "/usr/share/fonts/opentype/inter/Inter-Bold.otf",
        "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
        "/usr/share/fonts/TTF/DejaVuSans-Bold.ttf",
        "/Library/Fonts/Arial Bold.ttf",
        "C:/Windows/Fonts/arialbd.ttf",
    ]
    for c in candidates:
        if os.path.exists(c):
            return c
    return ""


def _font_family_name() -> str:
    """Nom de famille pour ASS (libass utilise fontconfig pour résoudre)."""
    # Inter est la meilleure police Insta-like en libre
    if os.path.exists("/usr/share/fonts/truetype/inter/Inter-Bold.ttf") or \
       os.path.exists("/usr/share/fonts/opentype/inter/Inter-Bold.otf"):
        return "Inter"
    if os.path.exists("/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf"):
        return "Liberation Sans"
    return "DejaVu Sans"


def _ass_escape(text: str) -> str:
    """Escape caractères spéciaux ASS. Garde les emojis intacts."""
    text = text.replace("\\", "\\\\")
    text = text.replace("{", "\\{").replace("}", "\\}")
    # Newlines en \N (ASS hard break)
    text = text.replace("\r\n", "\\N").replace("\n", "\\N").replace("\r", "\\N")
    return text


def _build_ass_file(
    caption: str,
    font_size: int,
    margin_v: int,
    align_code: int = 5,
    duration_s: float = 9999.0,
) -> Path:
    """
    Crée un fichier .ass temporaire pour le subtitles filter de FFmpeg.

    Style Insta :
    - Texte blanc, contour noir fin
    - Background semi-transparent (BorderStyle=4 = box derrière le texte)
    - Police Inter (Insta-like) si dispo
    - Emojis colorés via fontconfig fallback Noto Color Emoji
    """
    family = _font_family_name()
    # ASS colors are &HAABBGGRR (alpha + bgr)
    primary = "&H00FFFFFF"           # blanc opaque
    outline = "&H00000000"           # noir opaque
    back    = "&H80000000"           # noir 50% (alpha=80 hex = 128/255)

    # Aligns ASS (numpad layout) :
    # 7=top-left  8=top-center  9=top-right
    # 4=mid-left  5=mid-center  6=mid-right
    # 1=bot-left  2=bot-center  3=bot-right

    end_ts = max(1.0, duration_s)
    # Format ASS time : H:MM:SS.cc
    def fmt_t(s: float) -> str:
        h = int(s // 3600)
        m = int((s % 3600) // 60)
        sec = s - h * 3600 - m * 60
        return f"{h}:{m:02d}:{sec:05.2f}"

    end_str = fmt_t(end_ts)

    text_ass = _ass_escape(caption or "")

    ass_content = f"""[Script Info]
ScriptType: v4.00+
PlayResX: {TARGET_W}
PlayResY: {TARGET_H}
WrapStyle: 0
ScaledBorderAndShadow: yes

[V4+ Styles]
Format: Name, Fontname, Fontsize, PrimaryColour, SecondaryColour, OutlineColour, BackColour, Bold, Italic, Underline, StrikeOut, ScaleX, ScaleY, Spacing, Angle, BorderStyle, Outline, Shadow, Alignment, MarginL, MarginR, MarginV, Encoding
Style: Insta,{family},{font_size},{primary},&H000000FF,{outline},{back},1,0,0,0,100,100,0,0,3,3,0,{align_code},80,80,{margin_v},1

[Events]
Format: Layer, Start, End, Style, Name, MarginL, MarginR, MarginV, Effect, Text
Dialogue: 0,0:00:00.00,{end_str},Insta,,0,0,0,,{text_ass}
"""
    # Stocke dans /tmp avec nom unique
    ass_path = OUTPUT_DIR / f"_caption_{random.randint(100000, 999999)}.ass"
    ass_path.write_text(ass_content, encoding="utf-8")
    return ass_path


def _ass_align_for(align: str) -> int:
    """Convert align string en code numpad ASS."""
    a = (align or "center").lower()
    if a == "top":
        return 8        # haut centré
    if a == "tiktok":
        return 2        # bas centré (placement TikTok)
    if a == "bottom":
        return 2        # bas centré
    return 5            # milieu centré (center default)


def _margin_v_for_align(align: str, position_pct: Optional[float] = None) -> int:
    """Compute MarginV (pixels depuis le haut/bas selon align)."""
    if position_pct is not None:
        # Si on a un % custom, on convertit. ASS MarginV est en px depuis bas (pour align=2)
        # ou depuis haut (pour align=8). On reste simple en center et on règle via MarginV.
        return int(TARGET_H * (1.0 - max(0.0, min(100.0, position_pct)) / 100.0))
    a = (align or "center").lower()
    if a == "top":
        return int(TARGET_H * 0.10)
    if a == "tiktok":
        return int(TARGET_H * 0.18)  # un peu plus haut que le bas pur
    if a == "bottom":
        return int(TARGET_H * 0.06)
    return 0  # center : MarginV ignoré pour align=5


def _y_pixel_for_align(align: str, position_pct: Optional[float], img_height: int) -> str:
    """Retourne l'expression FFmpeg pour la position Y de l'overlay."""
    if position_pct is not None:
        pct = max(0.0, min(100.0, float(position_pct))) / 100.0
        return f"(H-{img_height})*{pct:.4f}"
    a = (align or "center").lower()
    if a == "top":
        return f"H*0.10"
    if a == "tiktok":
        return f"H*0.72"
    if a == "bottom":
        return f"H*0.85-{img_height}"
    return f"(H-{img_height})/2"


def _build_ffmpeg_cmd(
    video_path: str,
    caption: str,
    align: str,
    size_label: str,
    music_path: Optional[str],
    audio_priority: str,
    out_path: Path,
    position_pct: Optional[float] = None,  # 0..100 — overrides align if set
    font_size_px: Optional[int] = None,    # overrides size_label if set
    max_duration: Optional[float] = None,  # in seconds, cuts video if set
    metadata: Optional[Dict[str, str]] = None,  # iPhone/Android spoofed metadata
    caption_style: str = "outlined",       # "boxed" | "outlined"
    spoof_params: Optional[Dict[str, Optional[float]]] = None,  # video spoof filters
) -> Tuple[List[str], Optional[Path]]:
    """
    Construit la commande FFmpeg.
    Pour les captions : génère un PNG via Pillow puis overlay (emojis Apple natifs,
    rendu pixel-précis style TikTok/Insta).
    Pour le spoof vidéo : applique brightness/contrast/saturation/zoom/etc si
    spoof_params est fourni (alignés sur Repurpose Bot).
    Retourne (cmd, png_path_temp) — l'appelant doit cleanup le png_path après.
    """
    from app.services import cf_caption_renderer

    font_size = font_size_px if font_size_px else _font_size_for_size(size_label)

    # ===== Génère le PNG de la caption si elle existe =====
    caption_png: Optional[Path] = None
    if caption and caption.strip():
        try:
            caption_png = cf_caption_renderer.render_caption_png(
                text=caption,
                font_size=font_size,
                max_width=int(TARGET_W * 0.90),
                style=caption_style,
            )
        except Exception as e:
            logger.warning(f"Caption render failed: {e}")
            caption_png = None

    # Mesure la hauteur du PNG pour positionner verticalement
    overlay_h = 0
    if caption_png and caption_png.exists():
        try:
            from PIL import Image as _PILImage
            with _PILImage.open(caption_png) as im:
                overlay_h = im.size[1]
        except Exception:
            overlay_h = font_size + 40

    # ===== Spoof params (cuts en input args, le reste en filtre) =====
    cut_start = 0.0
    cut_end = 0.0
    bitrate_kbps = 10000  # défaut
    if spoof_params:
        cut_start = float(spoof_params.get("cut_start") or 0.0)
        cut_end = float(spoof_params.get("cut_end") or 0.0)
        bitrate_kbps = int(spoof_params.get("bitrate") or 10000)

    cmd: List[str] = ["ffmpeg", "-y"]

    # Cut start — appliqué à l'input pour économiser des frames
    if cut_start > 0:
        cmd += ["-ss", f"{cut_start:.3f}"]

    cmd += ["-i", video_path]
    has_music = bool(music_path and os.path.exists(music_path) and audio_priority != "video")
    if has_music:
        cmd += ["-i", music_path]
    if caption_png:
        cmd += ["-i", str(caption_png)]

    # ===== Construction du filter_complex =====
    # Étape 1 : scale + pad au format 1080x1920
    scale_filters = [
        f"scale={TARGET_W}:{TARGET_H}:force_original_aspect_ratio=decrease",
        f"pad={TARGET_W}:{TARGET_H}:(ow-iw)/2:(oh-ih)/2:color=black",
        "setsar=1",
    ]

    # Étape 2 : ajoute les filtres de spoof (zoom, rotation, eq, noise, vignette, speed)
    spoof_chain = _build_video_spoof_chain(spoof_params or {}) if spoof_params else []

    # Concatène : [0:v] -> scale -> spoof -> [vid]
    all_v_filters = scale_filters + spoof_chain
    scale_chain = f"[0:v]{','.join(all_v_filters)}[vid]"

    if caption_png:
        png_idx = 2 if has_music else 1
        y_expr = _y_pixel_for_align(align, position_pct, overlay_h)
        overlay_chain = f"[vid][{png_idx}:v]overlay=x=(W-w)/2:y={y_expr}[out]"
        filter_complex_parts = [scale_chain, overlay_chain]
        out_label = "[out]"
    else:
        filter_complex_parts = [scale_chain]
        out_label = "[vid]"

    # ===== Audio filter pour speed (atempo) + pitch (asetrate) =====
    # On probe le sample rate RÉEL du fichier audio source (vidéo ou musique)
    # pour calibrer asetrate. Sans ça on assume 44100 alors que les sources
    # iPhone/TikTok/Insta sont en 48000 → pitch shift faussé.
    audio_source_path = music_path if has_music else video_path
    audio_sr = _get_audio_sample_rate(audio_source_path) if spoof_params else 44100
    af = (
        _build_audio_spoof_filter(spoof_params or {}, source_sample_rate=audio_sr)
        if spoof_params else None
    )
    if af and not has_music:
        # Applique atempo + pitch à l'audio source vidéo
        filter_complex_parts.append(f"[0:a]{af}[aout]")
        audio_out_label = "[aout]"
    elif af and has_music:
        # Applique atempo + pitch à la musique source
        filter_complex_parts.append(f"[1:a]{af}[aout]")
        audio_out_label = "[aout]"
    else:
        audio_out_label = None

    filter_complex = ";".join(filter_complex_parts)
    cmd += ["-filter_complex", filter_complex]
    cmd += ["-map", out_label]

    # Audio mapping
    if audio_out_label:
        cmd += ["-map", audio_out_label]
        if has_music:
            cmd += ["-shortest"]
    elif has_music:
        cmd += ["-map", "1:a:0", "-shortest"]
    else:
        cmd += ["-map", "0:a?"]

    # Cut end : on calcule la durée à garder via -t
    if cut_end > 0 or (max_duration and max_duration > 0):
        if max_duration and max_duration > 0:
            cmd += ["-t", f"{max_duration:.2f}"]
        elif cut_end > 0:
            # On a besoin de la durée de la vidéo source pour calculer
            try:
                probe = subprocess.run(
                    ["ffprobe", "-v", "error", "-show_entries", "format=duration",
                     "-of", "default=noprint_wrappers=1:nokey=1", video_path],
                    capture_output=True, text=True, timeout=10,
                )
                src_dur = float(probe.stdout.strip() or "0")
                if src_dur > (cut_start + cut_end + 0.1):
                    target_dur = src_dur - cut_start - cut_end
                    cmd += ["-t", f"{target_dur:.3f}"]
            except Exception:
                pass

    cmd += [
        "-c:v", "libx264",
        # medium = compression 2-3x meilleure que veryfast à bitrate égal.
        # Encode plus lent (30s → 60-90s par vidéo) mais qualité visuelle nettement
        # supérieure (moins de pixellisation/artefacts dans le mouvement).
        "-preset", "medium",
        "-crf", "23",
        "-b:v", f"{bitrate_kbps}k",
        "-pix_fmt", "yuv420p",
        "-c:a", "aac",
        "-b:a", "128k",
        # Strip original metadata
        "-map_metadata", "-1",
        # bitexact : wipe complètement le tag Encoder auto-ajouté par FFmpeg
        # (sans ça, on a "Encoder: Lavf61.x.x" qui trahit FFmpeg même si on met
        # le metadata encoder= à vide).
        "-fflags", "+bitexact",
        "-flags:v", "+bitexact",
        "-flags:a", "+bitexact",
        # Major Brand "qt  " comme un vrai iPhone (au lieu de "MP4 Base Media v1" FFmpeg-style)
        "-brand", "qt  ",
        # use_metadata_tags : permet de définir des tags arbitraires (com.apple.quicktime.*)
        "-movflags", "+faststart+use_metadata_tags",
    ]

    if metadata:
        cmd += metadata_randomizer.metadata_to_ffmpeg_args(metadata)

    cmd += [str(out_path)]
    return cmd, caption_png


def mix_one(
    video_path: str,
    caption: str,
    align: str = "center",
    size_label: str = "L",
    music_path: Optional[str] = None,
    audio_priority: str = "template",
    output_filename: Optional[str] = None,
    device_choice: str = "mix_random",
) -> str:
    if not output_filename:
        base = Path(video_path).stem
        output_filename = f"mix_{base}_{random.randint(1000,9999)}.mp4"
    out_path = OUTPUT_DIR / output_filename

    # Generate randomized iPhone/Android metadata
    meta = metadata_randomizer.random_metadata(device_choice)

    cmd, ass_path = _build_ffmpeg_cmd(video_path, caption, align, size_label, music_path,
                                       audio_priority, out_path, metadata=meta)

    try:
        # MEM FIX : on n'utilise PAS capture_output=True (bufferise tout en RAM).
        # On redirige stderr vers un fichier temp et on relit que les derniers 2KB en cas d'erreur.
        # FFmpeg verbose peut produire des MB de logs sur des mix longs.
        import tempfile as _tempfile
        with _tempfile.NamedTemporaryFile(mode="w+b", delete=False, suffix=".log") as _err_log:
            _err_log_path = _err_log.name
        try:
            with open(_err_log_path, "wb") as _err_f:
                result = subprocess.run(
                    cmd,
                    stdout=subprocess.DEVNULL,
                    stderr=_err_f,
                    timeout=600,
                )
            if result.returncode != 0:
                # Lit seulement les 2 derniers KB du log pour message d'erreur
                try:
                    with open(_err_log_path, "rb") as _err_f:
                        _err_f.seek(0, 2)  # fin
                        _size = _err_f.tell()
                        _err_f.seek(max(0, _size - 2048))
                        _tail = _err_f.read().decode("utf-8", errors="ignore")
                except Exception:
                    _tail = ""
                raise RuntimeError(f"FFmpeg failed: {_tail[-500:]}")
        finally:
            try:
                Path(_err_log_path).unlink(missing_ok=True)
            except Exception:
                pass
    except subprocess.TimeoutExpired:
        raise RuntimeError("FFmpeg timeout")
    finally:
        # Cleanup ASS temp file
        if ass_path and ass_path.exists():
            try:
                ass_path.unlink()
            except Exception:
                pass

    # Post-patch MP4 atoms with distinct creation_time per stream
    try:
        fmt_dt = parse_iso_datetime(meta["creation_time"])
        vid_dt = parse_iso_datetime(meta["_video_creation_time"])
        aud_dt = parse_iso_datetime(meta["_audio_creation_time"])
        patch_mp4_creation_times(out_path, fmt_dt, vid_dt, aud_dt)
    except Exception as e:
        # Non-blocking: file is still valid, just less perfect spoof
        pass

    return str(out_path)


def mix_batch(
    templates: List[Dict[str, Any]],
    videos: List[Dict[str, Any]],
    music_list: Optional[List[Dict[str, Any]]] = None,
    max_variants: int = 1,
    size_label: str = "L",
    audio_priority: str = "template",
    progress_callback=None,
) -> List[str]:
    outputs: List[str] = []
    if not templates or not videos:
        return outputs

    pairs: List[Tuple[Dict, Dict]] = []
    for t in templates:
        for v in videos:
            pairs.append((t, v))
    # Mélange aléatoire pour éviter de toujours retomber sur les mêmes paires
    random.shuffle(pairs)
    pairs = pairs[:max_variants] if max_variants > 0 else pairs

    total = len(pairs)
    for idx, (tpl, vid) in enumerate(pairs):
        music_path = None
        if music_list:
            m = random.choice(music_list)
            music_path = m.get("path")
        try:
            out = mix_one(
                video_path=vid["path"],
                caption=tpl.get("caption", ""),
                align=tpl.get("align", "center"),
                size_label=size_label,
                music_path=music_path,
                audio_priority=audio_priority,
            )
            outputs.append(out)
            if progress_callback:
                progress_callback(idx + 1, total)
        except Exception as e:
            print(f"Mix failed for {vid.get('filename')}: {e}")

    return outputs


def mix_batch_stream(
    templates: List[Dict[str, Any]],
    videos: List[Dict[str, Any]],
    music_list: Optional[List[Dict[str, Any]]] = None,
    max_variants: int = 1,
    size_label: str = "L",
    audio_priority: str = "template",
    position_pct: Optional[float] = None,
    font_size_px: Optional[int] = None,
    max_duration: Optional[float] = None,
    caption_style: str = "outlined",
    device_choice: str = "smart_mix",
    va_name: str = "",
    team: str = "",
    enabled_filters: Optional[List[str]] = None,
    custom_ranges: Optional[Dict[str, Tuple[float, float]]] = None,
    model_id: Optional[int] = None,
    account: Optional[Dict[str, Any]] = None,
    tz_name: str = "benin",
) -> Generator[Dict[str, Any], None, None]:
    """
    Streaming version yielding progress events.

    Si `account` est fourni (dict avec 'username', 'device_choice', 'gps_lat',
    'gps_lng'), le mix utilise :
    - Le device LOCKÉ du compte (pas de random)
    - Le GPS LOCKÉ du compte (avec petit jitter)
    - Découpe les vidéos en 3 fenêtres horaires (matin 9h / soir 17h / nuit 23h)
      heure locale du VA (tz_name = 'benin' ou 'madagascar')
    - Surplus si N pas divisible par 3 → matin reçoit l'extra
    - Drive output organisé en 3 sous-dossiers

    Sans `account`, le comportement reste comme avant (random complet).
    """
    if not templates or not videos:
        yield {"type": "log", "level": "ERROR", "message": "Aucun template ou vidéo"}
        yield {"type": "done", "outputs": [], "total_elapsed": 0}
        return

    # ===== QUEUE : 1 SEUL MIX À LA FOIS (évite OOM kill) =====
    # Si un autre mix est en cours, on attend qu'il finisse avant de commencer.
    waiting_for_lock = not _MIX_LOCK.acquire(blocking=False)
    if waiting_for_lock:
        yield {"type": "log", "level": "INFO",
               "message": "⏳ Un autre mix est en cours, attente de mon tour..."}
        _MIX_LOCK.acquire(blocking=True)  # Bloque jusqu'à libération
        yield {"type": "log", "level": "INFO", "message": "✅ Tour arrivé, lancement du mix"}

    try:
        v = subprocess.run(["ffmpeg", "-version"], capture_output=True, text=True, timeout=5)
        engine_line = v.stdout.splitlines()[0][:60] if v.stdout else "ffmpeg"
    except Exception:
        engine_line = "ffmpeg"

    pairs: List[Tuple[Dict, Dict]] = []
    for t in templates:
        for v in videos:
            pairs.append((t, v))
    # Mélange aléatoire pour éviter de toujours retomber sur les mêmes paires
    random.shuffle(pairs)
    pairs = pairs[:max_variants] if max_variants > 0 else pairs
    total = len(pairs)

    # Si un compte est fourni, on découpe en 3 fenêtres horaires
    # window_assignments[i] = (target_hour, window_label) pour la i-ème pair
    # Si pas de compte, tous à None → comportement legacy (random complet)
    window_assignments: List[Optional[Tuple[int, str]]] = [None] * total
    # iOS du compte pour ce batch : décidé une seule fois en début de batch
    # (toutes les vidéos du batch auront le même iOS, comme un vrai humain)
    batch_ios_version = None
    if account and total > 0:
        # Décide l'iOS pour ce batch (dernière version dispo pour ce device)
        try:
            device_choice = account.get("device_choice", "")
            model_name = metadata_randomizer._IPHONE_MAP.get(device_choice, "iPhone 16 Pro Max")
            # Récupère les iOS dispo pour ce modèle et prend la dernière (la plus récente)
            for m, ios_versions in metadata_randomizer.IPHONE_MODELS:
                if m == model_name:
                    batch_ios_version = ios_versions[-1]
                    break
            if not batch_ios_version:
                batch_ios_version = "26.4.1"

            # Update DB si différent de l'ancien
            current_ios = account.get("ios_version", "") or ""
            if account.get("id") and current_ios != batch_ios_version:
                try:
                    from app.services import cf_storage as _cfs_ios
                    _cfs_ios.update_account_ios(int(account["id"]), batch_ios_version)
                    account["ios_version"] = batch_ios_version
                    if current_ios:
                        yield {"type": "log", "level": "INFO",
                               "message": f"📱 iOS update : {current_ios} → {batch_ios_version} (compte @{account.get('username','?')})"}
                    else:
                        yield {"type": "log", "level": "INFO",
                               "message": f"📱 iOS initial : {batch_ios_version} pour @{account.get('username','?')}"}
                except Exception as _ios_err:
                    yield {"type": "log", "level": "WARN",
                           "message": f"iOS DB update failed: {_ios_err}"}
        except Exception as _pick_err:
            yield {"type": "log", "level": "WARN",
                   "message": f"iOS pick failed, fallback random: {_pick_err}"}

        # Répartition : matin (1/3 + extra), soir (1/3), nuit (1/3)
        # ex pour 10 → matin=4, soir=3, nuit=3
        n_per = total // 3
        extra = total % 3
        n_matin = n_per + extra  # le matin reçoit l'extra
        n_soir = n_per
        n_nuit = n_per
        # Les pairs sont déjà shuffled, on assigne séquentiellement
        for i in range(n_matin):
            window_assignments[i] = (9, "matin")
        for i in range(n_matin, n_matin + n_soir):
            window_assignments[i] = (17, "soir")
        for i in range(n_matin + n_soir, total):
            window_assignments[i] = (23, "nuit")
        yield {"type": "log", "level": "INFO",
               "message": f"📅 Compte @{account.get('username', '?')} · "
                          f"device={account.get('device_choice', '?')} · "
                          f"GPS={account.get('gps_city', '?')}"}
        yield {"type": "log", "level": "INFO",
               "message": f"🕐 Répartition fenêtres : matin {n_matin} · soir {n_soir} · nuit {n_nuit} (TZ={tz_name})"}

    yield {"type": "init", "total": total, "engine": engine_line}
    yield {"type": "log", "level": "RUN", "message": f"Lancement du mix · {total} variante(s)"}
    yield {"type": "log", "level": "INIT", "message": "Starting mix engine..."}
    try:
        import multiprocessing
        cores = multiprocessing.cpu_count()
    except Exception:
        cores = 1
    yield {"type": "log", "level": "INFO", "message": f"CPU: {cores} cores · ffmpeg ready"}
    yield {"type": "log", "level": "INFO", "message": f"Output: {OUTPUT_DIR.name}/"}
    yield {"type": "log", "level": "INFO", "message": "Caption pre-render OK"}

    # ===== CLEANUP : supprime les fichiers orphelins du dernier batch =====
    # (mix_*.mp4 et _caption_*.ass laissés par des batchs précédents qui ont planté)
    try:
        cleaned_count = 0
        cleaned_bytes = 0
        for f in OUTPUT_DIR.glob("mix_*.mp4"):
            try:
                cleaned_bytes += f.stat().st_size
                f.unlink()
                cleaned_count += 1
            except Exception:
                pass
        for f in OUTPUT_DIR.glob("_caption_*.ass"):
            try:
                f.unlink()
            except Exception:
                pass
        if cleaned_count > 0:
            mb = cleaned_bytes / (1024 * 1024)
            yield {"type": "log", "level": "INFO",
                   "message": f"🧹 Cleanup: {cleaned_count} anciens fichiers supprimés ({mb:.1f} MB libérés)"}
    except Exception as _clean_err:
        yield {"type": "log", "level": "WARN", "message": f"Cleanup start failed: {_clean_err}"}

    started = time.time()
    output_metas: List[Dict[str, Any]] = []

    for idx, (tpl, vid) in enumerate(pairs):
        item_idx = idx + 1
        music_path = None
        if music_list and audio_priority == "music":
            m = random.choice(music_list)
            music_path = m.get("path")

        base = Path(vid["path"]).stem
        out_filename = f"mix_{base}_{random.randint(1000,9999)}.mp4"
        out_path = OUTPUT_DIR / out_filename

        original_name = vid.get("original_name", vid.get("filename", "?"))
        yield {
            "type": "item_start",
            "index": item_idx,
            "total": total,
            "filename": original_name,
        }
        yield {"type": "log", "level": "RUN", "message": f"({item_idx}/{total}) libx264 · {original_name[:40]}"}

        duration = _get_video_duration(vid["path"]) or 1.0

        # Generate metadata pour CETTE variante
        # Si on a un compte avec window_assignment, on utilise device + GPS lockés
        # + creation_time dans la fenêtre cible. Sinon, comportement legacy random.
        window = window_assignments[idx] if idx < len(window_assignments) else None
        if account and window:
            target_hour, window_label = window
            # Lookup altitude cohérente avec la ville (Miami=2m, Denver=1600m, etc.)
            try:
                from app.services import cf_storage as _cfs_alt
                gps_alt = _cfs_alt.get_city_altitude(account.get("gps_city", ""))
            except Exception:
                gps_alt = 0

            # Génère metadata iPhone pour le device locké au compte
            model_name = metadata_randomizer._IPHONE_MAP.get(
                account["device_choice"], "iPhone 16 Pro Max"
            )
            spoof_meta = metadata_randomizer._iphone_metadata_fixed(model_name)
            # Force la dernière iOS (cohérence sur tout le batch)
            if batch_ios_version:
                spoof_meta["com.apple.quicktime.software"] = batch_ios_version
            # Override le GPS avec celui locké du compte
            try:
                location_str = metadata_randomizer._format_iso6709(
                    float(account["gps_lat"]),
                    float(account["gps_lng"]),
                    float(gps_alt or 0),
                )
                spoof_meta["location"] = location_str
                spoof_meta["location-eng"] = location_str
                spoof_meta["com.apple.quicktime.location.ISO6709"] = location_str
            except Exception:
                pass
            device_label = spoof_meta.get("model", "?")
            yield {"type": "log", "level": "INFO",
                   "message": f"🔒 [{window_label} {target_hour}h] {device_label} · @{account.get('username', '?')}"}
        else:
            # Comportement legacy : random complet
            spoof_meta = metadata_randomizer.random_metadata(device_choice or "mix_random")
            platform_label = "iPhone" if spoof_meta.get("_platform") == "iphone" else "Android"
            device_label = spoof_meta.get("model", "?")
            yield {"type": "log", "level": "INFO", "message": f"Spoofing as {platform_label} {device_label}"}

        # Tirage aléatoire des params spoof vidéo (différent à chaque variante)
        # Si enabled_filters est None -> tous activés, sinon seulement ceux listés
        # Si compte avec device locké, on override le bitrate range pour qu'il soit
        # cohérent avec le device (Pro Max plus haut, etc.)
        # Si compte avec device locké, on override le bitrate range pour qu'il soit
        # cohérent avec le device (Pro Max plus haut, etc.)
        device_custom_ranges = dict(custom_ranges or {})
        if account and window:
            try:
                # Bitrate table par modèle (kbps) - valeurs réalistes pour Instagram/TikTok.
                # Les vrais iPhones recordent en 1080p à 15-40 Mbps natif. Insta
                # re-encode tout à ~5 Mbps de toute façon, le bitrate brut n'est
                # PAS un signal de détection. On vise haut pour rendu net.
                # Les modèles "regular" (iPhone X / iPhone X Plus) tombent sur le
                # fallback Air/Plus (6000-8000), assez propre.
                _BITRATE_BY_MODEL = {
                    "iPhone 17 Pro Max": (10000, 13000),
                    "iPhone 17 Pro":     (9000, 11000),
                    "iPhone 17 Air":     (6000, 8000),
                    "iPhone 16 Pro Max": (10000, 13000),
                    "iPhone 16 Pro":     (9000, 11000),
                    "iPhone 16 Plus":    (6000, 8000),
                }
                bitrate_range = _BITRATE_BY_MODEL.get(spoof_meta.get("model", ""), (6000, 8000))
                device_custom_ranges["bitrate"] = bitrate_range
                # Note : on ne force PAS le fps de sortie. Garder le fps source
                # évite les artefacts de compression (force 60 fps depuis source 30 fps
                # = 2x plus de frames mais même bitrate = qualité dégradée).
                # Le fps de la vidéo reste celui de la source.
            except Exception:
                pass

        spoof_params = _random_spoof_params(
            enabled_filters=enabled_filters,
            custom_ranges=device_custom_ranges,
        )

        cmd, ass_path = _build_ffmpeg_cmd(
            vid["path"], tpl.get("caption", ""), tpl.get("align", "center"),
            size_label, music_path, audio_priority, out_path,
            position_pct=position_pct,
            font_size_px=font_size_px,
            max_duration=max_duration,
            metadata=spoof_meta,
            caption_style=caption_style,
            spoof_params=spoof_params,
        )
        cmd_with_progress = cmd[:1] + ["-progress", "pipe:1", "-nostats"] + cmd[1:]

        item_started = time.time()
        # MEM FIX : stderr en PIPE qui n'est jamais lu pendant l'exec = buffer pipe sature
        # à 64KB (Linux) puis ffmpeg se bloque OU Python accumule en RAM. On redirige
        # stderr vers un fichier temp, lu seulement si erreur.
        import tempfile as _tempfile
        with _tempfile.NamedTemporaryFile(mode="w+b", delete=False, suffix=".log") as _err_log:
            _err_log_path = _err_log.name
        _err_file_handle = open(_err_log_path, "wb")
        try:
            proc = subprocess.Popen(
                cmd_with_progress,
                stdout=subprocess.PIPE,
                stderr=_err_file_handle,
                text=True,
                bufsize=1,
            )

            last_emit = 0.0
            while True:
                line = proc.stdout.readline() if proc.stdout else ""
                if not line:
                    if proc.poll() is not None:
                        break
                    time.sleep(0.05)
                    continue
                line = line.strip()
                if line.startswith("out_time_ms="):
                    try:
                        ms = int(line.split("=", 1)[1])
                        secs = ms / 1_000_000.0
                        pct = min(100, max(0, int((secs / duration) * 100)))
                        now = time.time()
                        if now - last_emit > 0.25:
                            elapsed = now - item_started
                            yield {
                                "type": "item_progress",
                                "index": item_idx,
                                "percent": pct,
                                "elapsed": round(elapsed, 1),
                            }
                            last_emit = now
                    except Exception:
                        pass
                elif line.startswith("progress=end"):
                    break

            proc.wait(timeout=600)
            # Ferme stdout pour libérer le pipe
            try:
                if proc.stdout:
                    proc.stdout.close()
            except Exception:
                pass
            # Flush et ferme le file handle stderr
            try:
                _err_file_handle.flush()
                _err_file_handle.close()
            except Exception:
                pass

            if proc.returncode != 0:
                # Lit seulement les 2 derniers KB du log d'erreur (pas tout en RAM)
                err = ""
                try:
                    with open(_err_log_path, "rb") as _ef:
                        _ef.seek(0, 2)
                        _size = _ef.tell()
                        _ef.seek(max(0, _size - 2048))
                        err = _ef.read().decode("utf-8", errors="ignore")
                except Exception:
                    err = ""
                # Log COMPLET côté serveur pour qu'on puisse débugger via Railway logs
                logger.error(f"FFmpeg failed (returncode={proc.returncode}) for variant {item_idx}")
                logger.error(f"Command was: {' '.join(cmd_with_progress)}")
                logger.error(f"FFmpeg stderr (last 2KB):\n{err}")
                # Envoie un résumé court côté UI
                short_err = err.strip()[-300:].replace("\n", " | ")
                yield {"type": "log", "level": "ERROR", "message": f"FFmpeg fail: {short_err[:200]}"}
                yield {"type": "item_error", "index": item_idx, "error": err[-500:]}
                # cleanup ASS even on error
                if ass_path and ass_path.exists():
                    try: ass_path.unlink()
                    except Exception: pass
                # cleanup log temp
                try: Path(_err_log_path).unlink(missing_ok=True)
                except Exception: pass
                continue

            # cleanup log temp succès
            try: Path(_err_log_path).unlink(missing_ok=True)
            except Exception: pass

            # Cleanup ASS temp après ffmpeg success
            if ass_path and ass_path.exists():
                try: ass_path.unlink()
                except Exception: pass

            # Post-patch MP4 atoms with distinct creation_time per stream
            try:
                fmt_dt = parse_iso_datetime(spoof_meta["creation_time"])
                vid_dt = parse_iso_datetime(spoof_meta["_video_creation_time"])
                aud_dt = parse_iso_datetime(spoof_meta["_audio_creation_time"])
                patch_mp4_creation_times(out_path, fmt_dt, vid_dt, aud_dt)
            except Exception as patch_err:
                yield {"type": "log", "level": "WARN", "message": f"MP4 patch skipped: {patch_err}"}

            elapsed = time.time() - item_started
            url = f"/output/{out_filename}"
            meta = {"filename": out_filename, "path": str(out_path), "url": url}
            # Attache la fenêtre horaire pour le rangement Drive en sous-dossiers
            if window:
                meta["window_hour"] = window[0]
                meta["window_label"] = window[1]
            output_metas.append(meta)
            yield {"type": "item_progress", "index": item_idx, "percent": 100, "elapsed": round(elapsed, 1)}
            yield {"type": "item_done", "index": item_idx, "output": meta, "duration": round(elapsed, 2)}
            yield {"type": "log", "level": "INFO", "message": f"({item_idx}/{total}) ✓ {out_filename[:40]} · {round(elapsed,1)}s"}

        except subprocess.TimeoutExpired:
            yield {"type": "log", "level": "ERROR", "message": f"Timeout on item {item_idx}"}
            yield {"type": "item_error", "index": item_idx, "error": "timeout"}
        except Exception as e:
            yield {"type": "log", "level": "ERROR", "message": f"Exception: {e}"}
            yield {"type": "item_error", "index": item_idx, "error": str(e)}
        finally:
            # MEM FIX : cleanup garanti du file handle + log temp pour ne pas leak les FD
            try:
                if not _err_file_handle.closed:
                    _err_file_handle.close()
            except Exception:
                pass
            try:
                Path(_err_log_path).unlink(missing_ok=True)
            except Exception:
                pass
            # Force GC après chaque variant pour libérer les buffers ffmpeg
            import gc as _gc
            _gc.collect()

    total_elapsed = time.time() - started
    yield {"type": "log", "level": "INFO", "message": f"Mix terminé · {len(output_metas)}/{total} OK · {round(total_elapsed,1)}s"}

    # ===== DRIVE UPLOAD + VA SHARE + DISCORD (si configuré) =====
    drive_info: Optional[Dict[str, Any]] = None
    if output_metas:
        try:
            from app.services import drive_service
            if drive_service.is_drive_enabled():
                from datetime import datetime
                # Récupère le label du modèle si model_id fourni
                model_label = ""
                if model_id:
                    try:
                        from app.services import cf_storage as _cfs
                        m = _cfs.get_model(int(model_id))
                        if m:
                            model_label = m.get("label") or f"ID{model_id}"
                    except Exception:
                        model_label = f"ID{model_id}"

                # Nom du dossier :
                # - Avec compte : @username_VA_équipe_modèle_date_Nvids
                # - Sans compte (legacy) : ClipFusion_VA_équipe_modèle_date_Nvids
                date_part = datetime.now().strftime('%Y%m%d_%H%M%S')
                parts = []
                if account and account.get("username"):
                    # Premier élément = nom du compte Insta (sans @)
                    parts.append(_sanitize_folder_part(account["username"]))
                else:
                    # Fallback legacy : préfixe "ClipFusion"
                    parts.append("ClipFusion")
                if va_name:
                    parts.append(_sanitize_folder_part(va_name))
                if team:
                    parts.append(team.capitalize())
                if model_id:
                    # Utilise le label si défini, sinon "ID{n}"
                    if model_label and model_label.lower() != f"modele {model_id}".lower():
                        parts.append(_sanitize_folder_part(model_label))
                    else:
                        parts.append(f"ID{model_id}")
                parts.append(date_part)
                parts.append(f"{len(output_metas)}vids")
                folder_name = "_".join(parts)
                yield {"type": "log", "level": "INFO", "message": f"📤 Drive: création dossier {folder_name}"}

                folder_id = drive_service.create_batch_folder(folder_name)
                if folder_id:
                    folder_url = drive_service.get_folder_link(folder_id)
                    yield {"type": "log", "level": "INFO", "message": f"📁 Drive folder: {folder_url}"}

                    # Si on a un compte avec fenêtres → on crée 3 sous-dossiers
                    # et on upload chaque vidéo dans son sous-dossier (matin/soir/nuit).
                    # Sinon, upload direct dans le dossier principal (legacy).
                    subfolder_ids: Dict[str, str] = {}
                    if account:
                        for label, hour in [("matin", 9), ("soir", 17), ("nuit", 23)]:
                            subname = f"{['01_matin_8h-9h','02_soir_16h-17h','03_nuit_22h-23h'][['matin','soir','nuit'].index(label)]}"
                            sub_id = drive_service.get_or_create_subfolder(folder_id, subname)
                            if sub_id:
                                subfolder_ids[label] = sub_id
                                yield {"type": "log", "level": "INFO", "message": f"📁 Sous-dossier {subname} créé"}
                            else:
                                yield {"type": "log", "level": "WARN", "message": f"⚠️ Échec création sous-dossier {subname}, upload dans dossier parent"}

                    uploaded_count = 0
                    for i, m in enumerate(output_metas, 1):
                        try:
                            local_path = Path(m["path"])
                            # Choix du dossier cible : sous-dossier de fenêtre si dispo, sinon principal
                            target_folder = folder_id
                            window_label = m.get("window_label")
                            if account and window_label and window_label in subfolder_ids:
                                target_folder = subfolder_ids[window_label]
                            yield {"type": "log", "level": "RUN", "message": f"📤 ({i}/{len(output_metas)}) Upload {local_path.name}"}
                            up = drive_service.upload_file(local_path, target_folder, mime_type="video/mp4")
                            if up:
                                m["drive_id"] = up.get("id")
                                m["drive_url"] = up.get("webViewLink", "")
                                uploaded_count += 1
                                # CLEANUP : supprime le fichier local après upload réussi
                                # pour libérer l'espace disque (volume Railway limité)
                                try:
                                    if local_path.exists():
                                        local_path.unlink()
                                except Exception as _cleanup_err:
                                    yield {"type": "log", "level": "WARN",
                                           "message": f"Cleanup failed for {local_path.name}: {_cleanup_err}"}
                            else:
                                yield {"type": "log", "level": "WARN", "message": f"❌ Upload failed: {local_path.name}"}
                        except Exception as up_err:
                            yield {"type": "log", "level": "WARN", "message": f"Upload error: {up_err}"}

                    drive_info = {
                        "folder_id": folder_id,
                        "folder_url": folder_url,
                        "folder_name": folder_name,
                        "uploaded": uploaded_count,
                        "total": len(output_metas),
                    }
                    yield {"type": "log", "level": "INFO", "message": f"✓ Drive: {uploaded_count}/{len(output_metas)} uploaded"}

                    # ===== VA SHARING + DISCORD NOTIFS (réutilise pipeline Repurpose) =====
                    va_email = None
                    va_discord_id = None
                    if va_name:
                        try:
                            from app.services.discord_va_sync import find_va_by_discord_id, find_va_discord_id
                            va_discord_id = find_va_discord_id(va_name)
                            va_info = find_va_by_discord_id(va_discord_id) if va_discord_id else None
                            va_email = va_info.get("email") if va_info else None

                            # Fallback Postgres si pas en cache
                            if not va_email and va_discord_id:
                                from app.services.va_emails_db import load_all_emails, is_db_enabled as va_db_enabled
                                if va_db_enabled():
                                    db_emails = load_all_emails()
                                    va_email = db_emails.get(str(va_discord_id))
                        except Exception as e:
                            logger.warning(f"Lookup VA email échoué: {e}")

                    if va_email:
                        try:
                            yield {"type": "log", "level": "INFO", "message": f"👥 Partage Drive avec {va_email}"}
                            share_result = drive_service.share_folder_with_users(folder_id, [va_email], role="writer")
                            ok_count = len(share_result.get("success", []))
                            yield {"type": "log", "level": "INFO", "message": f"✓ Partagé avec {ok_count} VA(s)"}
                            drive_info["shared_with"] = [va_email]
                        except Exception as e:
                            yield {"type": "log", "level": "WARN", "message": f"Partage VA échoué: {e}"}
                    elif va_name:
                        yield {"type": "log", "level": "WARN", "message": f"Pas d'email enregistré pour {va_name}, partage auto skip"}

                    # DM Discord au VA + notif équipe
                    if va_discord_id and va_email:
                        try:
                            import asyncio
                            from app.services.discord_bot import (
                                notify_va_drive_ready,
                                send_batch_notification_via_bot,
                                is_bot_enabled,
                            )
                            if is_bot_enabled():
                                # On lance les coroutines en async background
                                async def _send_notifs():
                                    try:
                                        await notify_va_drive_ready(va_discord_id, folder_url or "")
                                    except Exception as e:
                                        logger.warning(f"DM VA échoué: {e}")
                                    try:
                                        await send_batch_notification_via_bot(
                                            team=team or "geelark",
                                            va_name=va_name,
                                            va_discord_id=va_discord_id or "",
                                            batch_name=folder_name,
                                            total_requested=len(output_metas),
                                            succeeded=uploaded_count,
                                            failed=len(output_metas) - uploaded_count,
                                            drive_uploaded=uploaded_count,
                                            retries_used=0,
                                            duration_seconds=total_elapsed,
                                            drive_url=folder_url or "",
                                        )
                                    except Exception as e:
                                        logger.warning(f"Notif équipe échouée: {e}")

                                # Run in background thread (we are in a sync generator)
                                try:
                                    loop = asyncio.new_event_loop()
                                    loop.run_until_complete(_send_notifs())
                                    loop.close()
                                    yield {"type": "log", "level": "INFO", "message": f"💬 Discord notifié ({team or 'geelark'})"}
                                except Exception as e:
                                    logger.warning(f"Async loop notif échoué: {e}")
                        except Exception as e:
                            yield {"type": "log", "level": "WARN", "message": f"Discord notif skipped: {e}"}

                else:
                    yield {"type": "log", "level": "WARN", "message": "Drive folder creation failed"}
            else:
                pass
        except Exception as drive_err:
            logger.warning(f"Drive step failed: {drive_err}")
            yield {"type": "log", "level": "WARN", "message": f"Drive step skipped: {drive_err}"}

    # ===== Enregistre le batch dans l'historique (table cf_batches) =====
    try:
        from app.services import cf_storage
        di = drive_info or {}
        # Récupère le label du modèle (si pas déjà fait pour Drive folder)
        _model_label_to_save = ""
        if model_id:
            try:
                m = cf_storage.get_model(int(model_id))
                if m:
                    _model_label_to_save = m.get("label", "")
            except Exception:
                pass

        cf_storage.add_batch(
            va_name=va_name or "",
            team=team or "",
            device_choice=device_choice or "",
            videos_count=len(output_metas),
            videos_uploaded=di.get("uploaded", 0),
            drive_folder_id=di.get("folder_id", ""),
            drive_folder_url=di.get("folder_url", ""),
            drive_folder_name=di.get("folder_name", ""),
            va_email=(di.get("shared_with") or [""])[0] if di.get("shared_with") else "",
            discord_notified=bool(va_name and di.get("folder_id")),
            duration_seconds=round(total_elapsed, 2),
            model_id=int(model_id) if model_id else None,
            model_label=_model_label_to_save,
            account_username=(account.get("username", "") if account else ""),
        )
    except Exception as e:
        logger.warning(f"Save batch history échoué: {e}")

    yield {"type": "done", "outputs": output_metas, "total_elapsed": round(total_elapsed, 2), "drive": drive_info}

    # ===== CLEANUP FINAL AGRESSIF =====
    # Même si Drive a échoué pour certains fichiers, on les supprime de toute façon :
    # - Le volume Railway est limité, on ne peut pas se permettre de les garder
    # - Le batch est déjà enregistré en historique, donc traçable
    # - Si Drive a foiré, les vidéos sont perdues mais le serveur ne crash pas
    try:
        _final_count, _final_bytes = _cleanup_output_dir_aggressive()
        if _final_count > 0:
            _final_mb = _final_bytes / (1024 * 1024)
            logger.info(f"🧹 [cf_mixer end-of-batch] Cleanup OUTPUT_DIR: {_final_count} fichiers ({_final_mb:.1f} MB)")
    except Exception:
        pass

    # ===== CLEANUP MÉMOIRE (évite OOM kill au fil des batchs) =====
    try:
        _release_memory()
    except Exception:
        pass

    # ===== LIBÈRE LA QUEUE pour le prochain mix =====
    try:
        _MIX_LOCK.release()
    except Exception:
        pass
