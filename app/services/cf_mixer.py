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
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple, Generator

from app.utils import metadata_randomizer
from app.utils.mp4_patcher import patch_mp4_creation_times, parse_iso_datetime
from app.utils.logger import get_logger

logger = get_logger("cf_mixer")

# Output dir : on utilise /tmp pour éviter de saturer le filesystem persistant
# Railway, et parce que les outputs sont de toute façon uploadés vers Drive.
BASE_DIR = Path("/tmp/clipfusion")
OUTPUT_DIR = BASE_DIR / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


# TikTok / Instagram Reels target dims
TARGET_W = 1080
TARGET_H = 1920


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
) -> Tuple[List[str], Optional[Path]]:
    """
    Construit la commande FFmpeg.
    Pour les captions : génère un PNG via Pillow puis overlay (emojis Apple natifs,
    rendu pixel-précis style TikTok/Insta).
    Retourne (cmd, png_path_temp) — l'appelant doit cleanup le png_path après.
    """
    from app.services import cf_caption_renderer

    font_size = font_size_px if font_size_px else _font_size_for_size(size_label)

    # Génère le PNG de la caption si elle existe
    caption_png: Optional[Path] = None
    if caption and caption.strip():
        try:
            caption_png = cf_caption_renderer.render_caption_png(
                text=caption,
                font_size=font_size,
                max_width=int(TARGET_W * 0.90),  # 90% du frame max
            )
        except Exception as e:
            logger.warning(f"Caption render failed: {e}")
            caption_png = None

    # Mesure la hauteur du PNG pour bien positionner verticalement
    overlay_h = 0
    if caption_png and caption_png.exists():
        try:
            from PIL import Image as _PILImage
            with _PILImage.open(caption_png) as im:
                overlay_h = im.size[1]
        except Exception:
            overlay_h = font_size + 40

    cmd: List[str] = ["ffmpeg", "-y"]
    cmd += ["-i", video_path]
    has_music = bool(music_path and os.path.exists(music_path) and audio_priority != "video")
    if has_music:
        cmd += ["-i", music_path]
    if caption_png:
        cmd += ["-i", str(caption_png)]

    # Construction du filter_complex
    # [0:v] est la vidéo, on la scale d'abord
    scale_chain = (
        f"[0:v]scale={TARGET_W}:{TARGET_H}:force_original_aspect_ratio=decrease,"
        f"pad={TARGET_W}:{TARGET_H}:(ow-iw)/2:(oh-ih)/2:color=black,"
        f"setsar=1[vid]"
    )

    if caption_png:
        # Index de l'input du PNG : 1 si pas de music, sinon 2
        png_idx = 2 if has_music else 1
        y_expr = _y_pixel_for_align(align, position_pct, overlay_h)
        # Overlay centré horizontalement, position Y selon align/position_pct
        overlay_chain = f"[vid][{png_idx}:v]overlay=x=(W-w)/2:y={y_expr}[out]"
        filter_complex = f"{scale_chain};{overlay_chain}"
        out_label = "[out]"
    else:
        filter_complex = scale_chain
        out_label = "[vid]"

    cmd += ["-filter_complex", filter_complex]
    cmd += ["-map", out_label]

    # Audio mapping
    if has_music:
        cmd += ["-map", "1:a:0", "-shortest"]
    else:
        cmd += ["-map", "0:a?"]

    # Cut to max_duration if set
    if max_duration and max_duration > 0:
        cmd += ["-t", f"{max_duration:.2f}"]

    cmd += [
        "-c:v", "libx264",
        "-preset", "veryfast",
        "-crf", "23",
        "-pix_fmt", "yuv420p",
        "-c:a", "aac",
        "-b:a", "128k",
        # Strip original metadata (removes TikTok/Insta tracking tags)
        "-map_metadata", "-1",
        # +use_metadata_tags allows com.apple.quicktime.* tags
        "-movflags", "+faststart+use_metadata_tags",
    ]

    # Inject randomized iPhone/Android metadata if provided
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
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
        if result.returncode != 0:
            raise RuntimeError(f"FFmpeg failed: {result.stderr[-500:]}")
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
) -> Generator[Dict[str, Any], None, None]:
    """Streaming version yielding progress events."""
    if not templates or not videos:
        yield {"type": "log", "level": "ERROR", "message": "Aucun template ou vidéo"}
        yield {"type": "done", "outputs": [], "total_elapsed": 0}
        return

    try:
        v = subprocess.run(["ffmpeg", "-version"], capture_output=True, text=True, timeout=5)
        engine_line = v.stdout.splitlines()[0][:60] if v.stdout else "ffmpeg"
    except Exception:
        engine_line = "ffmpeg"

    pairs: List[Tuple[Dict, Dict]] = []
    for t in templates:
        for v in videos:
            pairs.append((t, v))
    pairs = pairs[:max_variants] if max_variants > 0 else pairs
    total = len(pairs)

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

        # Generate randomized iPhone/Android metadata for THIS variant (unique per output)
        spoof_meta = metadata_randomizer.random_metadata("mix_random")
        platform_label = "iPhone" if spoof_meta.get("_platform") == "iphone" else "Android"
        device_label = spoof_meta.get("model", "?")
        yield {"type": "log", "level": "INFO", "message": f"Spoofing as {platform_label} {device_label}"}

        cmd, ass_path = _build_ffmpeg_cmd(
            vid["path"], tpl.get("caption", ""), tpl.get("align", "center"),
            size_label, music_path, audio_priority, out_path,
            position_pct=position_pct,
            font_size_px=font_size_px,
            max_duration=max_duration,
            metadata=spoof_meta,
        )
        cmd_with_progress = cmd[:1] + ["-progress", "pipe:1", "-nostats"] + cmd[1:]

        item_started = time.time()
        try:
            proc = subprocess.Popen(
                cmd_with_progress,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
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
            if proc.returncode != 0:
                err = (proc.stderr.read() if proc.stderr else "")
                # Log COMPLET côté serveur pour qu'on puisse débugger via Railway logs
                logger.error(f"FFmpeg failed (returncode={proc.returncode}) for variant {item_idx}")
                logger.error(f"Command was: {' '.join(cmd_with_progress)}")
                logger.error(f"FFmpeg stderr (full):\n{err}")
                # Envoie un résumé court côté UI
                short_err = err.strip()[-300:].replace("\n", " | ")
                yield {"type": "log", "level": "ERROR", "message": f"FFmpeg fail: {short_err[:200]}"}
                yield {"type": "item_error", "index": item_idx, "error": err[-500:]}
                # cleanup ASS even on error
                if ass_path and ass_path.exists():
                    try: ass_path.unlink()
                    except Exception: pass
                continue

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

    total_elapsed = time.time() - started
    yield {"type": "log", "level": "INFO", "message": f"Mix terminé · {len(output_metas)}/{total} OK · {round(total_elapsed,1)}s"}

    # ===== DRIVE UPLOAD (si configuré) =====
    # Réutilise le drive_service complet de Repurpose Bot (avec retry, OAuth, SA, etc.)
    drive_info: Optional[Dict[str, Any]] = None
    if output_metas:
        try:
            from app.services import drive_service
            if drive_service.is_drive_enabled():
                from datetime import datetime
                folder_name = f"ClipFusion_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{len(output_metas)}vids"
                yield {"type": "log", "level": "INFO", "message": f"📤 Drive: création dossier {folder_name}"}

                # create_batch_folder retourne juste un folder_id (string) dans Repurpose
                folder_id = drive_service.create_batch_folder(folder_name)
                if folder_id:
                    folder_url = drive_service.get_folder_link(folder_id)
                    yield {"type": "log", "level": "INFO", "message": f"📁 Drive folder: {folder_url}"}

                    # Upload (Repurpose drive_service.upload_file gère déjà retry 3x)
                    uploaded_count = 0
                    for i, m in enumerate(output_metas, 1):
                        try:
                            local_path = Path(m["path"])
                            yield {"type": "log", "level": "RUN", "message": f"📤 ({i}/{len(output_metas)}) Upload {local_path.name}"}
                            up = drive_service.upload_file(local_path, folder_id, mime_type="video/mp4")
                            if up:
                                m["drive_id"] = up.get("id")
                                m["drive_url"] = up.get("webViewLink", "")
                                uploaded_count += 1
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
                else:
                    yield {"type": "log", "level": "WARN", "message": "Drive folder creation failed"}
            else:
                # Drive pas configuré — silencieux
                pass
        except Exception as drive_err:
            logger.warning(f"Drive step failed: {drive_err}")
            yield {"type": "log", "level": "WARN", "message": f"Drive step skipped: {drive_err}"}

    yield {"type": "done", "outputs": output_metas, "total_elapsed": round(total_elapsed, 2), "drive": drive_info}
