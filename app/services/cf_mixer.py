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
    candidates = [
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
        "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf",
        "/usr/share/fonts/TTF/DejaVuSans-Bold.ttf",
        "/Library/Fonts/Arial Bold.ttf",
        "C:/Windows/Fonts/arialbd.ttf",
    ]
    for c in candidates:
        if os.path.exists(c):
            return c
    return ""


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
) -> List[str]:
    font_size = font_size_px if font_size_px else _font_size_for_size(size_label)
    if position_pct is not None:
        # Convert percent (0..100) to a y= expr; account for text_h so text stays in frame
        # 0% = top, 100% = bottom
        pct = max(0.0, min(100.0, float(position_pct))) / 100.0
        y_expr = f"(h-text_h)*{pct:.4f}"
    else:
        y_expr = _y_position_for_align(align, font_size)
    font = _font_path()

    safe_text = _escape_drawtext(caption or "")
    drawtext_parts = [
        f"text='{safe_text}'",
        f"fontsize={font_size}",
        "fontcolor=white",
        "borderw=4",
        "bordercolor=black@0.85",
        "box=1",
        "boxcolor=black@0.45",
        "boxborderw=18",
        "x=(w-text_w)/2",
        f"y={y_expr}",
        "line_spacing=8",
    ]
    if font:
        drawtext_parts.insert(0, f"fontfile={font}")

    drawtext = "drawtext=" + ":".join(drawtext_parts)

    vf = (
        f"scale={TARGET_W}:{TARGET_H}:force_original_aspect_ratio=decrease,"
        f"pad={TARGET_W}:{TARGET_H}:(ow-iw)/2:(oh-ih)/2:color=black,"
        f"setsar=1,"
        + drawtext
    )

    cmd: List[str] = ["ffmpeg", "-y"]
    cmd += ["-i", video_path]
    if music_path and os.path.exists(music_path) and audio_priority != "video":
        cmd += ["-i", music_path]
    cmd += ["-vf", vf]

    if music_path and os.path.exists(music_path) and audio_priority != "video":
        cmd += ["-map", "0:v:0", "-map", "1:a:0", "-shortest"]
    else:
        cmd += ["-map", "0:v:0", "-map", "0:a?"]

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
    return cmd


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

    cmd = _build_ffmpeg_cmd(video_path, caption, align, size_label, music_path,
                            audio_priority, out_path, metadata=meta)

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
        if result.returncode != 0:
            raise RuntimeError(f"FFmpeg failed: {result.stderr[-500:]}")
    except subprocess.TimeoutExpired:
        raise RuntimeError("FFmpeg timeout")

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

        cmd = _build_ffmpeg_cmd(
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
                err = (proc.stderr.read() if proc.stderr else "")[-300:]
                yield {"type": "log", "level": "ERROR", "message": f"FFmpeg fail: {err.strip()[:120]}"}
                yield {"type": "item_error", "index": item_idx, "error": err}
                continue

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
