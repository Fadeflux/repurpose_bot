"""Video scanner & filter service.

Scans a local folder for videos, then applies automatic filters:
- horizontal: drops videos with W >= H (landscape)
- talking: drops videos where someone is talking (frequent face/lip motion)
- captions: drops videos that already have caption overlay text

NOTE: this scans a folder ON THE SERVER. Since the app runs on Railway,
local Windows paths from the user's machine cannot be accessed.
However, an UPLOAD-based variant is provided too (filter already-uploaded videos).
"""
import subprocess
import os
import tempfile
from pathlib import Path
from typing import Tuple, Optional, List, Dict, Any
import re

from app.services import cf_ocr as ocr


VIDEO_EXTS = {".mp4", ".mov", ".m4v", ".webm", ".avi", ".mkv"}


def get_video_info(path: str) -> Dict[str, Any]:
    """Return width, height, duration. Returns zeros on failure."""
    try:
        result = subprocess.run(
            [
                "ffprobe", "-v", "error",
                "-select_streams", "v:0",
                "-show_entries", "stream=width,height : format=duration",
                "-of", "default=noprint_wrappers=1:nokey=0",
                path,
            ],
            capture_output=True, text=True, timeout=15,
        )
        info = {"width": 0, "height": 0, "duration": 0.0}
        for line in result.stdout.splitlines():
            if "=" not in line:
                continue
            k, v = line.split("=", 1)
            k = k.strip()
            v = v.strip()
            if k == "width":
                info["width"] = int(v) if v.isdigit() else 0
            elif k == "height":
                info["height"] = int(v) if v.isdigit() else 0
            elif k == "duration":
                try:
                    info["duration"] = float(v)
                except ValueError:
                    pass
        return info
    except Exception:
        return {"width": 0, "height": 0, "duration": 0.0}


def is_horizontal(path: str, info: Optional[Dict[str, Any]] = None) -> bool:
    """Returns True if video is landscape (width >= height)."""
    if info is None:
        info = get_video_info(path)
    w, h = info.get("width", 0), info.get("height", 0)
    if w == 0 or h == 0:
        return False
    return w >= h


def _extract_frames_to_tmp(path: str, count: int = 3) -> List[str]:
    """Extract `count` frames evenly across the video. Returns list of paths."""
    info = get_video_info(path)
    duration = info.get("duration", 0)
    if duration <= 0.5:
        return []

    start = max(0.3, duration * 0.1)
    end = max(start + 0.3, duration * 0.9)
    span = max(0.1, end - start)

    tmp_dir = tempfile.mkdtemp(prefix="cf_filter_")
    out_paths = []
    for i in range(count):
        if count == 1:
            t = (start + end) / 2
        else:
            t = start + (span * i / (count - 1))
        out = os.path.join(tmp_dir, f"f_{i:02d}.png")
        try:
            subprocess.run(
                [
                    "ffmpeg", "-y", "-ss", f"{t:.2f}", "-i", path,
                    "-frames:v", "1", "-q:v", "3", "-vf", "scale=480:-1",
                    out,
                ],
                capture_output=True, timeout=20,
            )
            if os.path.exists(out) and os.path.getsize(out) > 0:
                out_paths.append(out)
        except Exception:
            pass
    return out_paths


def _cleanup(paths: List[str]):
    if not paths:
        return
    for p in paths:
        try:
            os.unlink(p)
        except Exception:
            pass
    try:
        os.rmdir(os.path.dirname(paths[0]))
    except Exception:
        pass


def has_caption_overlay(path: str) -> bool:
    """OCR a few frames; if substantial text is detected on >= 2 frames, return True."""
    frames = _extract_frames_to_tmp(path, count=3)
    if not frames:
        return False
    try:
        hits = 0
        for f in frames:
            text = ocr.extract_text_from_image(f)
            # Strong signal: text with at least 4 alphanumeric chars on multiple frames
            if text:
                cleaned = re.sub(r"[^\w]", "", text)
                if len(cleaned) >= 5:
                    hits += 1
        return hits >= 2
    finally:
        _cleanup(frames)


def is_talking(path: str) -> bool:
    """
    Lightweight heuristic for "person talking":
    Compute frame-to-frame difference in the central region (face area) over a few seconds.
    High variance = lots of motion in face area = probably talking.

    Uses ffmpeg `signalstats` on the central crop.
    Returns True if the signal exceeds a threshold.
    """
    info = get_video_info(path)
    duration = info.get("duration", 0)
    w, h = info.get("width", 0), info.get("height", 0)
    if duration < 1.0 or w == 0 or h == 0:
        return False

    # Sample first 5 seconds
    sample_dur = min(5.0, duration)

    # Crop central face region (top half, central 50% width)
    cw = int(w * 0.5)
    ch = int(h * 0.4)
    cx = int((w - cw) / 2)
    cy = int(h * 0.15)

    try:
        # signalstats -> reads YDIF (luma diff) per frame
        cmd = [
            "ffmpeg", "-y", "-t", f"{sample_dur:.2f}", "-i", path,
            "-vf", f"crop={cw}:{ch}:{cx}:{cy},signalstats,metadata=mode=print:key=lavfi.signalstats.YDIF",
            "-an", "-f", "null", "-",
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        # Parse YDIF values from stderr
        diffs = []
        for line in result.stderr.splitlines():
            m = re.search(r"YDIF=([\d.]+)", line)
            if m:
                try:
                    diffs.append(float(m.group(1)))
                except ValueError:
                    pass
        if not diffs:
            return False
        # Compute average diff between consecutive frames
        avg = sum(diffs) / len(diffs)
        # Empirical threshold: > 4.0 = lots of motion (talking, dancing).
        # Static videos (model posing) usually < 2.
        return avg > 4.0
    except Exception:
        return False


def scan_folder(folder: str, filter_horizontal: bool = True,
                filter_talking: bool = True, filter_captions: bool = True) -> Dict[str, Any]:
    """
    Scan a folder, return list of videos with their filter results.
    Each video:
      { path, filename, size, width, height, duration,
        kept: bool, reasons_dropped: [str] }
    """
    folder_path = Path(folder)
    if not folder_path.exists() or not folder_path.is_dir():
        return {"error": f"Dossier introuvable: {folder}", "videos": []}

    videos_meta: List[Dict[str, Any]] = []
    for f in sorted(folder_path.iterdir()):
        if not f.is_file():
            continue
        if f.suffix.lower() not in VIDEO_EXTS:
            continue

        info = get_video_info(str(f))
        meta = {
            "path": str(f),
            "filename": f.name,
            "size": f.stat().st_size,
            "width": info.get("width", 0),
            "height": info.get("height", 0),
            "duration": info.get("duration", 0),
            "kept": True,
            "reasons_dropped": [],
        }

        # Apply filters
        if filter_horizontal and is_horizontal(str(f), info):
            meta["kept"] = False
            meta["reasons_dropped"].append("horizontale")

        if meta["kept"] and filter_captions and has_caption_overlay(str(f)):
            meta["kept"] = False
            meta["reasons_dropped"].append("captions")

        if meta["kept"] and filter_talking and is_talking(str(f)):
            meta["kept"] = False
            meta["reasons_dropped"].append("parle")

        videos_meta.append(meta)

    kept = [v for v in videos_meta if v["kept"]]
    dropped = [v for v in videos_meta if not v["kept"]]
    return {
        "folder": folder,
        "total": len(videos_meta),
        "kept": len(kept),
        "dropped": len(dropped),
        "videos": videos_meta,
    }


def filter_uploaded_videos(uploaded_paths: List[Tuple[str, str]],
                           filter_horizontal: bool = True,
                           filter_talking: bool = True,
                           filter_captions: bool = True) -> List[Dict[str, Any]]:
    """
    Apply filters to already-uploaded videos.
    `uploaded_paths` is a list of (path, original_filename).
    Returns list of dicts with 'kept' bool and 'reasons_dropped'.
    """
    results = []
    for path, original_name in uploaded_paths:
        info = get_video_info(path)
        item = {
            "path": path,
            "original_name": original_name,
            "kept": True,
            "reasons_dropped": [],
            "width": info.get("width", 0),
            "height": info.get("height", 0),
        }
        if filter_horizontal and is_horizontal(path, info):
            item["kept"] = False
            item["reasons_dropped"].append("horizontale")
        if item["kept"] and filter_captions and has_caption_overlay(path):
            item["kept"] = False
            item["reasons_dropped"].append("captions")
        if item["kept"] and filter_talking and is_talking(path):
            item["kept"] = False
            item["reasons_dropped"].append("parle")
        results.append(item)
    return results
