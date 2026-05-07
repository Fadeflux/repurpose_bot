"""Video extractor service.

Extracts:
- Caption text by sampling N frames evenly across the video and running OCR on each
- Audio track as MP3 (for music library)
"""
import subprocess
import os
import tempfile
from pathlib import Path
from typing import Tuple, Optional, List
import re
from collections import Counter

from app.services import cf_ocr as ocr


def get_duration(video_path: str) -> float:
    """Get video duration in seconds via ffprobe."""
    try:
        result = subprocess.run(
            [
                "ffprobe", "-v", "error",
                "-show_entries", "format=duration",
                "-of", "default=noprint_wrappers=1:nokey=1",
                video_path,
            ],
            capture_output=True, text=True, timeout=30,
        )
        return float(result.stdout.strip() or 0)
    except Exception:
        return 0.0


def extract_frames(video_path: str, num_frames: int = 6) -> List[str]:
    """Extract `num_frames` evenly-spaced frames as PNG. Returns list of paths."""
    duration = get_duration(video_path)
    if duration <= 0:
        return []

    # Avoid the very first/last 5% (intros/transitions)
    start = max(0.5, duration * 0.05)
    end = max(start + 0.5, duration * 0.95)
    span = max(0.1, end - start)

    frame_paths = []
    tmp_dir = tempfile.mkdtemp(prefix="clipfusion_frames_")
    for i in range(num_frames):
        if num_frames == 1:
            t = (start + end) / 2
        else:
            t = start + (span * i / (num_frames - 1))
        out = os.path.join(tmp_dir, f"frame_{i:02d}.png")
        try:
            subprocess.run(
                [
                    "ffmpeg", "-y", "-ss", f"{t:.2f}", "-i", video_path,
                    "-frames:v", "1", "-q:v", "2", out,
                ],
                capture_output=True, timeout=30,
            )
            if os.path.exists(out) and os.path.getsize(out) > 0:
                frame_paths.append(out)
        except Exception as e:
            print(f"Frame extract failed at t={t}: {e}")

    return frame_paths


def cleanup_frames(frame_paths: List[str]):
    """Delete frame files and their temp directory."""
    if not frame_paths:
        return
    for p in frame_paths:
        try:
            os.unlink(p)
        except Exception:
            pass
    try:
        os.rmdir(os.path.dirname(frame_paths[0]))
    except Exception:
        pass


def _normalize_caption(s: str) -> str:
    """Lower-case + strip punctuation for fuzzy matching."""
    s = s.lower()
    s = re.sub(r"[^\w\s]", "", s, flags=re.UNICODE)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def extract_caption_from_video(video_path: str, num_frames: int = 6) -> str:
    """
    Extract the most likely caption from a video by OCRing N frames.
    Strategy: keep the longest caption that appears in at least 2 frames
    (filters out random text like timestamps), or the longest one if no repeats.
    """
    frames = extract_frames(video_path, num_frames=num_frames)
    if not frames:
        return ""

    try:
        captions = []
        for f in frames:
            text = ocr.extract_text_from_image(f)
            if text and len(text.strip()) >= 3:
                captions.append(text.strip())

        if not captions:
            return ""

        # Group by normalized form to find recurring captions
        norm_groups = {}
        for cap in captions:
            key = _normalize_caption(cap)
            if not key:
                continue
            if key not in norm_groups:
                norm_groups[key] = []
            norm_groups[key].append(cap)

        if not norm_groups:
            return ""

        # Score: count of occurrences * length (favor recurring AND substantial captions)
        best_key = None
        best_score = -1
        for key, items in norm_groups.items():
            count = len(items)
            avg_len = sum(len(c) for c in items) / count
            # Recurring captions get a big boost
            score = count * 100 + avg_len
            if score > best_score:
                best_score = score
                best_key = key

        if best_key:
            # Return the longest variant of the winning group (best OCR quality)
            return max(norm_groups[best_key], key=len)
        return ""
    finally:
        cleanup_frames(frames)


def extract_audio_to_mp3(video_path: str, out_path: str) -> bool:
    """Extract audio track as MP3. Returns True on success."""
    try:
        result = subprocess.run(
            [
                "ffmpeg", "-y", "-i", video_path,
                "-vn", "-acodec", "libmp3lame", "-b:a", "192k",
                out_path,
            ],
            capture_output=True, timeout=300,
        )
        return result.returncode == 0 and os.path.exists(out_path) and os.path.getsize(out_path) > 0
    except Exception as e:
        print(f"Audio extract failed: {e}")
        return False
