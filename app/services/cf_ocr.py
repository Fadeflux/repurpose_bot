"""OCR service - extract caption text from Instagram/TikTok screenshots.

Strategy:
1. Try OCR.space API (env var OCR_SPACE_API_KEY) — much better on stylized overlay text
2. Fall back to local Tesseract if no API key OR if API call fails
"""
import os
import base64
import json
from pathlib import Path
from typing import Optional

try:
    import urllib.request
    import urllib.parse
except ImportError:
    pass


def extract_text_from_image(image_path: str) -> str:
    """Extract text from screenshot. Returns empty string if everything fails."""
    api_key = os.environ.get("OCR_SPACE_API_KEY", "").strip()

    # 1) Try OCR.space if key is set
    if api_key:
        try:
            text = _ocr_space(image_path, api_key)
            if text:
                return _clean_caption(text)
        except Exception as e:
            print(f"[OCR.space] failed, falling back to Tesseract: {e}")

    # 2) Fallback: Tesseract (local)
    try:
        return _tesseract(image_path)
    except Exception as e:
        print(f"[Tesseract] failed: {e}")
        return ""


def _ocr_space(image_path: str, api_key: str) -> str:
    """Call OCR.space API. Returns extracted text."""
    # Use multipart/form-data via urllib to avoid extra deps
    import io
    import mimetypes
    import uuid

    boundary = uuid.uuid4().hex
    fname = Path(image_path).name
    mime, _ = mimetypes.guess_type(fname)
    if not mime:
        mime = "image/png"

    with open(image_path, "rb") as f:
        file_bytes = f.read()

    body_parts = []

    def add_field(name: str, value: str):
        body_parts.append((
            f'--{boundary}\r\n'
            f'Content-Disposition: form-data; name="{name}"\r\n\r\n'
            f'{value}\r\n'
        ).encode())

    # Fields
    add_field("apikey", api_key)
    add_field("language", "fre")          # French; OCR.space auto-detects too
    add_field("isOverlayRequired", "false")
    add_field("scale", "true")
    add_field("OCREngine", "2")           # Engine 2 is much better on overlay text
    add_field("detectOrientation", "true")

    # File field
    body_parts.append((
        f'--{boundary}\r\n'
        f'Content-Disposition: form-data; name="file"; filename="{fname}"\r\n'
        f'Content-Type: {mime}\r\n\r\n'
    ).encode())
    body_parts.append(file_bytes)
    body_parts.append(f'\r\n--{boundary}--\r\n'.encode())

    body = b"".join(body_parts)
    req = urllib.request.Request(
        "https://api.ocr.space/parse/image",
        data=body,
        headers={"Content-Type": f"multipart/form-data; boundary={boundary}"},
        method="POST",
    )

    with urllib.request.urlopen(req, timeout=30) as resp:
        raw = resp.read().decode("utf-8")
    data = json.loads(raw)

    if data.get("IsErroredOnProcessing"):
        msg = data.get("ErrorMessage") or data.get("ErrorDetails") or "unknown"
        if isinstance(msg, list):
            msg = " | ".join(str(m) for m in msg)
        raise RuntimeError(f"OCR.space error: {msg}")

    parsed = data.get("ParsedResults") or []
    if not parsed:
        return ""
    return parsed[0].get("ParsedText", "") or ""


def _tesseract(image_path: str) -> str:
    """Local Tesseract fallback."""
    try:
        import pytesseract
        from PIL import Image
    except ImportError:
        return ""

    img = Image.open(image_path)
    try:
        text = pytesseract.image_to_string(img, lang="fra+eng")
    except Exception:
        text = pytesseract.image_to_string(img)
    return _clean_caption(text)


def _clean_caption(raw: str) -> str:
    """Clean up OCR output to keep only the meaningful caption."""
    if not raw:
        return ""
    lines = [ln.strip() for ln in raw.splitlines()]
    cleaned = []
    for ln in lines:
        if not ln:
            continue
        if len(ln) < 2:
            continue
        if not any(c.isalnum() for c in ln):
            continue
        cleaned.append(ln)
    return "\n".join(cleaned).strip()
