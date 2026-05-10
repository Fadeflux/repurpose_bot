"""OCR service - extract caption text from Instagram/TikTok screenshots.

Stratégie de fallback en cascade :
1. Google Vision API (env var GOOGLE_VISION_CREDENTIALS_JSON) — la meilleure qualité
   sur captions stylisées (gros texte blanc avec contour)
2. OCR.space API (env var OCR_SPACE_API_KEY) — fallback API gratuit
3. Tesseract local — fallback ultime, qualité plus basse mais 0 dépendance

Si une priorité plus haute est configurée mais échoue (timeout, quota, etc.),
on fallback automatiquement sur la suivante.
"""
import os
import base64
import json
import logging
from pathlib import Path
from typing import Optional

try:
    import urllib.request
    import urllib.parse
except ImportError:
    pass

logger = logging.getLogger(__name__)


def extract_text_from_image(image_path: str) -> str:
    """
    Extrait le texte d'un screenshot. Retourne une chaîne vide si tout échoue.

    Cascade :
    - 1. Google Vision (si GOOGLE_VISION_CREDENTIALS_JSON configuré)
    - 2. OCR.space (si OCR_SPACE_API_KEY configuré)
    - 3. Tesseract local (fallback ultime)
    """
    google_creds = os.environ.get("GOOGLE_VISION_CREDENTIALS_JSON", "").strip()
    ocr_space_key = os.environ.get("OCR_SPACE_API_KEY", "").strip()

    # 1) Google Vision (meilleure qualité)
    if google_creds:
        try:
            text = _google_vision(image_path, google_creds)
            if text:
                logger.info(f"[OCR] Google Vision extracted {len(text)} chars")
                return _clean_caption(text)
        except Exception as e:
            logger.warning(f"[OCR] Google Vision failed, falling back: {e}")

    # 2) OCR.space (fallback API)
    if ocr_space_key:
        try:
            text = _ocr_space(image_path, ocr_space_key)
            if text:
                logger.info(f"[OCR] OCR.space extracted {len(text)} chars")
                return _clean_caption(text)
        except Exception as e:
            logger.warning(f"[OCR] OCR.space failed, falling back to Tesseract: {e}")

    # 3) Tesseract (local, fallback ultime)
    try:
        text = _tesseract(image_path)
        if text:
            logger.info(f"[OCR] Tesseract extracted {len(text)} chars")
        return text
    except Exception as e:
        logger.error(f"[OCR] Tesseract failed: {e}")
        return ""


# ---------------------------------------------------------------------------
# Google Vision API
# ---------------------------------------------------------------------------
def _google_vision(image_path: str, credentials_json: str) -> str:
    """
    Appel à Google Cloud Vision API via la lib google-cloud-vision.
    Utilise document_text_detection qui est optimisé pour les overlays/captions
    (vs text_detection qui est plus pour les vidéos OCR brut).
    """
    try:
        from google.cloud import vision
        from google.oauth2 import service_account
    except ImportError as e:
        raise RuntimeError(
            "google-cloud-vision non installé. Ajoute 'google-cloud-vision' "
            "dans requirements.txt et redéploie."
        ) from e

    # Parse le JSON de credentials (passé via variable d'env)
    try:
        creds_dict = json.loads(credentials_json)
    except json.JSONDecodeError as e:
        raise RuntimeError(f"GOOGLE_VISION_CREDENTIALS_JSON invalide (pas du JSON): {e}")

    creds = service_account.Credentials.from_service_account_info(creds_dict)
    client = vision.ImageAnnotatorClient(credentials=creds)

    # Lit l'image en bytes
    with open(image_path, "rb") as f:
        content = f.read()
    image = vision.Image(content=content)

    # ImageContext avec language hints pour aider sur l'anglais (texte Insta)
    # et améliorer la détection des emojis (Google Vision les lit comme des "Cn" Unicode)
    image_context = vision.ImageContext(language_hints=["en"])

    # document_text_detection est plus performant pour les overlays/textes denses
    # que text_detection (utilisé pour les photos de panneaux/etc.)
    response = client.document_text_detection(
        image=image,
        image_context=image_context,
    )

    if response.error.message:
        raise RuntimeError(f"Google Vision API error: {response.error.message}")

    # Log brut pour diagnostic (montre exactement ce que Google a renvoyé)
    if response.full_text_annotation and response.full_text_annotation.text:
        raw = response.full_text_annotation.text
        logger.info(f"[OCR/GoogleVision] RAW result ({len(raw)} chars): {repr(raw[:200])}")
        return raw

    # Fallback : essaie text_annotations[0] si full_text vide
    if response.text_annotations:
        raw = response.text_annotations[0].description
        logger.info(f"[OCR/GoogleVision] RAW (text_annotations) ({len(raw)} chars): {repr(raw[:200])}")
        return raw

    return ""


# ---------------------------------------------------------------------------
# OCR.space API
# ---------------------------------------------------------------------------
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
    add_field("language", "eng")          # English for Insta captions
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


# ---------------------------------------------------------------------------
# Tesseract local (fallback ultime)
# ---------------------------------------------------------------------------
def _tesseract(image_path: str) -> str:
    """Local Tesseract fallback."""
    try:
        import pytesseract
        from PIL import Image
    except ImportError:
        return ""

    img = Image.open(image_path)
    try:
        text = pytesseract.image_to_string(img, lang="eng")
    except Exception:
        text = pytesseract.image_to_string(img)
    return _clean_caption(text)


# ---------------------------------------------------------------------------
# Cleaning
# ---------------------------------------------------------------------------
def _clean_caption(raw: str) -> str:
    """
    Clean up OCR output to keep only the meaningful caption.
    Preserves lines that have alphanumeric chars OR emojis (Unicode > U+2600).
    Single emojis (1 char) are also preserved.
    """
    if not raw:
        return ""
    lines = [ln.strip() for ln in raw.splitlines()]
    cleaned = []
    for ln in lines:
        if not ln:
            continue
        # Check si la ligne contient au moins un caractère utile (lettre/chiffre/emoji)
        has_alnum = any(c.isalnum() for c in ln)
        has_emoji = any(ord(c) >= 0x2600 for c in ln)
        if not (has_alnum or has_emoji):
            continue
        # Filtre des lignes trop courtes (genre "a", "I"), MAIS on garde les
        # lignes courtes qui contiennent un emoji (genre "💕" ou "🤝")
        if len(ln) < 2 and not has_emoji:
            continue
        cleaned.append(ln)
    return "\n".join(cleaned).strip()
