"""OCR service - extract caption text from Instagram/TikTok screenshots.

Strategie : Claude Sonnet 4.6 Vision UNIQUEMENT.
Plus précis que Tesseract / OCR.space / Google Vision sur les captions stylisées
+ détecte et préserve les emojis (qu'aucun autre OCR mainstream ne fait bien).

Coût pour un volume de 100-200 templates/mois : ~0.50-1€/mois.

Variable d'env requise :
- ANTHROPIC_API_KEY : la clé API Anthropic (commence par sk-ant-)
"""
import os
import base64
import json
import logging
import mimetypes
from pathlib import Path
from typing import Optional

try:
    import urllib.request
    import urllib.error
except ImportError:
    pass

logger = logging.getLogger(__name__)


# Modèle Claude utilisé. Sonnet 4.6 = bon ratio qualité/prix pour OCR avec emojis.
# Pour basculer sur Haiku (moins cher) ou Opus (plus cher mais meilleur), changer ici.
CLAUDE_MODEL = "claude-sonnet-4-6"

# Prompt système pour l'OCR : on guide Claude pour qu'il sorte UNIQUEMENT
# le texte extrait, sans rien ajouter (pas de "Voici le texte extrait :", etc.)
OCR_PROMPT = (
    "Extract the EXACT text visible in this image, including all emojis "
    "(💕, 🤝, 🥺, etc.) and special characters. "
    "Output ONLY the raw text, exactly as it appears, with line breaks preserved. "
    "Do NOT add any explanation, prefix, or commentary. "
    "Do NOT translate or modify the text. "
    "If you see no text, output exactly the word: NONE"
)


def extract_text_from_image(image_path: str) -> str:
    """
    Extrait le texte d'un screenshot via Claude Vision.
    Retourne une chaîne vide en cas d'échec.
    """
    api_key = os.environ.get("ANTHROPIC_API_KEY", "").strip()
    if not api_key:
        logger.error(
            "[OCR] ANTHROPIC_API_KEY non configuré. "
            "Ajoute la variable Railway pour activer Claude Vision."
        )
        return ""

    try:
        text = _claude_vision(image_path, api_key)
        if text:
            logger.info(f"[OCR/Claude] extracted {len(text)} chars: {repr(text[:200])}")
            return _clean_caption(text)
        return ""
    except Exception as e:
        logger.error(f"[OCR/Claude] failed: {e}")
        return ""


# ---------------------------------------------------------------------------
# Claude Vision API
# ---------------------------------------------------------------------------
def _claude_vision(image_path: str, api_key: str) -> str:
    """
    Appelle l'API Anthropic Messages avec une image en base64.
    Retourne le texte extrait par Claude (raw, à passer ensuite par _clean_caption).
    """
    image_path_obj = Path(image_path)
    if not image_path_obj.exists():
        raise RuntimeError(f"Image not found: {image_path}")

    # Détermine le media type
    mime_type, _ = mimetypes.guess_type(image_path_obj.name)
    if not mime_type or not mime_type.startswith("image/"):
        ext = image_path_obj.suffix.lower()
        ext_map = {
            ".png": "image/png",
            ".jpg": "image/jpeg",
            ".jpeg": "image/jpeg",
            ".gif": "image/gif",
            ".webp": "image/webp",
        }
        mime_type = ext_map.get(ext, "image/png")

    with open(image_path_obj, "rb") as f:
        image_bytes = f.read()
    image_b64 = base64.standard_b64encode(image_bytes).decode("utf-8")

    # Construit le payload Messages API
    payload = {
        "model": CLAUDE_MODEL,
        "max_tokens": 1024,
        "messages": [
            {
                "role": "user",
                "content": [
                    {
                        "type": "image",
                        "source": {
                            "type": "base64",
                            "media_type": mime_type,
                            "data": image_b64,
                        },
                    },
                    {
                        "type": "text",
                        "text": OCR_PROMPT,
                    },
                ],
            }
        ],
    }

    body = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        "https://api.anthropic.com/v1/messages",
        data=body,
        headers={
            "Content-Type": "application/json",
            "x-api-key": api_key,
            "anthropic-version": "2023-06-01",
        },
        method="POST",
    )

    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            raw = resp.read().decode("utf-8")
    except urllib.error.HTTPError as e:
        try:
            err_body = e.read().decode("utf-8")
            err_data = json.loads(err_body)
            err_msg = err_data.get("error", {}).get("message", err_body)
        except Exception:
            err_msg = str(e)
        raise RuntimeError(f"Claude API HTTP {e.code}: {err_msg}")

    data = json.loads(raw)

    # Extrait le texte de la réponse
    content = data.get("content", [])
    text_parts = []
    for block in content:
        if block.get("type") == "text":
            text_parts.append(block.get("text", ""))
    text = "".join(text_parts).strip()

    # Si Claude a répondu "NONE" → pas de texte détecté
    if text.upper() == "NONE":
        return ""

    return text


# ---------------------------------------------------------------------------
# Cleaning
# ---------------------------------------------------------------------------
def _clean_caption(raw: str) -> str:
    """
    Clean up OCR output to keep only the meaningful caption.
    Preserves lines with alphanumeric chars OR emojis (Unicode > U+2600).
    Single emojis are also preserved.
    """
    if not raw:
        return ""
    lines = [ln.strip() for ln in raw.splitlines()]
    cleaned = []
    for ln in lines:
        if not ln:
            continue
        has_alnum = any(c.isalnum() for c in ln)
        has_emoji = any(ord(c) >= 0x2600 for c in ln)
        if not (has_alnum or has_emoji):
            continue
        # Skip lines too short EXCEPT if they're emojis
        if len(ln) < 2 and not has_emoji:
            continue
        cleaned.append(ln)
    return "\n".join(cleaned).strip()
