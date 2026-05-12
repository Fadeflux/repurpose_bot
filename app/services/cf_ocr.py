"""OCR service - extract caption text from Instagram/TikTok screenshots.

Strategy (in priority order):
1. Cache lookup (PostgreSQL) — instant, free
2. Claude Haiku Vision — best quality on stylized captions, controlled cost
3. Tesseract (fallback) — local, free, lower quality

Cost controls:
- Haiku model (15x cheaper than Sonnet)
- Image resized to 512px max before API call (10x cheaper tokens)
- Daily limit (CLAUDE_OCR_MAX_PER_DAY env var, default 100)
- Cache by SHA256(image) → re-upload of same image = 0 API call
- Daily counter logged in Railway
"""
import os
import base64
import hashlib
import json
import logging
from pathlib import Path
from datetime import date
from typing import Optional, Tuple

logger = logging.getLogger(__name__)

# ====== CONFIG ======
CLAUDE_MODEL = os.environ.get("CLAUDE_OCR_MODEL", "claude-haiku-4-5-20251001")
MAX_PER_DAY = int(os.environ.get("CLAUDE_OCR_MAX_PER_DAY", "100"))
MAX_IMAGE_SIZE_PX = 512  # Resize image avant envoi pour réduire tokens

# ====== STATE (compteur quotidien en mémoire) ======
_today_date: Optional[date] = None
_today_count: int = 0


def _reset_counter_if_new_day() -> None:
    global _today_date, _today_count
    today = date.today()
    if _today_date != today:
        _today_date = today
        _today_count = 0


def _can_call_claude() -> bool:
    """Check si on est sous la limite quotidienne."""
    _reset_counter_if_new_day()
    return _today_count < MAX_PER_DAY


def _increment_counter() -> None:
    global _today_count
    _reset_counter_if_new_day()
    _today_count += 1
    logger.info(f"[claude_ocr] Today: {_today_count}/{MAX_PER_DAY} OCR calls used")


def _hash_image(image_path: str) -> str:
    """SHA256 du fichier pour cle de cache stable."""
    h = hashlib.sha256()
    with open(image_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def _cache_get(image_hash: str) -> Optional[str]:
    """Recupere le texte cached depuis PostgreSQL."""
    try:
        from app.services import cf_storage
        if not cf_storage.is_db_enabled():
            return None
        with cf_storage._get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("CREATE TABLE IF NOT EXISTS ocr_cache (image_hash TEXT PRIMARY KEY, extracted_text TEXT, created_at TIMESTAMPTZ DEFAULT NOW())")
                cur.execute("SELECT extracted_text FROM ocr_cache WHERE image_hash = %s", (image_hash,))
                row = cur.fetchone()
                conn.commit()
                if row:
                    return row[0]
    except Exception as e:
        logger.warning(f"[claude_ocr] cache_get failed: {e}")
    return None


def _cache_set(image_hash: str, text: str) -> None:
    """Stocke le texte en cache PostgreSQL."""
    try:
        from app.services import cf_storage
        if not cf_storage.is_db_enabled():
            return
        with cf_storage._get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("CREATE TABLE IF NOT EXISTS ocr_cache (image_hash TEXT PRIMARY KEY, extracted_text TEXT, created_at TIMESTAMPTZ DEFAULT NOW())")
                cur.execute(
                    "INSERT INTO ocr_cache (image_hash, extracted_text) VALUES (%s, %s) "
                    "ON CONFLICT (image_hash) DO UPDATE SET extracted_text = EXCLUDED.extracted_text",
                    (image_hash, text)
                )
                conn.commit()
    except Exception as e:
        logger.warning(f"[claude_ocr] cache_set failed: {e}")


def _resize_image_to_base64(image_path: str) -> Tuple[str, str]:
    """Redimensionne l'image a 512px max et retourne (base64, mime_type).
    Reduit la consommation de tokens de ~10x."""
    from PIL import Image
    import io

    img = Image.open(image_path)
    # Convert RGBA -> RGB pour JPEG (sinon ca plante)
    if img.mode in ("RGBA", "LA", "P"):
        bg = Image.new("RGB", img.size, (255, 255, 255))
        if img.mode == "P":
            img = img.convert("RGBA")
        bg.paste(img, mask=img.split()[-1] if img.mode == "RGBA" else None)
        img = bg

    # Resize en conservant le ratio, max 512px
    img.thumbnail((MAX_IMAGE_SIZE_PX, MAX_IMAGE_SIZE_PX), Image.LANCZOS)

    # Encode en JPEG (plus leger que PNG)
    buf = io.BytesIO()
    img.save(buf, format="JPEG", quality=85)
    buf.seek(0)
    b64 = base64.b64encode(buf.read()).decode("utf-8")
    return b64, "image/jpeg"


def _claude_vision_ocr(image_path: str) -> str:
    """Appelle Claude Haiku Vision pour extraire le texte de l'image."""
    api_key = os.environ.get("ANTHROPIC_API_KEY", "").strip()
    if not api_key:
        return ""

    try:
        import urllib.request
        b64, mime = _resize_image_to_base64(image_path)

        body = json.dumps({
            "model": CLAUDE_MODEL,
            "max_tokens": 200,
            "messages": [{
                "role": "user",
                "content": [
                    {
                        "type": "image",
                        "source": {"type": "base64", "media_type": mime, "data": b64}
                    },
                    {
                        "type": "text",
                        "text": "Extract ONLY the text overlay/caption visible on this Instagram/TikTok screenshot. Return ONLY the text, nothing else. No explanation, no quotes, no formatting. If there's no caption text, return an empty string."
                    }
                ]
            }]
        }).encode("utf-8")

        req = urllib.request.Request(
            "https://api.anthropic.com/v1/messages",
            data=body,
            headers={
                "x-api-key": api_key,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            method="POST"
        )

        with urllib.request.urlopen(req, timeout=20) as resp:
            data = json.loads(resp.read().decode("utf-8"))

        content = data.get("content", [])
        if content and content[0].get("type") == "text":
            return content[0].get("text", "").strip()
        return ""
    except Exception as e:
        logger.warning(f"[claude_ocr] Claude Vision call failed: {e}")
        return ""


def extract_text_from_image(image_path: str) -> str:
    """Extract text from screenshot.

    Priorite :
    1. Cache (instantane, gratuit)
    2. Claude Haiku Vision (qualite top, cout controle)
    3. Tesseract (fallback gratuit, qualite moyenne)
    """
    # === 1. CACHE LOOKUP ===
    image_hash = None
    try:
        image_hash = _hash_image(image_path)
        cached = _cache_get(image_hash)
        if cached is not None:
            logger.info(f"[claude_ocr] Cache HIT for {Path(image_path).name}")
            return _clean_caption(cached)
    except Exception as e:
        logger.warning(f"[claude_ocr] cache lookup failed: {e}")

    # === 2. CLAUDE VISION (avec limite quotidienne) ===
    if _can_call_claude():
        text = _claude_vision_ocr(image_path)
        if text:
            _increment_counter()
            if image_hash:
                _cache_set(image_hash, text)
            return _clean_caption(text)
        else:
            logger.warning("[claude_ocr] Claude Vision returned empty, falling back to Tesseract")
    else:
        logger.warning(f"[claude_ocr] Daily limit reached ({MAX_PER_DAY}), using Tesseract fallback")

    # === 3. TESSERACT FALLBACK ===
    try:
        text = _tesseract(image_path)
        if image_hash and text:
            _cache_set(image_hash, text)
        return _clean_caption(text)
    except Exception as e:
        logger.warning(f"[claude_ocr] Tesseract failed: {e}")
        return ""


def _tesseract(image_path: str) -> str:
    """Local OCR with Tesseract (fallback when Claude is unavailable)."""
    try:
        import subprocess
        result = subprocess.run(
            ["tesseract", image_path, "-", "-l", "eng+fra"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        return result.stdout.strip()
    except Exception as e:
        logger.warning(f"[tesseract] failed: {e}")
        return ""


def _clean_caption(text: str) -> str:
    """Nettoie le texte extrait (retire whitespace excessif, etc.)."""
    if not text:
        return ""
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    return "\n".join(lines).strip()
