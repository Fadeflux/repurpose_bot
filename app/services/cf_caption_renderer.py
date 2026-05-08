"""
Rendu pixel-parfait des captions style Insta/TikTok via Pillow.

Génère une image RGBA transparente avec :
  - Texte Inter Bold blanc (style Helvetica/Insta)
  - Background noir semi-transparent arrondi (style TikTok box)
  - Emojis natifs Apple Color (font apple-emoji-linux téléchargée par Dockerfile)

Stratégie : on dessine token par token. Les chunks de texte sont rendus avec Inter,
les chunks emoji sont rendus à 137px (taille SBIX native) puis resized au besoin
et collés à la position courante.

Utilisé en overlay FFmpeg : moteur > drawtext/subtitles, contrôle pixel-précis,
emojis colorés Apple natifs.
"""
import os
import re
from pathlib import Path
from typing import List, Optional, Tuple

from PIL import Image, ImageDraw, ImageFont

from app.utils.logger import get_logger

logger = get_logger("cf_caption_renderer")

# Path de la font Apple Color Emoji téléchargée par le Dockerfile
APPLE_EMOJI_FONT = Path("/opt/fonts/AppleColorEmoji.ttf")

# Taille SBIX native de la font Apple (la seule supportée pour le rendu).
# On dessine les emojis à cette taille puis on les resize.
APPLE_EMOJI_NATIVE_SIZE = 137

# Polices texte (Inter Bold = Insta-look)
FONT_TEXT_CANDIDATES = [
    "/usr/share/fonts/truetype/inter/Inter-Bold.ttf",
    "/usr/share/fonts/opentype/inter/Inter-Bold.otf",
    "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf",
    "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
]

# Style box TikTok/Insta
DEFAULT_BOX_ALPHA = 180        # ~70% noir (255 = opaque)
DEFAULT_BOX_RADIUS = 10        # rayon coins arrondis
DEFAULT_BOX_PAD_X = 22         # padding horizontal interne
DEFAULT_BOX_PAD_Y = 12         # padding vertical interne
DEFAULT_LINE_SPACING = 6


def _get_text_font_path() -> str:
    """Trouve la première police de texte dispo."""
    for c in FONT_TEXT_CANDIDATES:
        if os.path.exists(c):
            return c
    raise RuntimeError("Aucune police texte trouvée")


def is_apple_emoji_available() -> bool:
    """Check si la font Apple Color Emoji est dispo."""
    return APPLE_EMOJI_FONT.exists()


# ---------------------------------------------------------------------------
# Détection des emojis dans le texte
# ---------------------------------------------------------------------------
_EMOJI_PATTERN = re.compile(
    "["
    "\U0001F600-\U0001F64F"
    "\U0001F300-\U0001F5FF"
    "\U0001F680-\U0001F6FF"
    "\U0001F1E0-\U0001F1FF"
    "\U00002500-\U00002BEF"
    "\U00002702-\U000027B0"
    "\U000024C2-\U0001F251"
    "\U0001F900-\U0001F9FF"
    "\U0001FA00-\U0001FA6F"
    "\U0001FA70-\U0001FAFF"
    "\U00010000-\U0010FFFF"
    "\u2640-\u2642"
    "\u2600-\u2B55"
    "\u200d"
    "\u23cf"
    "\u23e9"
    "\u231a"
    "\ufe0f"
    "\u3030"
    "]+",
    flags=re.UNICODE,
)


def _tokenize(text: str) -> List[Tuple[str, str]]:
    """Coupe le texte en tokens (kind, value) : 'text' | 'emoji'."""
    if not text:
        return []
    tokens: List[Tuple[str, str]] = []
    pos = 0
    for m in _EMOJI_PATTERN.finditer(text):
        if m.start() > pos:
            tokens.append(("text", text[pos:m.start()]))
        tokens.append(("emoji", m.group(0)))
        pos = m.end()
    if pos < len(text):
        tokens.append(("text", text[pos:]))
    return tokens


# ---------------------------------------------------------------------------
# Rendu d'un emoji unique en image RGBA (taille cible donnée)
# ---------------------------------------------------------------------------
def _render_emoji_to_image(emoji_str: str, target_size: int) -> Optional[Image.Image]:
    """
    Dessine un emoji avec Apple Color Emoji font sur une image transparente,
    puis resize à target_size.
    """
    if not is_apple_emoji_available():
        return None

    # Apple Color Emoji a des tailles SBIX fixes. La bonne taille varie selon
    # la version : Apple original = 20/32/40/48/64/96/160, apple-emoji-linux = 96/137/160
    # On essaie dans l'ordre jusqu'à ce qu'une marche.
    emoji_font = None
    used_size = None
    for try_size in [160, 137, 96, 64, 48, 40, 32, 20]:
        try:
            emoji_font = ImageFont.truetype(str(APPLE_EMOJI_FONT), try_size)
            used_size = try_size
            break
        except Exception:
            continue
    if emoji_font is None:
        logger.warning("Apple emoji font: aucune taille SBIX ne fonctionne")
        return None

    # Crée une image carrée à la taille du SBIX trouvé + un peu de marge
    canvas_size = used_size + 20
    img = Image.new("RGBA", (canvas_size, canvas_size), (0, 0, 0, 0))
    draw = ImageDraw.Draw(img)
    try:
        # embedded_color=True permet à Pillow de rendre les glyphes color SBIX
        draw.text((10, 10), emoji_str, font=emoji_font, embedded_color=True)
    except Exception as e:
        logger.warning(f"Emoji draw failed for {repr(emoji_str)}: {e}")
        return None

    # Crop sur le contenu réel (bbox de la zone non-transparente)
    bbox = img.getbbox()
    if bbox:
        img = img.crop(bbox)
    else:
        # Rien dessiné → emoji pas dans la font
        return None

    # Resize à la taille cible (LANCZOS = haute qualité)
    if img.size != (target_size, target_size):
        ratio = target_size / max(img.size)
        new_w = max(1, int(img.size[0] * ratio))
        new_h = max(1, int(img.size[1] * ratio))
        img = img.resize((new_w, new_h), Image.LANCZOS)

    return img


# ---------------------------------------------------------------------------
# Rendu principal de la caption
# ---------------------------------------------------------------------------
def render_caption_png(
    text: str,
    font_size: int = 56,
    max_width: int = 980,
    out_path: Optional[Path] = None,
    box_alpha: int = DEFAULT_BOX_ALPHA,
    box_radius: int = DEFAULT_BOX_RADIUS,
    pad_x: int = DEFAULT_BOX_PAD_X,
    pad_y: int = DEFAULT_BOX_PAD_Y,
    line_spacing: int = DEFAULT_LINE_SPACING,
) -> Path:
    """Rend une caption en PNG RGBA, retourne le path."""
    text_font_path = _get_text_font_path()
    text_font = ImageFont.truetype(text_font_path, font_size)
    emoji_size = int(font_size * 1.15)  # un peu plus gros que le texte pour matcher

    # ---------- Étape 1 : tokenize + word-wrap ----------
    tmp = Image.new("RGBA", (1, 1))
    measure_draw = ImageDraw.Draw(tmp)

    def text_width(s: str) -> int:
        bbox = measure_draw.textbbox((0, 0), s, font=text_font)
        return bbox[2] - bbox[0]

    def token_width(kind: str, value: str) -> int:
        if kind == "emoji":
            return emoji_size
        return text_width(value)

    raw_lines = (text or "").split("\n")
    layout_lines: List[List[Tuple[str, str]]] = []

    for raw_line in raw_lines:
        tokens = _tokenize(raw_line)
        # Expand "text" tokens en mots pour permettre le word-wrap
        expanded: List[Tuple[str, str]] = []
        for kind, val in tokens:
            if kind == "text":
                # Split en mots (en gardant les espaces)
                words = re.split(r"(\s+)", val)
                for w in words:
                    if w:
                        expanded.append(("text", w))
            else:
                expanded.append((kind, val))

        current: List[Tuple[str, str]] = []
        current_w = 0
        for kind, val in expanded:
            w = token_width(kind, val)
            if current and current_w + w > max_width:
                layout_lines.append(current)
                current = []
                current_w = 0
                # skip whitespace en début de nouvelle ligne
                if kind == "text" and val.strip() == "":
                    continue
            current.append((kind, val))
            current_w += w
        if current:
            layout_lines.append(current)

    if not layout_lines:
        out = out_path or Path("/tmp/clipfusion/output/_caption_empty.png")
        out.parent.mkdir(parents=True, exist_ok=True)
        Image.new("RGBA", (1, 1), (0, 0, 0, 0)).save(out)
        return out

    # ---------- Étape 2 : dimensions de chaque ligne ----------
    line_height = max(font_size + 8, emoji_size + 4)
    line_metrics = []
    for line in layout_lines:
        lw = sum(token_width(k, v) for k, v in line)
        line_metrics.append((lw, line))

    max_line_w = max(lw for lw, _ in line_metrics)
    total_w = max_line_w + 2 * pad_x
    total_h = (
        len(line_metrics) * line_height
        + 2 * pad_y
        + (len(line_metrics) - 1) * line_spacing
    )

    # ---------- Étape 3 : crée canvas + dessine box noire ----------
    img = Image.new("RGBA", (total_w, total_h), (0, 0, 0, 0))
    draw = ImageDraw.Draw(img, "RGBA")

    draw.rounded_rectangle(
        (0, 0, total_w, total_h),
        radius=box_radius,
        fill=(0, 0, 0, box_alpha),
    )

    # ---------- Étape 4 : dessine chaque ligne ----------
    y = pad_y
    for lw, line in line_metrics:
        x = (total_w - lw) // 2
        for kind, val in line:
            if kind == "emoji":
                emoji_img = _render_emoji_to_image(val, emoji_size)
                if emoji_img:
                    # Center vertical sur la ligne
                    ey = y + (line_height - emoji_img.size[1]) // 2
                    img.paste(emoji_img, (x, ey), emoji_img)
                    x += emoji_img.size[0]
                else:
                    # Fallback texte si emoji rendering fail
                    draw.text((x, y), val, font=text_font, fill=(255, 255, 255, 255))
                    x += emoji_size
            else:
                # Texte blanc
                draw.text((x, y), val, font=text_font, fill=(255, 255, 255, 255))
                x += text_width(val)
        y += line_height + line_spacing

    # ---------- Étape 5 : sauvegarde ----------
    if out_path is None:
        import random as _r
        out_path = Path("/tmp/clipfusion/output") / f"_caption_{_r.randint(100000, 999999)}.png"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    img.save(out_path, "PNG")
    return out_path
