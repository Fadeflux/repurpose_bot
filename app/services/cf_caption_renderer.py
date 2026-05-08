"""
Rendu pixel-parfait des captions style Insta/TikTok via Pillow.

Génère une image RGBA transparente avec :
  - Texte Inter Bold blanc
  - Background noir semi-transparent arrondi (style TikTok box)
  - Emojis remplacés par leurs PNGs Apple (téléchargés dans /opt/apple-emoji)

Utilisé en overlay FFmpeg : moteur > drawtext/subtitles, contrôle pixel-précis,
emojis colorés natifs.
"""
import os
import re
from pathlib import Path
from typing import List, Optional, Tuple

from PIL import Image, ImageDraw, ImageFont

from app.utils.logger import get_logger

logger = get_logger("cf_caption_renderer")

# Dossier des emojis Apple installés par le Dockerfile
APPLE_EMOJI_DIR = Path("/opt/apple-emoji")

# Polices candidate (Inter Bold = Insta-look)
FONT_CANDIDATES = [
    "/usr/share/fonts/truetype/inter/Inter-Bold.ttf",
    "/usr/share/fonts/opentype/inter/Inter-Bold.otf",
    "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf",
    "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
]

# Style box TikTok/Insta
DEFAULT_BOX_ALPHA = 180        # ~70% noir (255 = opaque)
DEFAULT_BOX_RADIUS = 8         # rayon coins arrondis
DEFAULT_BOX_PAD_X = 18         # padding horizontal interne
DEFAULT_BOX_PAD_Y = 10         # padding vertical interne
DEFAULT_LINE_SPACING = 6


def _get_font_path() -> str:
    """Trouve la première police dispo dans la liste."""
    for c in FONT_CANDIDATES:
        if os.path.exists(c):
            return c
    raise RuntimeError("Aucune police trouvée pour le rendu caption")


# ---------------------------------------------------------------------------
# Détection des emojis dans le texte
# ---------------------------------------------------------------------------
# Plages Unicode emoji courantes
_EMOJI_PATTERN = re.compile(
    "["
    "\U0001F600-\U0001F64F"  # emoticons
    "\U0001F300-\U0001F5FF"  # symbols & pictographs
    "\U0001F680-\U0001F6FF"  # transport & map
    "\U0001F1E0-\U0001F1FF"  # flags
    "\U00002500-\U00002BEF"  # chinese
    "\U00002702-\U000027B0"  # dingbats
    "\U000024C2-\U0001F251"
    "\U0001f926-\U0001f937"
    "\U00010000-\U0010FFFF"
    "\u2640-\u2642"
    "\u2600-\u2B55"
    "\u200d"                 # zero-width joiner
    "\u23cf"
    "\u23e9"
    "\u231a"
    "\ufe0f"                 # variation selector
    "\u3030"
    "]+",
    flags=re.UNICODE,
)


def _emoji_to_codepoints(emoji: str) -> str:
    """Convertit un emoji en string codepoints style apple-emoji-linux.
    Ex: '❤️' (U+2764 U+FE0F) → '2764-fe0f'
    Ex: '😀' (U+1F600)        → '1f600'
    """
    parts = []
    for ch in emoji:
        code = ord(ch)
        # Skip variation selectors quand on cherche le PNG (Apple les ignore en général)
        if code == 0xFE0F:
            continue
        parts.append(f"{code:x}")
    return "-".join(parts)


def _find_emoji_png(emoji: str) -> Optional[Path]:
    """Cherche le PNG correspondant à un emoji dans /opt/apple-emoji/."""
    if not APPLE_EMOJI_DIR.exists():
        return None

    # Tente plusieurs variantes du nom (avec/sans variation selector)
    candidates = []
    full_codepoints = _emoji_to_codepoints(emoji)
    if full_codepoints:
        candidates.append(full_codepoints)
        # Aussi : codepoint principal seul
        first = emoji[0]
        candidates.append(_emoji_to_codepoints(first))

    for codepoints in candidates:
        if not codepoints:
            continue
        # Plusieurs naming conventions possibles selon le repo source
        for variant in [
            f"{codepoints}.png",
            f"emoji_u{codepoints.replace('-', '_')}.png",
            f"u{codepoints}.png",
        ]:
            p = APPLE_EMOJI_DIR / variant
            if p.exists():
                return p
    return None


# ---------------------------------------------------------------------------
# Tokenization : sépare le texte en (text_chunks, emoji_chunks)
# ---------------------------------------------------------------------------
def _tokenize(text: str) -> List[Tuple[str, str]]:
    """
    Coupe le texte en tokens (kind, value) :
    - ("text", "Hello ")
    - ("emoji", "😀")
    - ("text", " world")
    """
    if not text:
        return []
    tokens: List[Tuple[str, str]] = []
    pos = 0
    for m in _EMOJI_PATTERN.finditer(text):
        if m.start() > pos:
            tokens.append(("text", text[pos:m.start()]))
        emoji_chunk = m.group(0)
        # Sépare les emojis composés en emojis individuels (1 par 1)
        # Note : ZWJ sequences (👨‍👩‍👧) restent groupées par le PATTERN, on les laisse tel quel
        tokens.append(("emoji", emoji_chunk))
        pos = m.end()
    if pos < len(text):
        tokens.append(("text", text[pos:]))
    return tokens


# ---------------------------------------------------------------------------
# Rendu principal
# ---------------------------------------------------------------------------
def render_caption_png(
    text: str,
    font_size: int = 56,
    max_width: int = 980,                       # max width avant wrap (px)
    out_path: Optional[Path] = None,
    box_alpha: int = DEFAULT_BOX_ALPHA,
    box_radius: int = DEFAULT_BOX_RADIUS,
    pad_x: int = DEFAULT_BOX_PAD_X,
    pad_y: int = DEFAULT_BOX_PAD_Y,
    line_spacing: int = DEFAULT_LINE_SPACING,
) -> Path:
    """
    Rend une caption en PNG RGBA.

    Strategy : layout simple ligne par ligne, avec wrap au mot quand la ligne
    dépasse max_width. Chaque ligne a sa propre box noire (style TikTok).

    Retourne le path du PNG généré.
    """
    font_path = _get_font_path()
    font = ImageFont.truetype(font_path, font_size)

    # Hauteur emoji = hauteur ligne (un peu plus grand pour matcher le baseline)
    emoji_size = int(font_size * 1.1)

    # Étape 1 : tokenize + word-wrap
    # On split d'abord par lignes explicites (\n), puis on fait du wrap au mot
    raw_lines = (text or "").split("\n")
    layout_lines: List[List[Tuple[str, str]]] = []  # liste de lignes, chaque ligne = liste de tokens

    # Image temp pour mesurer textes
    tmp = Image.new("RGBA", (1, 1))
    draw = ImageDraw.Draw(tmp)

    def measure_token(kind: str, value: str) -> int:
        if kind == "emoji":
            return emoji_size
        bbox = draw.textbbox((0, 0), value, font=font)
        return bbox[2] - bbox[0]

    for raw_line in raw_lines:
        tokens = _tokenize(raw_line)
        # Word-wrap : on accumule les tokens, on coupe au mot avant overflow
        current_line: List[Tuple[str, str]] = []
        current_w = 0
        # On split les tokens "text" en mots pour pouvoir wrapper
        expanded: List[Tuple[str, str]] = []
        for kind, val in tokens:
            if kind == "text":
                # On garde les espaces dans les mots qui les précèdent (pour le rendu)
                words = re.split(r"(\s+)", val)
                for w in words:
                    if w:
                        expanded.append(("text", w))
            else:
                expanded.append((kind, val))

        for kind, val in expanded:
            w = measure_token(kind, val)
            # Si ajouter ce token dépasse la largeur ET qu'on a déjà du contenu : nouvelle ligne
            if current_line and current_w + w > max_width:
                layout_lines.append(current_line)
                current_line = []
                current_w = 0
                # skip pure-whitespace au début de nouvelle ligne
                if kind == "text" and val.strip() == "":
                    continue
            current_line.append((kind, val))
            current_w += w

        if current_line:
            layout_lines.append(current_line)

    # Si aucune ligne (texte vide), on génère une PNG 1x1 transparente
    if not layout_lines:
        out = out_path or Path("/tmp/clipfusion/output/_caption_empty.png")
        out.parent.mkdir(parents=True, exist_ok=True)
        Image.new("RGBA", (1, 1), (0, 0, 0, 0)).save(out)
        return out

    # Étape 2 : calcule les dimensions de chaque ligne (largeur réelle + hauteur uniforme)
    line_height = font_size + 4   # un peu de padding interne
    line_metrics = []  # (line_width, line_tokens)
    for line in layout_lines:
        lw = sum(measure_token(k, v) for k, v in line)
        line_metrics.append((lw, line))

    # Étape 3 : dimensions canvas final
    max_line_w = max(lw for lw, _ in line_metrics)
    total_w = max_line_w + 2 * pad_x
    total_h = len(line_metrics) * line_height + 2 * pad_y + (len(line_metrics) - 1) * line_spacing

    # Étape 4 : création canvas + dessin
    img = Image.new("RGBA", (total_w, total_h), (0, 0, 0, 0))
    draw = ImageDraw.Draw(img, "RGBA")

    # Une seule grosse box derrière TOUTES les lignes (style "block")
    draw.rounded_rectangle(
        (0, 0, total_w, total_h),
        radius=box_radius,
        fill=(0, 0, 0, box_alpha),
    )

    # Étape 5 : dessin des lignes (centrées horizontalement)
    y = pad_y
    for lw, line in line_metrics:
        x = (total_w - lw) // 2
        # Dessine chaque token sur la ligne
        for kind, val in line:
            if kind == "emoji":
                emoji_png = _find_emoji_png(val)
                if emoji_png:
                    try:
                        emoji_img = Image.open(emoji_png).convert("RGBA")
                        emoji_img = emoji_img.resize((emoji_size, emoji_size), Image.LANCZOS)
                        # Vertical center of line
                        ey = y + (line_height - emoji_size) // 2
                        img.paste(emoji_img, (x, ey), emoji_img)
                    except Exception as e:
                        logger.warning(f"emoji paste failed for '{val}': {e}")
                        # Fallback : draw text
                        draw.text((x, y), val, font=font, fill=(255, 255, 255, 255))
                else:
                    # Pas de PNG trouvé pour cet emoji → tente fallback texte
                    logger.warning(f"emoji PNG not found for {repr(val)} (codepoints={_emoji_to_codepoints(val)})")
                    draw.text((x, y), val, font=font, fill=(255, 255, 255, 255))
                x += emoji_size
            else:
                draw.text((x, y), val, font=font, fill=(255, 255, 255, 255))
                bbox = draw.textbbox((0, 0), val, font=font)
                x += bbox[2] - bbox[0]
        y += line_height + line_spacing

    # Sauvegarde
    if out_path is None:
        import random as _r
        out_path = Path("/tmp/clipfusion/output") / f"_caption_{_r.randint(100000, 999999)}.png"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    img.save(out_path, "PNG")
    return out_path


def is_apple_emoji_available() -> bool:
    """Check si les PNGs Apple sont bien installés."""
    if not APPLE_EMOJI_DIR.exists():
        return False
    # Au moins quelques fichiers
    return len(list(APPLE_EMOJI_DIR.glob("*.png"))) > 50
