"""
Générateur de métadonnées simulant un vrai iPhone OU un vrai Android.

Chaque appel produit une signature COMPLÈTEMENT UNIQUE :
- 50% iPhone / 50% Android (random à chaque vidéo)
- Modèle + OS cohérent avec la marque
- Géoloc random (100+ coordonnées de villes monde) avec jitter
- Dates randomisées avec décalage format/streams
- UUID + encoder version uniques
- Tags spécifiques à la plateforme (QuickTime pour Apple, Android tags sinon)
"""
import random
import uuid
from datetime import datetime, timedelta
from typing import Dict


# ---------------------------------------------------------------------------
# Catalogue iPhone 16/17 avec iOS cohérent (version avril 2026)
# ---------------------------------------------------------------------------
IPHONE_MODELS = [
    # iPhone 17 family (sortie septembre 2025, iOS 26)
    ("iPhone 17 Pro Max", ["26.0", "26.0.1", "26.1", "26.1.1", "26.2", "26.3", "26.3.1", "26.4", "26.4.1"]),
    ("iPhone 17 Pro",     ["26.0", "26.0.1", "26.1", "26.1.1", "26.2", "26.3", "26.3.1", "26.4", "26.4.1"]),
    ("iPhone 17 Air",     ["26.0", "26.0.1", "26.1", "26.1.1", "26.2", "26.3", "26.3.1", "26.4", "26.4.1"]),
    ("iPhone 17",         ["26.0", "26.0.1", "26.1", "26.1.1", "26.2", "26.3", "26.3.1", "26.4", "26.4.1"]),
    # iPhone 16 family (sortie septembre 2024, iOS 18 et 26)
    ("iPhone 16 Pro Max", ["18.0", "18.0.1", "18.1", "18.2", "18.3", "18.4", "26.0", "26.1", "26.2", "26.3", "26.4", "26.4.1"]),
    ("iPhone 16 Pro",     ["18.0", "18.0.1", "18.1", "18.2", "18.3", "18.4", "26.0", "26.1", "26.2", "26.3", "26.4", "26.4.1"]),
    ("iPhone 16 Plus",    ["18.0", "18.0.1", "18.1", "18.2", "18.3", "18.4", "26.0", "26.1", "26.2", "26.3", "26.4", "26.4.1"]),
    ("iPhone 16",         ["18.0", "18.0.1", "18.1", "18.2", "18.3", "18.4", "26.0", "26.1", "26.2", "26.3", "26.4", "26.4.1"]),
    ("iPhone 16e",        ["18.3", "18.4", "26.0", "26.1", "26.2", "26.3", "26.4", "26.4.1"]),
]


# ---------------------------------------------------------------------------
# Catalogue Android haut de gamme cohérent (avril 2026)
# Format : (make, model_code, marketing_name, Android versions)
# ---------------------------------------------------------------------------
ANDROID_MODELS = [
    # Samsung Galaxy S25 family (sortie janvier 2025, Android 15/16)
    ("samsung", "SM-S938B", "Galaxy S25 Ultra",  ["15", "16"]),
    ("samsung", "SM-S936B", "Galaxy S25+",       ["15", "16"]),
    ("samsung", "SM-S931B", "Galaxy S25",        ["15", "16"]),
    # Samsung Galaxy S24 family (sortie janvier 2024, Android 14/15/16)
    ("samsung", "SM-S928B", "Galaxy S24 Ultra",  ["14", "15", "16"]),
    ("samsung", "SM-S926B", "Galaxy S24+",       ["14", "15", "16"]),
    ("samsung", "SM-S921B", "Galaxy S24",        ["14", "15", "16"]),
    # Samsung Galaxy S23 family (sortie 2023, Android 13/14/15)
    ("samsung", "SM-S918B", "Galaxy S23 Ultra",  ["13", "14", "15"]),
    ("samsung", "SM-S916B", "Galaxy S23+",       ["13", "14", "15"]),
    ("samsung", "SM-S911B", "Galaxy S23",        ["13", "14", "15"]),
    # Google Pixel 9 family (sortie août 2024, Android 14/15/16)
    ("Google",  "Pixel 9 Pro XL", "Pixel 9 Pro XL", ["14", "15", "16"]),
    ("Google",  "Pixel 9 Pro",    "Pixel 9 Pro",    ["14", "15", "16"]),
    ("Google",  "Pixel 9",        "Pixel 9",        ["14", "15", "16"]),
    # Google Pixel 8 family (sortie octobre 2023, Android 14/15)
    ("Google",  "Pixel 8 Pro",    "Pixel 8 Pro",    ["14", "15"]),
    ("Google",  "Pixel 8",        "Pixel 8",        ["14", "15"]),
    # Xiaomi 15 family (sortie 2024/2025)
    ("Xiaomi",  "24129PN74G",     "Xiaomi 15 Ultra", ["14", "15"]),
    ("Xiaomi",  "2410FPN6DG",     "Xiaomi 15 Pro",   ["14", "15"]),
    ("Xiaomi",  "2410FPN6DC",     "Xiaomi 15",       ["14", "15"]),
]


# Versions d'encoder Lavf / Lavc (vraies versions qui ont existé)
LAVF_VERSIONS = [
    "60.3.100", "60.16.100", "60.22.101",
    "61.1.100", "61.7.100", "61.19.101",
    "62.1.101", "62.12.100", "62.28.100",
]
LAVC_VERSIONS = [
    "60.3.100", "60.16.102", "60.31.102",
    "61.1.100", "61.5.103", "61.19.101",
    "62.1.101", "62.4.100", "62.28.100",
]

# ---------------------------------------------------------------------------
# Géolocalisation : 100+ villes dans le monde
# ---------------------------------------------------------------------------
CITY_COORDS = [
    # USA
    (40.7128, -74.0060),  (34.0522, -118.2437), (41.8781, -87.6298),
    (29.7604, -95.3698),  (33.4484, -112.0740), (39.9526,  -75.1652),
    (29.4241, -98.4936),  (32.7157, -117.1611), (32.7767,  -96.7970),
    (37.3382, -121.8863), (30.2672, -97.7431),  (37.7749, -122.4194),
    (39.7392, -104.9903), (35.2271, -80.8431),  (42.3601,  -71.0589),
    (47.6062, -122.3321), (42.3314,  -83.0458), (39.2904,  -76.6122),
    (36.1627, -86.7816),  (25.7617,  -80.1918), (45.5152, -122.6784),
    (36.1699, -115.1398), (27.9506,  -82.4572), (38.9072,  -77.0369),
    # Canada
    (43.6532, -79.3832),  (45.5017, -73.5673),  (49.2827, -123.1207),
    # UK / Irlande
    (51.5074,  -0.1278),  (53.4808,  -2.2426),  (52.4862, -1.8904),
    (55.9533,  -3.1883),  (53.3498,  -6.2603),  (51.4545,  -2.5879),
    # France
    (48.8566,   2.3522),  (45.7640,   4.8357),  (43.2965,   5.3698),
    (43.6047,   1.4442),  (47.2184,  -1.5536),  (43.7102,   7.2620),
    (50.6292,   3.0573),  (48.5734,   7.7521),  (44.8378,  -0.5792),
    # Espagne / Portugal
    (40.4168,  -3.7038),  (41.3851,   2.1734),  (37.3891,  -5.9845),
    (39.4699,  -0.3763),  (43.2627,  -2.9253),  (38.7223,  -9.1393),
    (41.1579,  -8.6291),
    # Italie
    (41.9028,  12.4964),  (45.4642,   9.1900),  (40.8518,  14.2681),
    (45.4408,  12.3155),  (43.7696,  11.2558),
    # Allemagne
    (52.5200,  13.4050),  (48.1351,  11.5820),  (50.1109,   8.6821),
    (53.5511,   9.9937),  (50.9375,   6.9603),
    # Pays-Bas / Belgique
    (52.3676,   4.9041),  (51.9244,   4.4777),  (50.8503,   4.3517),
    # Suisse / Autriche
    (47.3769,   8.5417),  (46.2044,   6.1432),  (48.2082,  16.3738),
    # Pays nordiques
    (59.3293,  18.0686),  (55.6761,  12.5683),  (60.1699,  24.9384),
    (59.9139,  10.7522),
    # Europe de l'Est
    (52.2297,  21.0122),  (50.0755,  14.4378),  (47.4979,  19.0402),
    # Moyen-Orient / Dubai
    (25.2048,  55.2708),  (24.4539,  54.3773),  (31.7683,  35.2137),
    # Asie
    (35.6762, 139.6503),  (34.6937, 135.5023),  (37.5665, 126.9780),
    (22.3193, 114.1694),  (1.3521,  103.8198),  (13.7563, 100.5018),
    (3.1390, 101.6869),   (39.9042, 116.4074),  (31.2304, 121.4737),
    # Océanie
    (-33.8688, 151.2093), (-37.8136, 144.9631), (-27.4698, 153.0251),
    (-36.8485, 174.7633),
    # Amérique latine
    (-23.5505, -46.6333), (-34.6037, -58.3816), (19.4326, -99.1332),
    (4.7110,   -74.0721), (-33.4489, -70.6693), (-22.9068, -43.1729),
    (-12.0464, -77.0428),
    # Afrique
    (-33.9249,  18.4241), (30.0444,   31.2357), (6.5244,    3.3792),
    (-1.2921,   36.8219), (33.5731,   -7.5898),
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _apple_uuid() -> str:
    """Génère un UUID unique au format Apple (majuscules, avec tirets)."""
    return str(uuid.uuid4()).upper()


def _format_iso6709(lat: float, lng: float, alt: float = 0.0) -> str:
    """Format ISO 6709 attendu par les containers Apple MP4."""
    return f"{lat:+08.4f}{lng:+09.4f}{alt:+07.3f}/"


def _format_iso6709_short(lat: float, lng: float) -> str:
    """Format court ISO 6709 (Android utilise souvent ce format sans altitude)."""
    return f"{lat:+08.4f}{lng:+09.4f}/"


def _random_datetime() -> datetime:
    """Date random dans les 365 derniers jours."""
    days_ago = random.randint(3, 365)
    seconds_ago = random.randint(0, 86400)
    return datetime.utcnow() - timedelta(days=days_ago, seconds=seconds_ago)


def _random_timings(base_date: datetime) -> Dict[str, str]:
    """Génère les creation_time avec décalage format/video/audio."""
    format_time = base_date.strftime("%Y-%m-%dT%H:%M:%S.000000Z")
    apple_creationdate = base_date.strftime("%Y-%m-%dT%H:%M:%S+0000")
    video_offset = timedelta(seconds=random.randint(1, 3))
    audio_offset_extra = timedelta(seconds=random.randint(1, 2))
    v_stream_time = (base_date + video_offset).strftime("%Y-%m-%dT%H:%M:%S.000000Z")
    a_stream_time = (base_date + video_offset + audio_offset_extra).strftime("%Y-%m-%dT%H:%M:%S.000000Z")
    return {
        "format_time": format_time,
        "apple_creationdate": apple_creationdate,
        "v_stream_time": v_stream_time,
        "a_stream_time": a_stream_time,
    }


# ---------------------------------------------------------------------------
# Générateur iPhone
# ---------------------------------------------------------------------------
def _iphone_metadata() -> Dict[str, str]:
    """Métadonnées complètes pour simuler un iPhone 16/17."""
    model, ios_versions = random.choice(IPHONE_MODELS)
    ios_version = random.choice(ios_versions)

    base_lat, base_lng = random.choice(CITY_COORDS)
    lat = base_lat + random.uniform(-0.05, 0.05)
    lng = base_lng + random.uniform(-0.05, 0.05)
    alt = random.uniform(0, 200)
    location_str = _format_iso6709(lat, lng, alt)

    base_date = _random_datetime()
    timings = _random_timings(base_date)

    content_uuid = _apple_uuid()
    lavf = random.choice(LAVF_VERSIONS)
    lavc = random.choice(LAVC_VERSIONS)

    return {
        # Stripping
        "comment": "", "description": "", "title": "", "artist": "", "album": "",

        # Identité Apple
        "make": "Apple",
        "model": model,
        "com.apple.quicktime.make": "Apple",
        "com.apple.quicktime.model": model,
        "com.apple.quicktime.software": ios_version,

        # Géoloc
        "location": location_str,
        "location-eng": location_str,
        "com.apple.quicktime.location.ISO6709": location_str,
        "com.apple.quicktime.location.accuracy.horizontal": f"{random.uniform(4.5, 15.0):.6f}",

        # Dates
        "creation_time": timings["format_time"],
        "date": timings["format_time"][:10],
        "com.apple.quicktime.creationdate": timings["apple_creationdate"],

        # UUID
        "com.apple.quicktime.content.identifier": content_uuid,

        # Encoder
        "encoder": f"Lavf{lavf}",

        # Clés privées
        "_platform": "iphone",
        "_video_creation_time": timings["v_stream_time"],
        "_audio_creation_time": timings["a_stream_time"],
        "_stream_encoder_lavc": f"Lavc{lavc} libx264",
        "_video_handler_name": "Core Media Video",
        "_audio_handler_name": "Core Media Audio",
    }


# ---------------------------------------------------------------------------
# Générateur Android
# ---------------------------------------------------------------------------
def _android_metadata() -> Dict[str, str]:
    """Métadonnées complètes pour simuler un Android récent (Samsung/Pixel/Xiaomi)."""
    make, model_code, _marketing_name, android_versions = random.choice(ANDROID_MODELS)
    android_version = random.choice(android_versions)

    base_lat, base_lng = random.choice(CITY_COORDS)
    lat = base_lat + random.uniform(-0.05, 0.05)
    lng = base_lng + random.uniform(-0.05, 0.05)
    # Android stocke souvent la loc sans altitude
    location_str = _format_iso6709_short(lat, lng)

    base_date = _random_datetime()
    timings = _random_timings(base_date)

    lavf = random.choice(LAVF_VERSIONS)
    lavc = random.choice(LAVC_VERSIONS)

    # Les téléphones Android enregistrent souvent en 30/60 fps avec com.android.capture.fps
    capture_fps = random.choice([30.0, 60.0])

    return {
        # Stripping
        "comment": "", "description": "", "title": "", "artist": "", "album": "",

        # Identité Android
        "make": make,
        "model": model_code,
        # Android-specific tags
        "com.android.version": android_version,
        "com.android.capture.fps": f"{capture_fps:.6f}",
        "com.android.manufacturer": make,
        "com.android.model": model_code,

        # Géoloc (format court, sans altitude comme Android)
        "location": location_str,
        "location-eng": location_str,

        # Dates
        "creation_time": timings["format_time"],
        "date": timings["format_time"][:10],

        # Encoder
        "encoder": f"Lavf{lavf}",

        # Clés privées
        "_platform": "android",
        "_video_creation_time": timings["v_stream_time"],
        "_audio_creation_time": timings["a_stream_time"],
        "_stream_encoder_lavc": f"Lavc{lavc} libx264",
        "_video_handler_name": "VideoHandle",
        "_audio_handler_name": "SoundHandle",
    }


# ---------------------------------------------------------------------------
# Mapping des choix de device disponibles dans l'UI
# ---------------------------------------------------------------------------
# Clé = valeur envoyée par le frontend
# Valeur = fonction qui retourne les metadata
DEVICE_CHOICES = {
    # Mix et catégories
    "mix_random",           # 50/50 iPhone/Android
    "iphone_random",        # random parmi tous les iPhone 16/17
    "android_random",       # random parmi tous les Android
    "samsung_random",       # random parmi Samsung uniquement
    "pixel_random",         # random parmi Pixel uniquement
    # iPhone spécifiques
    "iphone_17_pro_max", "iphone_17_pro", "iphone_17_air", "iphone_17",
    "iphone_16_pro_max", "iphone_16_pro", "iphone_16_plus", "iphone_16", "iphone_16e",
    # Samsung spécifiques
    "samsung_s25_ultra", "samsung_s25_plus", "samsung_s25",
    "samsung_s24_ultra", "samsung_s24_plus", "samsung_s24",
    "samsung_s23_ultra", "samsung_s23_plus", "samsung_s23",
    # Pixel spécifiques
    "pixel_9_pro_xl", "pixel_9_pro", "pixel_9",
    "pixel_8_pro", "pixel_8",
    # Xiaomi
    "xiaomi_15_ultra", "xiaomi_15_pro", "xiaomi_15",
}

# Mapping des clés → tuples correspondants dans les catalogues
_IPHONE_MAP = {
    "iphone_17_pro_max": "iPhone 17 Pro Max",
    "iphone_17_pro":     "iPhone 17 Pro",
    "iphone_17_air":     "iPhone 17 Air",
    "iphone_17":         "iPhone 17",
    "iphone_16_pro_max": "iPhone 16 Pro Max",
    "iphone_16_pro":     "iPhone 16 Pro",
    "iphone_16_plus":    "iPhone 16 Plus",
    "iphone_16":         "iPhone 16",
    "iphone_16e":        "iPhone 16e",
}

_ANDROID_MAP = {
    "samsung_s25_ultra": "SM-S938B",
    "samsung_s25_plus":  "SM-S936B",
    "samsung_s25":       "SM-S931B",
    "samsung_s24_ultra": "SM-S928B",
    "samsung_s24_plus":  "SM-S926B",
    "samsung_s24":       "SM-S921B",
    "samsung_s23_ultra": "SM-S918B",
    "samsung_s23_plus":  "SM-S916B",
    "samsung_s23":       "SM-S911B",
    "pixel_9_pro_xl":    "Pixel 9 Pro XL",
    "pixel_9_pro":       "Pixel 9 Pro",
    "pixel_9":           "Pixel 9",
    "pixel_8_pro":       "Pixel 8 Pro",
    "pixel_8":           "Pixel 8",
    "xiaomi_15_ultra":   "24129PN74G",
    "xiaomi_15_pro":     "2410FPN6DG",
    "xiaomi_15":         "2410FPN6DC",
}


def _iphone_metadata_fixed(model: str) -> Dict[str, str]:
    """Génère metadata iPhone pour un modèle spécifique (trouve les iOS versions)."""
    for m, ios_versions in IPHONE_MODELS:
        if m == model:
            return _iphone_metadata_impl(m, ios_versions)
    return _iphone_metadata()  # fallback si modèle pas trouvé


def _android_metadata_fixed(model_code: str) -> Dict[str, str]:
    """Génère metadata Android pour un modèle spécifique."""
    for make, mc, name, versions in ANDROID_MODELS:
        if mc == model_code:
            return _android_metadata_impl(make, mc, name, versions)
    return _android_metadata()  # fallback


def _iphone_metadata_impl(model: str, ios_versions: list) -> Dict[str, str]:
    """Version factorisée du generator iPhone."""
    ios_version = random.choice(ios_versions)
    base_lat, base_lng = random.choice(CITY_COORDS)
    lat = base_lat + random.uniform(-0.05, 0.05)
    lng = base_lng + random.uniform(-0.05, 0.05)
    alt = random.uniform(0, 200)
    location_str = _format_iso6709(lat, lng, alt)
    base_date = _random_datetime()
    timings = _random_timings(base_date)
    content_uuid = _apple_uuid()
    lavf = random.choice(LAVF_VERSIONS)
    lavc = random.choice(LAVC_VERSIONS)
    return {
        "comment": "", "description": "", "title": "", "artist": "", "album": "",
        "make": "Apple",
        "model": model,
        "com.apple.quicktime.make": "Apple",
        "com.apple.quicktime.model": model,
        "com.apple.quicktime.software": ios_version,
        "location": location_str,
        "location-eng": location_str,
        "com.apple.quicktime.location.ISO6709": location_str,
        "com.apple.quicktime.location.accuracy.horizontal": f"{random.uniform(4.5, 15.0):.6f}",
        "creation_time": timings["format_time"],
        "date": timings["format_time"][:10],
        "com.apple.quicktime.creationdate": timings["apple_creationdate"],
        "com.apple.quicktime.content.identifier": content_uuid,
        "encoder": f"Lavf{lavf}",
        "_platform": "iphone",
        "_video_creation_time": timings["v_stream_time"],
        "_audio_creation_time": timings["a_stream_time"],
        "_stream_encoder_lavc": f"Lavc{lavc} libx264",
        "_video_handler_name": "Core Media Video",
        "_audio_handler_name": "Core Media Audio",
    }


def _android_metadata_impl(make: str, model_code: str, _name: str, android_versions: list) -> Dict[str, str]:
    """Version factorisée du generator Android."""
    android_version = random.choice(android_versions)
    base_lat, base_lng = random.choice(CITY_COORDS)
    lat = base_lat + random.uniform(-0.05, 0.05)
    lng = base_lng + random.uniform(-0.05, 0.05)
    location_str = _format_iso6709_short(lat, lng)
    base_date = _random_datetime()
    timings = _random_timings(base_date)
    lavf = random.choice(LAVF_VERSIONS)
    lavc = random.choice(LAVC_VERSIONS)
    capture_fps = random.choice([30.0, 60.0])
    return {
        "comment": "", "description": "", "title": "", "artist": "", "album": "",
        "make": make,
        "model": model_code,
        "com.android.version": android_version,
        "com.android.capture.fps": f"{capture_fps:.6f}",
        "com.android.manufacturer": make,
        "com.android.model": model_code,
        "location": location_str,
        "location-eng": location_str,
        "creation_time": timings["format_time"],
        "date": timings["format_time"][:10],
        "encoder": f"Lavf{lavf}",
        "_platform": "android",
        "_video_creation_time": timings["v_stream_time"],
        "_audio_creation_time": timings["a_stream_time"],
        "_stream_encoder_lavc": f"Lavc{lavc} libx264",
        "_video_handler_name": "VideoHandle",
        "_audio_handler_name": "SoundHandle",
    }


# ---------------------------------------------------------------------------
# Fonction principale : supporte un choix de device
# ---------------------------------------------------------------------------
def random_metadata(device_choice: str = "mix_random") -> Dict[str, str]:
    """
    Retourne un dict de métadonnées COMPLET et UNIQUE.
    
    Args:
        device_choice: Le type de device à simuler.
            - "mix_random": 50/50 iPhone/Android (défaut)
            - "iphone_random": random parmi iPhone 16/17
            - "android_random": random parmi tous Android
            - "samsung_random", "pixel_random": random dans la marque
            - "iphone_17_pro_max", "samsung_s24_ultra", etc: modèle spécifique
    """
    # Mix 50/50 (par défaut)
    if device_choice == "mix_random" or device_choice not in DEVICE_CHOICES:
        if random.random() < 0.5:
            return _iphone_metadata()
        return _android_metadata()

    # iPhone aléatoire
    if device_choice == "iphone_random":
        return _iphone_metadata()

    # Android aléatoire (toutes marques)
    if device_choice == "android_random":
        return _android_metadata()

    # Samsung aléatoire
    if device_choice == "samsung_random":
        samsung_models = [m for m in ANDROID_MODELS if m[0] == "samsung"]
        make, code, name, versions = random.choice(samsung_models)
        return _android_metadata_impl(make, code, name, versions)

    # Pixel aléatoire
    if device_choice == "pixel_random":
        pixel_models = [m for m in ANDROID_MODELS if m[0] == "Google"]
        make, code, name, versions = random.choice(pixel_models)
        return _android_metadata_impl(make, code, name, versions)

    # iPhone spécifique
    if device_choice in _IPHONE_MAP:
        return _iphone_metadata_fixed(_IPHONE_MAP[device_choice])

    # Android spécifique
    if device_choice in _ANDROID_MAP:
        return _android_metadata_fixed(_ANDROID_MAP[device_choice])

    # Fallback
    return _iphone_metadata() if random.random() < 0.5 else _android_metadata()


def metadata_to_ffmpeg_args(metadata: Dict[str, str]) -> list:
    """
    Convertit un dict de metadata en args CLI ffmpeg.
    Les clés privées (préfixe `_`) sont gérées séparément.
    """
    args = []
    for key, value in metadata.items():
        if key.startswith("_"):
            continue
        args += ["-metadata", f"{key}={value}"]

    # Stream vidéo
    v_time = metadata.get("_video_creation_time")
    v_handler = metadata.get("_video_handler_name")
    v_encoder = metadata.get("_stream_encoder_lavc")
    if v_time:
        args += ["-metadata:s:v:0", f"creation_time={v_time}"]
    if v_handler:
        args += ["-metadata:s:v:0", f"handler_name={v_handler}"]
    if v_encoder:
        args += ["-metadata:s:v:0", f"encoder={v_encoder}"]

    # Stream audio
    a_time = metadata.get("_audio_creation_time")
    a_handler = metadata.get("_audio_handler_name")
    if a_time:
        args += ["-metadata:s:a:0", f"creation_time={a_time}"]
    if a_handler:
        args += ["-metadata:s:a:0", f"handler_name={a_handler}"]

    return args
