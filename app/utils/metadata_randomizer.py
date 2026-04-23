"""
Générateur de métadonnées simulant un vrai iPhone.

Chaque appel produit une signature COMPLÈTEMENT UNIQUE :
- Modèle iPhone random (avec version iOS cohérente)
- Géoloc random (150+ coordonnées de villes monde)
- Dates randomisées avec léger décalage entre format et streams
- Encoder string random (version iOS différente à chaque fois)
- Tous les tags Apple QuickTime complets
- Make/model/software cohérents entre eux
"""
import random
import uuid
from datetime import datetime, timedelta
from typing import Dict


# ---------------------------------------------------------------------------
# Catalogue iPhone + iOS (cohérent)
# ---------------------------------------------------------------------------
# Chaque entrée : (modèle, versions iOS compatibles, firmware version number)
IPHONE_MODELS = [
    # iPhone 15 family (iOS 17.x - 18.x)
    ("iPhone 15 Pro Max", ["17.5.1", "17.6", "17.6.1", "18.0", "18.0.1", "18.1", "18.1.1", "18.2"]),
    ("iPhone 15 Pro",     ["17.5.1", "17.6", "17.6.1", "18.0", "18.0.1", "18.1", "18.1.1", "18.2"]),
    ("iPhone 15 Plus",    ["17.5.1", "17.6", "17.6.1", "18.0", "18.0.1", "18.1", "18.1.1", "18.2"]),
    ("iPhone 15",         ["17.5.1", "17.6", "17.6.1", "18.0", "18.0.1", "18.1", "18.1.1", "18.2"]),
    # iPhone 14 family
    ("iPhone 14 Pro Max", ["16.7.8", "17.5.1", "17.6", "17.6.1", "18.0", "18.1"]),
    ("iPhone 14 Pro",     ["16.7.8", "17.5.1", "17.6", "17.6.1", "18.0", "18.1"]),
    ("iPhone 14 Plus",    ["16.7.8", "17.5.1", "17.6", "17.6.1", "18.0", "18.1"]),
    ("iPhone 14",         ["16.7.8", "17.5.1", "17.6", "17.6.1", "18.0", "18.1"]),
    # iPhone 13 family
    ("iPhone 13 Pro Max", ["16.7.8", "17.5.1", "17.6", "17.6.1", "18.0"]),
    ("iPhone 13 Pro",     ["16.7.8", "17.5.1", "17.6", "17.6.1", "18.0"]),
    ("iPhone 13",         ["16.7.8", "17.5.1", "17.6", "17.6.1", "18.0"]),
    ("iPhone 13 mini",    ["16.7.8", "17.5.1", "17.6", "17.6.1"]),
    # iPhone 12 family
    ("iPhone 12 Pro Max", ["16.7.8", "17.5.1", "17.6", "17.6.1"]),
    ("iPhone 12 Pro",     ["16.7.8", "17.5.1", "17.6", "17.6.1"]),
    ("iPhone 12",         ["16.7.8", "17.5.1", "17.6", "17.6.1"]),
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
# Géolocalisation : 150+ villes dans le monde
# (les coords seront jittered pour que chaque vidéo ait une loc unique)
# ---------------------------------------------------------------------------
CITY_COORDS = [
    # USA (beaucoup pour crédibilité Insta/TikTok US)
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
    """
    Format ISO 6709 attendu par les containers Apple MP4.
    Exemple Apple : +37.7749-122.4194+010.500/
    """
    return f"{lat:+08.4f}{lng:+09.4f}{alt:+07.3f}/"


def _random_datetime() -> datetime:
    """Date random dans les 365 derniers jours (pas trop récente, pas trop vieille)."""
    days_ago = random.randint(3, 365)
    seconds_ago = random.randint(0, 86400)
    return datetime.utcnow() - timedelta(days=days_ago, seconds=seconds_ago)


# ---------------------------------------------------------------------------
# Fonction principale
# ---------------------------------------------------------------------------
def random_metadata() -> Dict[str, str]:
    """
    Retourne un dict de métadonnées Apple COMPLET et UNIQUE.

    Chaque appel génère :
    - Un modèle iPhone random
    - Une version iOS cohérente avec ce modèle
    - Une géoloc random (parmi 100+ villes) avec jitter
    - Une date random dans les 365 derniers jours
    - Des identifiants uniques (UUID)
    - Des versions d'encoder random
    - Un décalage de 1-2ms entre format / streams (comme iPhone réel)
    """
    # Modèle iPhone + version iOS cohérente
    model, ios_versions = random.choice(IPHONE_MODELS)
    ios_version = random.choice(ios_versions)

    # Géoloc avec petit jitter (la vidéo n'est pas pile au centre-ville)
    base_lat, base_lng = random.choice(CITY_COORDS)
    lat = base_lat + random.uniform(-0.05, 0.05)
    lng = base_lng + random.uniform(-0.05, 0.05)
    # Altitude réaliste (0-200m)
    alt = random.uniform(0, 200)
    location_str = _format_iso6709(lat, lng, alt)

    # Date de création (format iPhone)
    base_date = _random_datetime()
    # Format ISO8601 UTC pour le container (comme iPhone)
    format_time = base_date.strftime("%Y-%m-%dT%H:%M:%S.000000Z")
    # Format Apple QuickTime (avec timezone +0000 au lieu de Z)
    apple_creationdate = base_date.strftime("%Y-%m-%dT%H:%M:%S+0000")
    # Les streams ont un léger décalage par rapport au format (typique iPhone)
    stream_offset = timedelta(milliseconds=random.randint(800, 2000))
    stream_time = (base_date + stream_offset).strftime("%Y-%m-%dT%H:%M:%S.000000Z")

    # UUID unique pour identifiant contenu
    content_uuid = _apple_uuid()

    # Versions d'encoder random (change le "fingerprint" du serveur)
    lavf = random.choice(LAVF_VERSIONS)
    lavc = random.choice(LAVC_VERSIONS)

    return {
        # ---- Stripping explicite des tags source ----
        "comment": "",
        "description": "",
        "title": "",
        "artist": "",
        "album": "",

        # ---- Identité de l'appareil (niveau format/container) ----
        "make": "Apple",
        "model": model,
        "com.apple.quicktime.make": "Apple",
        "com.apple.quicktime.model": model,
        "com.apple.quicktime.software": ios_version,

        # ---- Géolocalisation (tous les formats possibles) ----
        "location": location_str,
        "location-eng": location_str,
        "com.apple.quicktime.location.ISO6709": location_str,
        "com.apple.quicktime.location.accuracy.horizontal": f"{random.uniform(4.5, 15.0):.6f}",

        # ---- Dates de création ----
        "creation_time": format_time,
        "date": format_time[:10],   # format YYYY-MM-DD
        "com.apple.quicktime.creationdate": apple_creationdate,

        # ---- Identifiants uniques (UUID) ----
        "com.apple.quicktime.content.identifier": content_uuid,

        # ---- Encoder (fingerprint serveur) — version random ----
        "encoder": f"Lavf{lavf}",

        # ---- Stream-level tags (pour les pistes vidéo/audio) ----
        # Note: ffmpeg applique -metadata aussi aux streams par défaut.
        # Le décalage format/stream se fait naturellement via creation_time.
        "_stream_creation_time": stream_time,   # consommé par le service ffmpeg
        "_stream_encoder_lavc": f"Lavc{lavc} libx264",  # idem
    }


def metadata_to_ffmpeg_args(metadata: Dict[str, str]) -> list:
    """
    Convertit un dict de metadata en args CLI ffmpeg.
    Les clés privées (préfixe `_`) sont gérées séparément.
    """
    args = []
    for key, value in metadata.items():
        if key.startswith("_"):
            continue  # clés privées consommées ailleurs
        # -metadata s'applique au format (container MP4)
        args += ["-metadata", f"{key}={value}"]

    # Stream-specific (creation_time décalé sur les streams)
    stream_time = metadata.get("_stream_creation_time")
    if stream_time:
        args += [
            "-metadata:s:v:0", f"creation_time={stream_time}",
            "-metadata:s:a:0", f"creation_time={stream_time}",
        ]

    # Stream-specific (encoder) – seulement sur le stream vidéo
    stream_enc = metadata.get("_stream_encoder_lavc")
    if stream_enc:
        args += ["-metadata:s:v:0", f"encoder={stream_enc}"]

    return args
