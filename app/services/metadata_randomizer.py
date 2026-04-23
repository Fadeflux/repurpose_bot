"""
Génère des métadonnées aléatoires crédibles pour brouiller les pistes.

Supprime les tags identifiants TikTok (comment: vid:...) et ajoute des
métadonnées plausibles (géolocalisation, modèle d'appareil, date...).
"""
import random
from datetime import datetime, timedelta


# Modèles d'appareils plausibles (récents, majoritairement utilisés)
DEVICE_MODELS = [
    "iPhone 15 Pro", "iPhone 15", "iPhone 14 Pro", "iPhone 14",
    "iPhone 13 Pro", "iPhone 13",
    "SM-S928B",    # Galaxy S24 Ultra
    "SM-S918B",    # Galaxy S23 Ultra
    "SM-S911B",    # Galaxy S23
    "Pixel 8 Pro", "Pixel 8", "Pixel 7",
]

# Coordonnées approximatives de grandes villes (pour la géoloc random)
CITY_COORDS = [
    ("Paris, FR",       48.8566,   2.3522),
    ("London, UK",      51.5074,  -0.1278),
    ("New York, US",    40.7128, -74.0060),
    ("Los Angeles, US", 34.0522, -118.2437),
    ("Miami, US",       25.7617, -80.1918),
    ("Berlin, DE",      52.5200,  13.4050),
    ("Madrid, ES",      40.4168,  -3.7038),
    ("Rome, IT",        41.9028,  12.4964),
    ("Amsterdam, NL",   52.3676,   4.9041),
    ("Barcelona, ES",   41.3851,   2.1734),
    ("Dubai, AE",       25.2048,  55.2708),
    ("Sydney, AU",     -33.8688, 151.2093),
]


def random_metadata() -> dict:
    """
    Retourne un dict de métadonnées aléatoires à injecter via ffmpeg -metadata.
    """
    # Géoloc random avec petit jitter
    _, base_lat, base_lng = random.choice(CITY_COORDS)
    lat = base_lat + random.uniform(-0.1, 0.1)
    lng = base_lng + random.uniform(-0.1, 0.1)
    # Format ISO 6709 attendu par les containers MP4
    location_str = f"{lat:+.4f}{lng:+.4f}/"

    # Date random dans les 180 derniers jours
    days_ago = random.randint(1, 180)
    secs_ago = random.randint(0, 86400)
    random_date = datetime.utcnow() - timedelta(days=days_ago, seconds=secs_ago)
    creation_time = random_date.strftime("%Y-%m-%dT%H:%M:%S.000000Z")

    device = random.choice(DEVICE_MODELS)

    return {
        "comment": "",                   # vide le comment TikTok
        "description": "",               # vide aussi
        "title": "",                     # vide le titre
        "encoder": "",                   # vide l'encoder string
        "location": location_str,
        "location-eng": location_str,
        "com.apple.quicktime.location.ISO6709": location_str,
        "com.apple.quicktime.make": "Apple" if "iPhone" in device else "samsung" if "SM-" in device else "Google",
        "com.apple.quicktime.model": device,
        "com.apple.quicktime.software": "17.5.1" if "iPhone" in device else "14",
        "creation_time": creation_time,
        "make": "Apple" if "iPhone" in device else "samsung" if "SM-" in device else "Google",
        "model": device,
    }


def metadata_to_ffmpeg_args(metadata: dict) -> list:
    """Convertit un dict de metadata en args CLI ffmpeg."""
    args = []
    for key, value in metadata.items():
        args += ["-metadata", f"{key}={value}"]
    return args
