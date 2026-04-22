"""Génère un jeu de paramètres aléatoires dans les bornes définies (ou custom)."""
import random
from typing import Dict, List, Optional, Tuple
from app.config import PARAM_RANGES


INT_KEYS = {"framerate", "bitrate", "noise"}


def random_params(
    custom_ranges: Optional[Dict[str, Tuple[float, float]]] = None,
    enabled_filters: Optional[List[str]] = None,
) -> Dict[str, Optional[float]]:
    """
    Retourne un dict avec une valeur aléatoire entre min et max pour chaque paramètre.

    - custom_ranges : surcharge les bornes par défaut. Ex: {"speed": (1.02, 1.05)}
    - enabled_filters : liste des filtres activés. Les autres auront la valeur None
      (le service ffmpeg doit alors zapper le filtre correspondant).

    Si enabled_filters est None, tous les filtres sont actifs.
    """
    out: Dict[str, Optional[float]] = {}

    for key, default_range in PARAM_RANGES.items():
        # Filtre désactivé -> on ne randomise pas
        if enabled_filters is not None and key not in enabled_filters:
            out[key] = None
            continue

        lo, hi = default_range
        if custom_ranges and key in custom_ranges:
            lo, hi = custom_ranges[key]
            # Sécurité : on s'assure que min <= max
            if lo > hi:
                lo, hi = hi, lo

        if key in INT_KEYS:
            out[key] = random.randint(int(lo), int(hi))
        else:
            out[key] = round(random.uniform(lo, hi), 4)

    return out
