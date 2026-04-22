"""Génère un jeu de paramètres aléatoires dans les bornes définies."""
import random
from typing import Dict
from app.config import PARAM_RANGES


def random_params() -> Dict[str, float]:
    """
    Retourne un dict avec une valeur aléatoire entre min et max
    pour chaque paramètre. Les framerate/bitrate/noise sont des entiers,
    le reste des floats arrondis.
    """
    int_keys = {"framerate", "bitrate", "noise"}
    out: Dict[str, float] = {}
    for key, (lo, hi) in PARAM_RANGES.items():
        if key in int_keys:
            out[key] = random.randint(int(lo), int(hi))
        else:
            out[key] = round(random.uniform(lo, hi), 4)
    return out
