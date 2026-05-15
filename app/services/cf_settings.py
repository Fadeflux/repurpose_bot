"""
Catalogue centralisé des variables d'environnement ClipFusion.

Ce module ne remplace pas les `os.environ.get(...)` éparpillés dans le code —
il les DOCUMENTE et les VALIDE au démarrage. À chaque déploiement Railway,
ce module log un récap clair :
- Quelles variables critiques sont configurées
- Quelles variables sont absentes (avec defaults appliqués)
- Quelles variables sont mal formatées (pour pas crasher au runtime)

Avantages :
- Vue d'ensemble : un seul endroit pour voir toutes les CF_* env vars
- Fail-fast : on sait au boot si une var critique manque (pas runtime)
- Documentation vivante : pas de var oubliée dans la doc Railway
- Sécurité : on log JAMAIS les valeurs sensibles (tokens, webhooks)

Pour ajouter une nouvelle env var :
1. L'ajouter au tableau ENV_VARS ci-dessous (avec catégorie + description)
2. La lire normalement avec os.environ.get(...) dans ton module
3. Au prochain boot, validate_env_vars() la check automatiquement
"""
import os
from dataclasses import dataclass
from typing import Callable, List, Optional

from app.utils.logger import get_logger

logger = get_logger("cf_settings")


@dataclass
class EnvVar:
    """Métadonnées d'une variable d'environnement."""
    name: str
    category: str             # "discord", "ffmpeg", "stats", "limits", etc.
    description: str
    default: Optional[str] = None  # None = pas de default (var requise OU optionnelle non-critique)
    is_critical: bool = False      # True = warn loud si absente
    is_secret: bool = False        # True = ne JAMAIS log la valeur (juste "set"/"unset")
    validator: Optional[Callable[[str], bool]] = None  # check le format si fourni


def _is_int(v: str) -> bool:
    try:
        int(v)
        return True
    except (ValueError, TypeError):
        return False


def _is_float(v: str) -> bool:
    try:
        float(v)
        return True
    except (ValueError, TypeError):
        return False


def _is_csv_int(v: str) -> bool:
    if not v:
        return True
    parts = [p.strip() for p in v.replace(";", ",").split(",") if p.strip()]
    return all(p.isdigit() for p in parts)


def _is_bool_flag(v: str) -> bool:
    return v.strip().lower() in ("0", "1", "true", "false", "yes", "no", "on", "off")


# ============================================================================
# CATALOGUE DES ENV VARS CLIPFUSION
# ============================================================================
ENV_VARS: List[EnvVar] = [
    # --- DISCORD BOT (critique pour /request, /respoof, etc.) ---
    EnvVar("DISCORD_BOT_TOKEN", "discord", "Token du bot Discord (auth gateway)",
           is_critical=True, is_secret=True),
    EnvVar("DISCORD_GUILD_ID", "discord", "ID du serveur Discord principal (sync VAs)",
           validator=_is_int),
    EnvVar("CF_GUILD_ID_GEELARK", "discord", "ID du serveur Geelark → détection auto team",
           validator=_is_int),
    EnvVar("CF_GUILD_ID_INSTAGRAM", "discord", "ID du serveur Insta → détection auto team",
           validator=_is_int),
    EnvVar("CF_DEFAULT_TEAM", "discord", "Team par défaut si guild non mappé",
           default="geelark"),

    # --- CANAUX DISCORD ---
    EnvVar("CF_REQUEST_CHANNEL_IDS", "discord",
           "IDs des canaux où /request marche (CSV). Vide = partout.",
           validator=_is_csv_int),
    EnvVar("CF_RESPOOF_CHANNEL_IDS", "discord",
           "IDs des canaux où /respoof marche (CSV). Vide = fallback hardcoded.",
           validator=_is_csv_int),
    EnvVar("DISCORD_ONBOARDING_CHANNEL_ID", "discord",
           "Canal #email-drive Geelark (où VAs postent leur Gmail)",
           validator=_is_int),
    EnvVar("DISCORD_ONBOARDING_CHANNEL_ID_INSTAGRAM", "discord",
           "Canal #email-drive Instagram", validator=_is_int),
    EnvVar("DISCORD_ONBOARDING_CHANNEL_ID_THREADS", "discord",
           "Canal #email-drive Threads", validator=_is_int),
    EnvVar("DISCORD_SPOOF_CHANNEL_ID", "discord",
           "Canal spoof-photos Geelark", validator=_is_int),
    EnvVar("DISCORD_SPOOF_CHANNEL_ID_INSTAGRAM", "discord",
           "Canal spoof-photos Instagram", validator=_is_int),
    EnvVar("DISCORD_SPOOF_CHANNEL_ID_THREADS", "discord",
           "Canal spoof-photos Threads", validator=_is_int),

    # --- WEBHOOKS ---
    EnvVar("DISCORD_ADMIN_WEBHOOK_URL", "discord",
           "Webhook admin global (fallback si pas de webhook par équipe). "
           "Stats hebdo, anomaly alerts, batch fails, Drive quota.",
           is_secret=True),
    EnvVar("DISCORD_ADMIN_WEBHOOK_URL_GEELARK", "discord",
           "Webhook admin spécifique équipe Geelark (override le global)",
           is_secret=True),
    EnvVar("DISCORD_ADMIN_WEBHOOK_URL_INSTAGRAM", "discord",
           "Webhook admin spécifique équipe Instagram (override le global)",
           is_secret=True),
    EnvVar("DISCORD_ADMIN_WEBHOOK_URL_THREADS", "discord",
           "Webhook admin spécifique équipe Threads (override le global)",
           is_secret=True),
    EnvVar("DISCORD_WEBHOOK_URL", "discord",
           "Webhook général notifs batchs (fallback du bot)",
           is_secret=True),
    EnvVar("CF_LOLA_WEBHOOK_SECRET", "discord",
           "Shared secret pour le webhook Lola → repurpose_bot (va_banned). "
           "Si vide, le endpoint /api/lola/* est désactivé.",
           is_secret=True),

    # --- DRIVE ---
    EnvVar("GOOGLE_DRIVE_PARENT_ID", "drive",
           "ID du dossier Drive parent où les batchs sont créés",
           is_critical=True),
    EnvVar("GOOGLE_OAUTH_TOKEN_JSON", "drive", "Token OAuth Google Drive (JSON)",
           is_secret=True),
    EnvVar("GOOGLE_CREDENTIALS_JSON", "drive",
           "Credentials service account Google Drive (JSON)", is_secret=True),

    # --- DB ---
    EnvVar("DATABASE_URL", "db", "URL Postgres Railway", is_critical=True, is_secret=True),

    # --- LIMITS REQUEST ---
    EnvVar("CF_REQUEST_MAX_VIDEOS", "limits",
           "Max vidéos par /request", default="18", validator=_is_int),
    EnvVar("CF_RATE_LIMIT_VIDEOS", "limits",
           "Max vidéos par VA dans la fenêtre rate limit", default="500",
           validator=_is_int),
    EnvVar("CF_RATE_LIMIT_DAYS", "limits",
           "Durée fenêtre rate limit (jours)", default="3", validator=_is_int),
    EnvVar("CF_MIN_INTERVAL_HOURS_PER_ACCOUNT", "limits",
           "Intervalle min entre batchs sur le même compte (anti-pattern non humain)",
           default="6", validator=_is_int),

    # --- STATS & MONITORING ---
    EnvVar("CF_SECONDS_PER_VIDEO", "stats",
           "Estimation temps de mix par vidéo (sec) — utilisé pour ETA",
           default="22", validator=_is_int),
    EnvVar("CF_WEEKLY_STATS_ENABLED", "stats",
           "Active les stats hebdo (lundis 09:00 UTC)", default="1",
           validator=_is_bool_flag),
    EnvVar("CF_ANOMALY_CHECK_ENABLED", "stats",
           "Active la détection d'anomalies (toutes les 4h)", default="1",
           validator=_is_bool_flag),
    EnvVar("CF_ANOMALY_MULTIPLIER", "stats",
           "Seuil ratio today/baseline pour alerte", default="3",
           validator=_is_float),
    EnvVar("CF_ANOMALY_MIN_VIDEOS", "stats",
           "Min vidéos aujourd'hui pour considérer comme anomalie",
           default="50", validator=_is_int),
    EnvVar("CF_ANOMALY_BASELINE_DAYS", "stats",
           "Fenêtre de calcul du baseline (jours)", default="14",
           validator=_is_int),
    EnvVar("CF_ORPHAN_ACCOUNTS_DAYS", "stats",
           "Seuil cleanup comptes orphelins (0 = désactivé)", default="30",
           validator=_is_int),
    EnvVar("CF_ACCOUNT_AGING_ENABLED", "limits",
           "Active les daily caps progressifs sur les nouveaux comptes "
           "(évite le pattern bot 'spam day 1')", default="1",
           validator=_is_bool_flag),
    EnvVar("CF_AGING_DAY_0_3_MAX", "limits",
           "Cap vidéos/24h pour compte âgé 0-3 jours", default="5",
           validator=_is_int),
    EnvVar("CF_AGING_DAY_4_7_MAX", "limits",
           "Cap vidéos/24h pour compte âgé 4-7 jours", default="10",
           validator=_is_int),
    EnvVar("CF_AGING_DAY_8_14_MAX", "limits",
           "Cap vidéos/24h pour compte âgé 8-14 jours", default="20",
           validator=_is_int),

    # --- FFMPEG / QUALITÉ ---
    EnvVar("CF_HEVC_ENABLED", "ffmpeg",
           "Active l'encodage HEVC (libx265) pour iPhones modernes",
           default="1", validator=_is_bool_flag),
    EnvVar("CF_RESOLUTION_VARY", "ffmpeg",
           "Active la résolution variable (15% des mix en 720p)",
           default="1", validator=_is_bool_flag),

    # --- DISCORD MESSAGE TTL ---
    EnvVar("CF_CHANNEL_MSG_TTL", "discord",
           "TTL (sec) du message d'ack /request dans le canal (0 = jamais auto-delete)",
           default="30", validator=_is_int),

    # --- TIMEZONE ---
    EnvVar("CF_TZ_GEELARK", "discord",
           "TZ pour la team Geelark (benin / madagascar)", default="benin"),
    EnvVar("CF_TZ_INSTAGRAM", "discord",
           "TZ pour la team Instagram (benin / madagascar)", default="benin"),
]


def _redact(var: EnvVar, value: str) -> str:
    """Masque les valeurs secrètes pour logging."""
    if var.is_secret and value:
        return f"<set, {len(value)} chars>"
    if not value:
        return "<unset>"
    if len(value) > 60:
        return f"{value[:30]}...({len(value)} chars)"
    return value


def validate_env_vars() -> dict:
    """
    Valide toutes les env vars connues. Log le résumé.
    Retourne un dict {category: {name: {set, value, valid}}} pour usage programmatique.
    """
    report: dict = {}
    critical_missing: List[str] = []
    invalid: List[str] = []

    for var in ENV_VARS:
        raw = os.environ.get(var.name, "")
        is_set = bool(raw)
        is_valid = True
        if is_set and var.validator is not None:
            try:
                is_valid = var.validator(raw)
            except Exception:
                is_valid = False
        if not is_set and var.is_critical:
            critical_missing.append(var.name)
        if is_set and not is_valid:
            invalid.append(var.name)
        report.setdefault(var.category, {})[var.name] = {
            "set": is_set,
            "valid": is_valid,
            "display": _redact(var, raw) if is_set else "<unset>",
            "default": var.default,
            "critical": var.is_critical,
        }

    # Log la synthèse
    total = len(ENV_VARS)
    set_count = sum(1 for v in ENV_VARS if os.environ.get(v.name, ""))
    logger.info(
        f"⚙️ Env vars : {set_count}/{total} set, "
        f"{len(critical_missing)} critiques manquantes, "
        f"{len(invalid)} invalides"
    )
    if critical_missing:
        logger.warning(
            f"❌ Critical env vars MANQUANTES : {', '.join(critical_missing)} "
            f"→ certaines features ne marcheront pas"
        )
    if invalid:
        logger.warning(
            f"⚠️ Env vars INVALIDES (format) : {', '.join(invalid)} "
            f"→ fallback sur le default"
        )

    return report
