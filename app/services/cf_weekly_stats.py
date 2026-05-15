"""
Stats hebdo ClipFusion — envoyées automatiquement chaque lundi à 09:00 UTC
dans le webhook admin (DISCORD_ADMIN_WEBHOOK_URL).

Source : table cf_batches (les batchs sont loggés à chaque /request réussi).

Ce qui est tracké :
- Volume total : nb de batchs, vidéos demandées, vidéos uploadées Drive
- Top 5 VAs par volume de vidéos
- VAs avec taux d'échec élevé (>20% fails, min 3 batchs) → signal d'alerte
- Durée moyenne d'un batch (utile pour spot un slowdown FFmpeg)

Désactivation : env var CF_WEEKLY_STATS_ENABLED=0
"""
import asyncio
import os
import threading
import time as _time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from app.services import cf_storage
from app.services.discord_service import send_admin_alert, is_admin_webhook_enabled
from app.utils.logger import get_logger

logger = get_logger("cf_weekly_stats")


def _is_enabled() -> bool:
    """Désactivable via env var CF_WEEKLY_STATS_ENABLED=0."""
    raw = os.environ.get("CF_WEEKLY_STATS_ENABLED", "1").strip().lower()
    return raw not in ("0", "false", "no", "off", "")


def get_weekly_stats(days: int = 7) -> Optional[Dict[str, Any]]:
    """
    Query DB pour récupérer les stats des `days` derniers jours.
    Retourne None si DB pas dispo.
    """
    if not cf_storage.is_db_enabled():
        return None

    try:
        with cf_storage._get_connection() as conn:
            with conn.cursor() as cur:
                # 1. Vue d'ensemble
                cur.execute(
                    """
                    SELECT
                        COUNT(*) AS total_batches,
                        COALESCE(SUM(videos_count), 0) AS total_requested,
                        COALESCE(SUM(videos_uploaded), 0) AS total_uploaded,
                        COUNT(DISTINCT va_name) AS unique_vas,
                        COALESCE(AVG(duration_seconds), 0) AS avg_duration
                    FROM cf_batches
                    WHERE created_at > NOW() - (%s || ' days')::INTERVAL
                    """,
                    (str(days),),
                )
                row = cur.fetchone() or (0, 0, 0, 0, 0)
                overview = {
                    "total_batches": int(row[0]),
                    "total_requested": int(row[1]),
                    "total_uploaded": int(row[2]),
                    "unique_vas": int(row[3]),
                    "avg_duration_seconds": float(row[4]),
                }

                # 2. Top 5 VAs par vidéos uploadées
                cur.execute(
                    """
                    SELECT
                        va_name,
                        COUNT(*) AS batch_count,
                        COALESCE(SUM(videos_uploaded), 0) AS videos_uploaded
                    FROM cf_batches
                    WHERE created_at > NOW() - (%s || ' days')::INTERVAL
                        AND va_name != ''
                    GROUP BY va_name
                    ORDER BY videos_uploaded DESC
                    LIMIT 5
                    """,
                    (str(days),),
                )
                top_vas = [
                    {
                        "va_name": r[0],
                        "batch_count": int(r[1]),
                        "videos_uploaded": int(r[2]),
                    }
                    for r in cur.fetchall()
                ]

                # 3. VAs avec taux d'échec >20% (min 3 batchs pour éviter le bruit)
                cur.execute(
                    """
                    SELECT
                        va_name,
                        COUNT(*) AS batch_count,
                        COALESCE(SUM(videos_count), 0) AS requested,
                        COALESCE(SUM(videos_uploaded), 0) AS uploaded
                    FROM cf_batches
                    WHERE created_at > NOW() - (%s || ' days')::INTERVAL
                        AND va_name != ''
                    GROUP BY va_name
                    HAVING COUNT(*) >= 3
                        AND COALESCE(SUM(videos_count), 0) > 0
                        AND (1.0 - CAST(SUM(videos_uploaded) AS FLOAT)
                             / NULLIF(SUM(videos_count), 0)) > 0.20
                    ORDER BY batch_count DESC
                    LIMIT 5
                    """,
                    (str(days),),
                )
                problem_vas = []
                for r in cur.fetchall():
                    requested = int(r[2]) or 1
                    uploaded = int(r[3])
                    fail_rate = round(100.0 * (1 - uploaded / requested), 1)
                    problem_vas.append(
                        {
                            "va_name": r[0],
                            "batch_count": int(r[1]),
                            "requested": requested,
                            "uploaded": uploaded,
                            "fail_rate_pct": fail_rate,
                        }
                    )

        return {
            "overview": overview,
            "top_vas": top_vas,
            "problem_vas": problem_vas,
            "days": days,
        }
    except Exception as e:
        logger.error(f"get_weekly_stats failed: {e}")
        return None


def format_weekly_stats_message(stats: Dict[str, Any]) -> str:
    """Formate les stats en Markdown Discord (lisible dans un embed admin)."""
    ov = stats["overview"]
    days = stats.get("days", 7)
    success_rate = 0.0
    if ov["total_requested"] > 0:
        success_rate = round(100.0 * ov["total_uploaded"] / ov["total_requested"], 1)

    lines: List[str] = []
    lines.append(f"**📊 Bilan des {days} derniers jours**\n")
    lines.append(f"• **{ov['total_batches']}** batchs lancés par **{ov['unique_vas']}** VAs uniques")
    lines.append(
        f"• **{ov['total_uploaded']} / {ov['total_requested']}** vidéos uploadées Drive "
        f"({success_rate}% succès)"
    )
    lines.append(f"• Durée moyenne batch : **{ov['avg_duration_seconds']:.0f}s**")

    if stats["top_vas"]:
        lines.append("\n**🏆 Top VAs (vidéos uploadées) :**")
        for i, va in enumerate(stats["top_vas"], 1):
            lines.append(
                f"{i}. `{va['va_name']}` — **{va['videos_uploaded']}** vidéos "
                f"({va['batch_count']} batchs)"
            )

    if stats["problem_vas"]:
        lines.append("\n**⚠️ VAs avec taux d'échec > 20% :**")
        for va in stats["problem_vas"]:
            lines.append(
                f"• `{va['va_name']}` — **{va['fail_rate_pct']}%** "
                f"({va['uploaded']}/{va['requested']} sur {va['batch_count']} batchs)"
            )
    else:
        lines.append("\n✅ Aucun VA avec taux d'échec élevé")

    return "\n".join(lines)


async def send_weekly_stats(days: int = 7) -> bool:
    """
    Query + format + envoie au webhook admin.
    Retourne True si envoyé avec succès.
    """
    if not is_admin_webhook_enabled():
        logger.info("Stats hebdo skip : DISCORD_ADMIN_WEBHOOK_URL non configuré")
        return False

    stats = get_weekly_stats(days=days)
    if not stats:
        logger.warning("Stats hebdo : query DB échouée ou vide")
        return False

    msg = format_weekly_stats_message(stats)
    title = "Stats hebdo ClipFusion"
    return await send_admin_alert(title=title, message=msg, level="info")


# ============================================================================
# SCHEDULER : envoi auto chaque lundi 09:00 UTC
# ============================================================================
def _seconds_until_next_monday_9utc() -> float:
    """Calcule les secondes jusqu'au prochain lundi 09:00 UTC."""
    now = datetime.now(timezone.utc)
    # weekday : lundi = 0, dimanche = 6
    days_until = (7 - now.weekday()) % 7
    target = now.replace(hour=9, minute=0, second=0, microsecond=0)
    if days_until == 0 and now >= target:
        # Lundi mais après 9h → on vise le lundi suivant
        days_until = 7
    target += timedelta(days=days_until)
    return max(60.0, (target - now).total_seconds())


def _start_weekly_stats_scheduler() -> None:
    """
    Daemon thread qui envoie les stats chaque lundi à 09:00 UTC.
    Désactivable via env var CF_WEEKLY_STATS_ENABLED=0.
    """
    if not _is_enabled():
        logger.info("Stats hebdo désactivées (CF_WEEKLY_STATS_ENABLED=0)")
        return

    def _loop():
        while True:
            try:
                wait_seconds = _seconds_until_next_monday_9utc()
                logger.info(
                    f"Stats hebdo : prochaine exécution dans {wait_seconds/3600:.1f}h"
                )
                _time.sleep(wait_seconds)
                # Run async send dans son propre event loop (on est dans un thread)
                try:
                    asyncio.run(send_weekly_stats(days=7))
                except Exception as e:
                    logger.warning(f"send_weekly_stats échoué: {e}")
            except Exception as e:
                logger.warning(f"Stats hebdo loop error: {e}")
                # Anti-tight-loop si quelque chose foire en série
                _time.sleep(3600)

    try:
        t = threading.Thread(target=_loop, daemon=True, name="cf-weekly-stats")
        t.start()
        logger.info("✅ Weekly stats scheduler started (lundis 09:00 UTC)")
    except Exception as e:
        logger.warning(f"Failed to start weekly stats scheduler: {e}")
