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


def get_weekly_stats(days: int = 7, team: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """
    Query DB pour récupérer les stats des `days` derniers jours.

    Args:
        days: période (en jours)
        team: si fourni, filtre par team (geelark / instagram / threads).
              Si None, stats globales.

    Retourne None si DB pas dispo.
    """
    if not cf_storage.is_db_enabled():
        return None

    # Construit la clause team_filter dynamique
    team_filter_sql = ""
    team_params: tuple = ()
    if team:
        team_filter_sql = "AND team = %s"
        team_params = (team.lower().strip(),)

    try:
        with cf_storage._get_connection() as conn:
            with conn.cursor() as cur:
                # 1. Vue d'ensemble
                cur.execute(
                    f"""
                    SELECT
                        COUNT(*) AS total_batches,
                        COALESCE(SUM(videos_count), 0) AS total_requested,
                        COALESCE(SUM(videos_uploaded), 0) AS total_uploaded,
                        COUNT(DISTINCT va_name) AS unique_vas,
                        COALESCE(AVG(duration_seconds), 0) AS avg_duration
                    FROM cf_batches
                    WHERE created_at > NOW() - (%s || ' days')::INTERVAL
                    {team_filter_sql}
                    """,
                    (str(days),) + team_params,
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
                    f"""
                    SELECT
                        va_name,
                        COUNT(*) AS batch_count,
                        COALESCE(SUM(videos_uploaded), 0) AS videos_uploaded
                    FROM cf_batches
                    WHERE created_at > NOW() - (%s || ' days')::INTERVAL
                        AND va_name != ''
                        {team_filter_sql}
                    GROUP BY va_name
                    ORDER BY videos_uploaded DESC
                    LIMIT 5
                    """,
                    (str(days),) + team_params,
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
                    f"""
                    SELECT
                        va_name,
                        COUNT(*) AS batch_count,
                        COALESCE(SUM(videos_count), 0) AS requested,
                        COALESCE(SUM(videos_uploaded), 0) AS uploaded
                    FROM cf_batches
                    WHERE created_at > NOW() - (%s || ' days')::INTERVAL
                        AND va_name != ''
                        {team_filter_sql}
                    GROUP BY va_name
                    HAVING COUNT(*) >= 3
                        AND COALESCE(SUM(videos_count), 0) > 0
                        AND (1.0 - CAST(SUM(videos_uploaded) AS FLOAT)
                             / NULLIF(SUM(videos_count), 0)) > 0.20
                    ORDER BY batch_count DESC
                    LIMIT 5
                    """,
                    (str(days),) + team_params,
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

    Stratégie de routage :
    - Pour chaque team (geelark, instagram, threads) dont le webhook
      DISCORD_ADMIN_WEBHOOK_URL_<TEAM> est défini, on envoie les stats
      FILTRÉES sur cette team uniquement.
    - Si aucun webhook par team n'est défini, fallback : 1 seul message
      avec stats globales sur DISCORD_ADMIN_WEBHOOK_URL.

    Retourne True si au moins un envoi a marché.
    """
    sent_any = False
    teams_to_check = ["geelark", "instagram", "threads"]

    # 1. Envoi per-team si webhook spécifique configuré
    for team in teams_to_check:
        if not is_admin_webhook_enabled(team=team):
            continue
        # Si la team n'a pas son propre webhook ET qu'on tombe sur le webhook
        # global, on skip cette boucle (sinon on enverrait 3 fois la même chose).
        team_webhook = os.environ.get(
            f"DISCORD_ADMIN_WEBHOOK_URL_{team.upper()}", ""
        ).strip()
        if not team_webhook:
            continue
        stats = get_weekly_stats(days=days, team=team)
        if not stats or stats["overview"]["total_batches"] == 0:
            continue
        msg = format_weekly_stats_message(stats)
        title = f"Stats hebdo ClipFusion · {team.capitalize()}"
        ok = await send_admin_alert(title=title, message=msg, level="info", team=team)
        sent_any = sent_any or ok

    # 2. Fallback : si aucun webhook par team, envoi global
    if not sent_any and is_admin_webhook_enabled():
        stats = get_weekly_stats(days=days, team=None)
        if stats:
            msg = format_weekly_stats_message(stats)
            title = "Stats hebdo ClipFusion"
            ok = await send_admin_alert(title=title, message=msg, level="info")
            sent_any = sent_any or ok

    if not sent_any:
        logger.info("Stats hebdo skip : aucun webhook admin configuré")
    return sent_any


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


# ============================================================================
# ANOMALY DETECTION : check volume 3x usual toutes les 4h
# ============================================================================
async def check_and_alert_anomalies() -> int:
    """
    Vérifie les anomalies de volume et envoie un admin alert si y en a.
    Groupe par team pour router vers le webhook correspondant.
    Retourne le nombre TOTAL d'anomalies détectées (0 = tout normal).
    """
    try:
        anomalies = cf_storage.get_va_volume_anomalies(
            multiplier_threshold=float(os.environ.get("CF_ANOMALY_MULTIPLIER", "3")),
            min_today_videos=int(os.environ.get("CF_ANOMALY_MIN_VIDEOS", "50")),
            baseline_window_days=int(os.environ.get("CF_ANOMALY_BASELINE_DAYS", "14")),
        )
    except Exception as e:
        logger.warning(f"check_and_alert_anomalies query failed: {e}")
        return 0

    if not anomalies:
        return 0

    # Group by team (vide = unknown → ira sur webhook fallback)
    by_team: Dict[str, List[Dict[str, Any]]] = {}
    for a in anomalies:
        t = (a.get("team") or "").lower().strip() or "_unknown"
        by_team.setdefault(t, []).append(a)

    total_sent = 0
    for team, items in by_team.items():
        # Le webhook : team-specific si dispo, sinon fallback global
        team_arg = None if team == "_unknown" else team
        if not is_admin_webhook_enabled(team=team_arg):
            continue
        team_label = team.capitalize() if team != "_unknown" else ""
        lines = [f"**⚠️ Volumes anormaux 24h{(' · ' + team_label) if team_label else ''} :**\n"]
        for a in items:
            if a["is_new_va"]:
                lines.append(
                    f"• `{a['va_name']}` (NOUVEAU VA) — **{a['today_videos']}** vidéos "
                    f"en {a['today_batches']} batchs"
                )
            else:
                lines.append(
                    f"• `{a['va_name']}` — **{a['today_videos']}** vidéos "
                    f"(vs avg {a['baseline_avg']}/jour → **×{a['ratio']}**)"
                )
        lines.append(
            "\n_Check rapide : compte compromis ? VA qui spam ? Panique ?_"
        )
        ok = await send_admin_alert(
            title=f"Volume anormal{(' · ' + team_label) if team_label else ''} ({len(items)} VA)",
            message="\n".join(lines),
            level="warning",
            team=team_arg,
        )
        if ok:
            total_sent += len(items)
    return total_sent


async def check_drive_quota_and_alert(threshold_pct: float = 80.0) -> bool:
    """
    Check le quota Drive du service account et alerte si >= threshold_pct.

    Pour un Service Account "personnel" : quota Drive normal (15 GB free).
    Pour un Workspace : quota plus haut mais existe quand même.

    Si l'usage dépasse threshold_pct (default 80%), ping admin webhook avec
    le détail. Évite le shutdown brutal quand le quota est plein
    (les uploads échouent silencieusement après).
    """
    if not is_admin_webhook_enabled():
        return False
    try:
        from app.services.drive_service import get_drive_client, is_drive_enabled
        if not is_drive_enabled():
            return False
        client = get_drive_client()
        if client is None:
            return False
        about = client.about().get(fields="storageQuota,user").execute()
        quota = about.get("storageQuota", {}) or {}
        usage_bytes = int(quota.get("usage", 0))
        limit_raw = quota.get("limit")
        # Workspace / unlimited storage : pas de limit → skip
        if not limit_raw:
            return False
        limit_bytes = int(limit_raw)
        if limit_bytes <= 0:
            return False
        pct = round(100.0 * usage_bytes / limit_bytes, 1)
        if pct < threshold_pct:
            return False  # tout va bien, pas d'alerte
        used_gb = usage_bytes / (1024**3)
        limit_gb = limit_bytes / (1024**3)
        free_gb = (limit_bytes - usage_bytes) / (1024**3)
        msg = (
            f"⚠️ **Quota Drive à {pct}%** ({used_gb:.1f} / {limit_gb:.1f} GB)\n"
            f"Reste seulement **{free_gb:.1f} GB** libres.\n\n"
            "_Actions possibles :_\n"
            "• Cleanup manuel via `/api/admin/cleanup-drive`\n"
            "• Vide les vieux batchs Drive manuellement\n"
            "• Augmente le quota du compte (Workspace si pas déjà)"
        )
        await send_admin_alert(
            title=f"Drive quota {pct}% - action requise",
            message=msg,
            level="warning" if pct < 95 else "error",
        )
        return True
    except Exception as e:
        logger.warning(f"check_drive_quota_and_alert failed: {e}")
        return False


def _start_drive_quota_scheduler() -> None:
    """
    Daemon qui check le quota Drive toutes les 6h.
    Désactivable via CF_DRIVE_QUOTA_CHECK_ENABLED=0.
    """
    if os.environ.get("CF_DRIVE_QUOTA_CHECK_ENABLED", "1").strip().lower() in (
        "0", "false", "no", "off",
    ):
        logger.info("Drive quota check désactivé")
        return

    def _loop():
        # Premier passage 1h après boot
        _time.sleep(60 * 60)
        interval_seconds = 6 * 3600
        while True:
            try:
                threshold = float(os.environ.get("CF_DRIVE_QUOTA_THRESHOLD_PCT", "80"))
                asyncio.run(check_drive_quota_and_alert(threshold_pct=threshold))
            except Exception as e:
                logger.warning(f"Drive quota loop error: {e}")
            _time.sleep(interval_seconds)

    try:
        t = threading.Thread(target=_loop, daemon=True, name="cf-drive-quota")
        t.start()
        logger.info("✅ Drive quota scheduler started (toutes les 6h)")
    except Exception as e:
        logger.warning(f"Failed to start drive quota scheduler: {e}")


def _start_anomaly_check_scheduler() -> None:
    """
    Daemon qui check les anomalies toutes les 4h.
    Désactivable via CF_ANOMALY_CHECK_ENABLED=0.
    """
    if os.environ.get("CF_ANOMALY_CHECK_ENABLED", "1").strip().lower() in (
        "0",
        "false",
        "no",
        "off",
    ):
        logger.info("Anomaly check désactivé (CF_ANOMALY_CHECK_ENABLED=0)")
        return

    def _loop():
        # Premier passage 30 min après boot (laisse les batchs du moment se logger)
        _time.sleep(30 * 60)
        interval_seconds = 4 * 3600  # 4h
        while True:
            try:
                n = asyncio.run(check_and_alert_anomalies())
                if n > 0:
                    logger.info(f"🚨 Anomaly check: {n} anomalies envoyées à l'admin")
            except Exception as e:
                logger.warning(f"Anomaly check loop error: {e}")
            _time.sleep(interval_seconds)

    try:
        t = threading.Thread(target=_loop, daemon=True, name="cf-anomaly-check")
        t.start()
        logger.info("✅ Anomaly check scheduler started (toutes les 4h)")
    except Exception as e:
        logger.warning(f"Failed to start anomaly check scheduler: {e}")
