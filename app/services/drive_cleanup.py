"""
Service de nettoyage automatique du Drive.

Tourne 1 fois par jour à 3h du matin (heure UTC).
Supprime les sous-dossiers du dossier parent qui ont plus de N jours
(par défaut 30, configurable via DRIVE_CLEANUP_DAYS).

Le sous-dossier "spoof-photos-temp" est SAUVEGARDÉ (jamais supprimé)
car il contient des fichiers à durée de vie courte (2 min auto).

Variables d'environnement :
  DRIVE_CLEANUP_DAYS : âge max en jours avant suppression (défaut 30)
  DRIVE_CLEANUP_ENABLED : "true" pour activer (défaut "true")
  DRIVE_CLEANUP_HOUR_UTC : heure UTC à laquelle tourner (défaut 3)
"""
import asyncio
import os
from datetime import datetime, timedelta, timezone
from typing import List, Optional

from app.services.drive_service import get_drive_client
from app.utils.logger import get_logger

logger = get_logger("drive_cleanup")


# Sous-dossiers à JAMAIS supprimer (whitelist)
PROTECTED_FOLDERS = {"spoof-photos-temp"}


def is_cleanup_enabled() -> bool:
    return os.getenv("DRIVE_CLEANUP_ENABLED", "true").lower() in ("1", "true", "yes")


def _get_cleanup_days() -> int:
    try:
        return max(1, int(os.getenv("DRIVE_CLEANUP_DAYS", "30")))
    except ValueError:
        return 30


def _get_cleanup_hour_utc() -> int:
    try:
        h = int(os.getenv("DRIVE_CLEANUP_HOUR_UTC", "3"))
        return max(0, min(23, h))
    except ValueError:
        return 3


def _list_subfolders(parent_id: str) -> List[dict]:
    """Liste tous les sous-dossiers d'un parent Drive."""
    client = get_drive_client()
    if not client:
        return []
    try:
        all_folders = []
        page_token = None
        while True:
            query = (
                f"'{parent_id}' in parents and "
                f"mimeType = 'application/vnd.google-apps.folder' and "
                f"trashed = false"
            )
            res = client.files().list(
                q=query,
                fields="nextPageToken, files(id, name, createdTime)",
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
                pageToken=page_token,
                pageSize=100,
            ).execute()
            all_folders.extend(res.get("files", []))
            page_token = res.get("nextPageToken")
            if not page_token:
                break
        return all_folders
    except Exception as e:
        logger.exception(f"_list_subfolders error: {e}")
        return []


def _delete_folder(folder_id: str) -> bool:
    """Supprime un dossier Drive (et tout son contenu) en le mettant dans la corbeille."""
    client = get_drive_client()
    if not client:
        return False
    try:
        # Trash plutôt que delete définitif (récupérable pendant 30j depuis Drive UI)
        client.files().update(
            fileId=folder_id,
            body={"trashed": True},
            supportsAllDrives=True,
        ).execute()
        return True
    except Exception as e:
        logger.warning(f"Échec suppression {folder_id}: {e}")
        return False


def run_cleanup() -> dict:
    """
    Lance le nettoyage maintenant. Retourne un résumé.
    """
    parent_id = os.getenv("GOOGLE_DRIVE_PARENT_ID")
    if not parent_id:
        return {"ok": False, "error": "GOOGLE_DRIVE_PARENT_ID non défini"}

    days = _get_cleanup_days()
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    logger.info(f"Cleanup Drive: suppression des dossiers créés avant {cutoff.isoformat()} (>{days}j)")

    folders = _list_subfolders(parent_id)
    logger.info(f"{len(folders)} sous-dossiers à analyser")

    deleted = []
    skipped_protected = []
    skipped_recent = []
    failed = []

    for f in folders:
        name = f.get("name", "")
        fid = f.get("id")
        created_str = f.get("createdTime", "")

        # Skip whitelist
        if name in PROTECTED_FOLDERS:
            skipped_protected.append(name)
            continue

        # Parse la date de création
        try:
            created = datetime.fromisoformat(created_str.replace("Z", "+00:00"))
        except Exception:
            logger.warning(f"Date invalide pour {name}: {created_str}")
            continue

        if created > cutoff:
            skipped_recent.append({"name": name, "created": created_str})
            continue

        # Trop vieux : suppression
        if _delete_folder(fid):
            age_days = (datetime.now(timezone.utc) - created).days
            deleted.append({"name": name, "age_days": age_days, "id": fid})
            logger.info(f"  Supprimé: {name} ({age_days}j)")
        else:
            failed.append(name)

    summary = {
        "ok": True,
        "cutoff_days": days,
        "total_folders": len(folders),
        "deleted_count": len(deleted),
        "deleted": deleted,
        "skipped_protected": skipped_protected,
        "skipped_recent_count": len(skipped_recent),
        "failed": failed,
    }
    logger.info(
        f"Cleanup terminé: {len(deleted)} supprimés, "
        f"{len(skipped_recent)} récents, {len(skipped_protected)} protégés, "
        f"{len(failed)} échecs"
    )
    return summary


# =============================================================================
# Tâche planifiée : tourne 1 fois par jour à 3h UTC
# =============================================================================
_cleanup_task: Optional[asyncio.Task] = None


async def _periodic_cleanup_loop():
    """Tourne en boucle, attend jusqu'à la prochaine 3h UTC, puis run."""
    target_hour = _get_cleanup_hour_utc()
    while True:
        try:
            now = datetime.now(timezone.utc)
            # Calcule le prochain run (aujourd'hui à target_hour, ou demain si déjà passé)
            next_run = now.replace(hour=target_hour, minute=0, second=0, microsecond=0)
            if next_run <= now:
                next_run += timedelta(days=1)
            sleep_seconds = (next_run - now).total_seconds()
            logger.info(
                f"Drive cleanup planifié dans {sleep_seconds/3600:.1f}h "
                f"(prochain run: {next_run.isoformat()})"
            )
            await asyncio.sleep(sleep_seconds)

            # Run cleanup en thread pour ne pas bloquer
            loop = asyncio.get_event_loop()
            try:
                await loop.run_in_executor(None, run_cleanup)
            except Exception as e:
                logger.exception(f"Cleanup périodique échoué: {e}")
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.exception(f"Boucle cleanup error: {e}")
            # Attend 1h avant de retenter en cas d'erreur grave
            await asyncio.sleep(3600)


def start_periodic_cleanup():
    """Démarre le job de nettoyage en arrière-plan (à appeler au startup)."""
    global _cleanup_task
    if not is_cleanup_enabled():
        logger.info("Drive cleanup désactivé (DRIVE_CLEANUP_ENABLED=false)")
        return
    if _cleanup_task is None or _cleanup_task.done():
        _cleanup_task = asyncio.create_task(_periodic_cleanup_loop())
        days = _get_cleanup_days()
        hour = _get_cleanup_hour_utc()
        logger.info(
            f"Drive cleanup démarré "
            f"(dossiers >{days}j supprimés chaque jour à {hour}h UTC)"
        )
