"""
Service Google Drive.

Gère l'authentification via Service Account et l'upload de fichiers
dans un dossier partagé du Drive Fadeflux.

Variables d'environnement requises :
  - GOOGLE_CREDENTIALS_JSON : contenu JSON complet de la clé du Service Account
  - GOOGLE_DRIVE_PARENT_ID  : ID du dossier "racine" sur Drive
"""
import csv
import io
import json
import os
from pathlib import Path
from typing import Dict, List, Optional

from app.utils.logger import get_logger

logger = get_logger("drive_service")


# Imports Google : lazy pour que l'app démarre même si les libs ne sont pas là
def _load_google_libs():
    from google.oauth2 import service_account
    from googleapiclient.discovery import build
    from googleapiclient.http import MediaFileUpload, MediaIoBaseUpload
    return service_account, build, MediaFileUpload, MediaIoBaseUpload


# ---------------------------------------------------------------------------
# Singleton client Drive
# ---------------------------------------------------------------------------
_drive_client = None


def get_drive_client():
    """
    Retourne un client Drive authentifié, ou None si pas configuré.
    Cache le client pour éviter de re-authentifier à chaque appel.
    """
    global _drive_client
    if _drive_client is not None:
        return _drive_client

    creds_json = os.getenv("GOOGLE_CREDENTIALS_JSON")
    if not creds_json:
        logger.info("GOOGLE_CREDENTIALS_JSON non défini : Drive désactivé.")
        return None

    try:
        service_account, build, _, _ = _load_google_libs()
    except ImportError as e:
        logger.error(f"Libs Google manquantes : {e}")
        return None

    try:
        info = json.loads(creds_json)
        credentials = service_account.Credentials.from_service_account_info(
            info,
            scopes=["https://www.googleapis.com/auth/drive"],
        )
        _drive_client = build("drive", "v3", credentials=credentials, cache_discovery=False)
        logger.info("Client Google Drive initialisé.")
        return _drive_client
    except Exception as e:
        logger.error(f"Erreur init Drive : {e}")
        return None


def is_drive_enabled() -> bool:
    """True si Drive est correctement configuré."""
    return (
        get_drive_client() is not None
        and bool(os.getenv("GOOGLE_DRIVE_PARENT_ID"))
    )


# ---------------------------------------------------------------------------
# Opérations Drive
# ---------------------------------------------------------------------------
def create_batch_folder(batch_name: str) -> Optional[str]:
    """
    Crée un sous-dossier dans le dossier parent Drive.
    Retourne l'ID du dossier créé, ou None si échec.
    """
    client = get_drive_client()
    parent_id = os.getenv("GOOGLE_DRIVE_PARENT_ID")
    if not client or not parent_id:
        return None

    try:
        metadata = {
            "name": batch_name,
            "mimeType": "application/vnd.google-apps.folder",
            "parents": [parent_id],
        }
        folder = client.files().create(
            body=metadata,
            fields="id, webViewLink",
            supportsAllDrives=True,
        ).execute()
        logger.info(f"Dossier Drive créé : {batch_name} -> {folder['id']}")
        return folder["id"]
    except Exception as e:
        logger.error(f"Erreur création dossier Drive : {e}")
        return None


def upload_file(
    local_path: Path,
    folder_id: str,
    mime_type: str = "video/mp4",
) -> Optional[Dict]:
    """
    Upload un fichier local vers Drive dans le dossier spécifié.
    Retourne un dict {id, name, webViewLink} ou None.
    """
    client = get_drive_client()
    if not client:
        return None

    try:
        _, _, MediaFileUpload, _ = _load_google_libs()
        metadata = {
            "name": local_path.name,
            "parents": [folder_id],
        }
        media = MediaFileUpload(str(local_path), mimetype=mime_type, resumable=True)
        result = client.files().create(
            body=metadata,
            media_body=media,
            fields="id, name, webViewLink",
            supportsAllDrives=True,
        ).execute()
        logger.info(f"Uploadé sur Drive : {local_path.name}")
        return result
    except Exception as e:
        logger.error(f"Erreur upload Drive {local_path.name} : {e}")
        return None


def upload_csv(
    folder_id: str,
    rows: List[Dict],
    filename: str = "metadata.csv",
) -> Optional[Dict]:
    """
    Upload un CSV généré en mémoire depuis une liste de dicts.
    Utilisé pour les métadonnées des vidéos d'un batch.
    """
    client = get_drive_client()
    if not client or not rows:
        return None

    try:
        _, _, _, MediaIoBaseUpload = _load_google_libs()

        # Génère le CSV en mémoire
        buf = io.StringIO()
        # Union de toutes les clés pour gérer des rows hétérogènes
        fieldnames = []
        for r in rows:
            for k in r.keys():
                if k not in fieldnames:
                    fieldnames.append(k)
        writer = csv.DictWriter(buf, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
        data = buf.getvalue().encode("utf-8")

        media = MediaIoBaseUpload(
            io.BytesIO(data), mimetype="text/csv", resumable=False
        )
        metadata = {"name": filename, "parents": [folder_id]}
        result = client.files().create(
            body=metadata,
            media_body=media,
            fields="id, name, webViewLink",
            supportsAllDrives=True,
        ).execute()
        logger.info(f"CSV uploadé sur Drive : {filename}")
        return result
    except Exception as e:
        logger.error(f"Erreur upload CSV : {e}")
        return None


def get_folder_link(folder_id: str) -> str:
    """Construit l'URL publique du dossier Drive (pour l'afficher dans l'UI)."""
    return f"https://drive.google.com/drive/folders/{folder_id}"
