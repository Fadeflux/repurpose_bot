"""
Service Google Drive avec support OAuth (recommandé) ou Service Account (fallback).

Variables d'environnement :
  Option 1 - OAuth (RECOMMANDÉ — utilise ton quota personnel 200 GB) :
    - GOOGLE_OAUTH_TOKEN_JSON : contenu JSON du token OAuth généré avec generate_token.py
    - GOOGLE_DRIVE_PARENT_ID  : ID du dossier racine sur Drive

  Option 2 - Service Account (fallback, souvent limité par quota) :
    - GOOGLE_CREDENTIALS_JSON : contenu JSON du Service Account
    - GOOGLE_DRIVE_PARENT_ID  : ID du dossier racine
"""
import csv
import io
import json
import os
from pathlib import Path
from typing import Dict, List, Optional

from app.utils.logger import get_logger

logger = get_logger("drive_service")


# ---------------------------------------------------------------------------
# Lazy loading des libs Google
# ---------------------------------------------------------------------------
def _load_google_libs():
    from google.oauth2 import service_account
    from google.oauth2.credentials import Credentials as OAuthCredentials
    from google.auth.transport.requests import Request
    from googleapiclient.discovery import build
    from googleapiclient.http import MediaFileUpload, MediaIoBaseUpload
    return service_account, OAuthCredentials, Request, build, MediaFileUpload, MediaIoBaseUpload


# ---------------------------------------------------------------------------
# Client singleton
# ---------------------------------------------------------------------------
_drive_client = None
_auth_mode = None  # "oauth" ou "service_account"


def get_drive_client():
    """
    Retourne un client Drive authentifié.
    Priorité :
      1. OAuth via GOOGLE_OAUTH_TOKEN_JSON (recommandé, pas de problème de quota)
      2. Service Account via GOOGLE_CREDENTIALS_JSON (fallback, limité par quota)
    """
    global _drive_client, _auth_mode
    if _drive_client is not None:
        return _drive_client

    _, OAuthCredentials, Request, build, _, _ = _load_google_libs()

    # --- Tentative 1 : OAuth ---
    oauth_json = os.getenv("GOOGLE_OAUTH_TOKEN_JSON")
    if oauth_json:
        try:
            token_data = json.loads(oauth_json)
            creds = OAuthCredentials(
                token=token_data.get("token"),
                refresh_token=token_data.get("refresh_token"),
                token_uri=token_data.get("token_uri"),
                client_id=token_data.get("client_id"),
                client_secret=token_data.get("client_secret"),
                scopes=token_data.get("scopes", ["https://www.googleapis.com/auth/drive"]),
            )
            # Rafraîchit si expiré
            if creds.expired and creds.refresh_token:
                creds.refresh(Request())
                logger.info("Token OAuth rafraîchi automatiquement")
            _drive_client = build("drive", "v3", credentials=creds, cache_discovery=False)
            _auth_mode = "oauth"
            logger.info("Drive client initialisé (mode OAuth - quota utilisateur)")
            return _drive_client
        except Exception as e:
            logger.error(f"Erreur OAuth: {type(e).__name__}: {e}")
            # On tombe en fallback Service Account

    # --- Tentative 2 : Service Account (fallback) ---
    creds_json = os.getenv("GOOGLE_CREDENTIALS_JSON")
    if not creds_json:
        logger.info("Aucune auth configurée (ni OAuth ni Service Account) : Drive désactivé.")
        return None

    try:
        service_account, _, _, build, _, _ = _load_google_libs()
        creds_dict = json.loads(creds_json)
        creds = service_account.Credentials.from_service_account_info(
            creds_dict,
            scopes=["https://www.googleapis.com/auth/drive"],
        )
        _drive_client = build("drive", "v3", credentials=creds, cache_discovery=False)
        _auth_mode = "service_account"
        logger.info("Drive client initialisé (mode Service Account - attention quota)")
        return _drive_client
    except Exception as e:
        logger.error(f"Erreur Service Account: {type(e).__name__}: {e}")
        return None


def is_drive_enabled() -> bool:
    """Retourne True si Drive est configuré et accessible."""
    return (
        get_drive_client() is not None
        and bool(os.getenv("GOOGLE_DRIVE_PARENT_ID"))
    )


def get_auth_mode() -> Optional[str]:
    """Retourne 'oauth', 'service_account' ou None."""
    get_drive_client()  # force l'init
    return _auth_mode


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
        logger.error(f"upload_file: client Drive non initialisé pour {local_path.name}")
        return None

    if not local_path.exists():
        logger.error(f"upload_file: fichier inexistant {local_path}")
        return None

    try:
        _, _, _, _, MediaFileUpload, _ = _load_google_libs()
        metadata = {
            "name": local_path.name,
            "parents": [folder_id],
        }
        file_size_mb = local_path.stat().st_size / 1024 / 1024
        logger.info(f"upload_file: début upload {local_path.name} ({file_size_mb:.2f} MB) vers {folder_id}")

        media = MediaFileUpload(str(local_path), mimetype=mime_type, resumable=True)
        result = client.files().create(
            body=metadata,
            media_body=media,
            fields="id, name, webViewLink",
            supportsAllDrives=True,
        ).execute()
        logger.info(f"upload_file: OK {local_path.name} → {result.get('id')}")
        return result
    except Exception as e:
        logger.exception(f"upload_file: ÉCHEC {local_path.name} : {type(e).__name__}: {e}")
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

    # Construit le CSV en mémoire
    buf = io.StringIO()
    all_keys = set()
    for row in rows:
        all_keys.update(row.keys())
    fieldnames = sorted(all_keys)

    writer = csv.DictWriter(buf, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    for row in rows:
        writer.writerow(row)

    csv_bytes = buf.getvalue().encode("utf-8")
    try:
        _, _, _, _, _, MediaIoBaseUpload = _load_google_libs()
        metadata = {
            "name": filename,
            "parents": [folder_id],
        }
        media = MediaIoBaseUpload(
            io.BytesIO(csv_bytes), mimetype="text/csv", resumable=False
        )
        result = client.files().create(
            body=metadata,
            media_body=media,
            fields="id, webViewLink",
            supportsAllDrives=True,
        ).execute()
        logger.info(f"CSV uploadé sur Drive : {filename}")
        return result
    except Exception as e:
        logger.exception(f"Erreur upload CSV : {e}")
        return None


def get_folder_link(folder_id: str) -> str:
    """Construit l'URL web d'un dossier Drive."""
    return f"https://drive.google.com/drive/folders/{folder_id}"


def share_folder_with_users(
    folder_id: str,
    emails: List[str],
    role: str = "reader",
) -> dict:
    """
    Partage un dossier Drive avec une liste d'emails.
    role: "reader" (viewer) ou "writer" (editor).
    Retourne {"success": [...], "failed": [...]}.
    """
    client = get_drive_client()
    if not client or not folder_id or not emails:
        return {"success": [], "failed": []}

    success = []
    failed = []
    for email in emails:
        email = email.strip().lower()
        if not email or "@" not in email:
            continue
        try:
            client.permissions().create(
                fileId=folder_id,
                body={
                    "type": "user",
                    "role": role,
                    "emailAddress": email,
                },
                sendNotificationEmail=False,
                fields="id",
                supportsAllDrives=True,
            ).execute()
            success.append(email)
        except Exception as e:
            logger.warning(f"Drive share failed for {email}: {e}")
            failed.append({"email": email, "error": str(e)[:200]})
    logger.info(f"Dossier {folder_id} partagé : OK={len(success)}, KO={len(failed)}")
    return {"success": success, "failed": failed}


def upload_bytes(
    data: bytes,
    filename: str,
    folder_id: str,
    mime_type: str = "image/jpeg",
    make_public: bool = True,
) -> Optional[Dict]:
    """
    Upload des bytes directement vers Drive (pas besoin de fichier local).
    Si make_public=True, génère un lien de download public (anyone with link).
    Retourne {id, name, webViewLink, webContentLink} ou None.
    """
    import io
    client = get_drive_client()
    if not client:
        logger.error(f"upload_bytes: client Drive non initialisé pour {filename}")
        return None
    try:
        _, _, _, _, _, MediaIoBaseUpload = _load_google_libs()
        metadata = {
            "name": filename,
            "parents": [folder_id],
        }
        media = MediaIoBaseUpload(
            io.BytesIO(data),
            mimetype=mime_type,
            resumable=False,
        )
        result = client.files().create(
            body=metadata,
            media_body=media,
            fields="id, name, webViewLink, webContentLink",
            supportsAllDrives=True,
        ).execute()
        file_id = result.get("id")
        logger.info(f"upload_bytes: OK {filename} → {file_id}")

        # Rend le fichier accessible via "anyone with link"
        if make_public and file_id:
            try:
                client.permissions().create(
                    fileId=file_id,
                    body={"role": "reader", "type": "anyone"},
                    supportsAllDrives=True,
                ).execute()
                # Récupère le webContentLink (download direct) après permission set
                refreshed = client.files().get(
                    fileId=file_id,
                    fields="id, name, webViewLink, webContentLink",
                    supportsAllDrives=True,
                ).execute()
                result.update(refreshed)
                logger.info(f"upload_bytes: fichier {file_id} rendu public")
            except Exception as e:
                logger.warning(f"upload_bytes: impossible de rendre public {file_id}: {e}")

        return result
    except Exception as e:
        logger.exception(f"upload_bytes: ÉCHEC {filename}: {type(e).__name__}: {e}")
        return None


def delete_file(file_id: str) -> bool:
    """Supprime définitivement un fichier Drive par son ID."""
    client = get_drive_client()
    if not client:
        return False
    try:
        client.files().delete(fileId=file_id, supportsAllDrives=True).execute()
        logger.info(f"delete_file: {file_id} supprimé")
        return True
    except Exception as e:
        logger.warning(f"delete_file: ÉCHEC {file_id}: {e}")
        return False


def get_or_create_subfolder(parent_id: str, subfolder_name: str) -> Optional[str]:
    """
    Retourne l'ID du sous-dossier s'il existe, sinon le crée.
    Utile pour avoir un dossier 'spoof-photos-temp' persistant.
    """
    client = get_drive_client()
    if not client:
        return None
    try:
        # Cherche si le sous-dossier existe déjà
        query = (
            f"'{parent_id}' in parents and "
            f"name = '{subfolder_name}' and "
            f"mimeType = 'application/vnd.google-apps.folder' and "
            f"trashed = false"
        )
        res = client.files().list(
            q=query,
            fields="files(id, name)",
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
        ).execute()
        existing = res.get("files", [])
        if existing:
            logger.info(f"Sous-dossier '{subfolder_name}' déjà existant: {existing[0]['id']}")
            return existing[0]["id"]

        # Sinon, crée le sous-dossier
        metadata = {
            "name": subfolder_name,
            "mimeType": "application/vnd.google-apps.folder",
            "parents": [parent_id],
        }
        created = client.files().create(
            body=metadata,
            fields="id",
            supportsAllDrives=True,
        ).execute()
        sub_id = created.get("id")
        logger.info(f"Sous-dossier '{subfolder_name}' créé: {sub_id}")
        return sub_id
    except Exception as e:
        logger.exception(f"get_or_create_subfolder: ÉCHEC {subfolder_name}: {e}")
        return None

