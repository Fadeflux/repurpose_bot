"""Routes API pour l'upload et le traitement des vidéos."""
import asyncio
import json
import re
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import aiofiles
from fastapi import APIRouter, File, Form, HTTPException, UploadFile
from fastapi.responses import FileResponse, JSONResponse

from app.config import OUTPUT_DIR, UPLOAD_DIR, settings, PARAM_RANGES
from app.services.drive_service import (
    create_batch_folder,
    get_folder_link,
    is_drive_enabled,
    upload_csv,
    upload_file,
)
from app.services.discord_service import (
    is_discord_enabled,
    send_batch_notification,
)
from app.services.ffmpeg_service import process_video
from app.utils.logger import get_logger

router = APIRouter(prefix="/api", tags=["video"])
logger = get_logger("routes")


# ---------------------------------------------------------------------------
# Thread pool dédié aux uploads Drive : permet de paralléliser vraiment
# les uploads sans bloquer le pool default asyncio (qui est déjà pris par
# d'autres IO bloquants).
# ---------------------------------------------------------------------------
_drive_executor = ThreadPoolExecutor(max_workers=8, thread_name_prefix="drive-up")


# ---------------------------------------------------------------------------
# Tracking en mémoire du progrès des batches Drive
# {batch_id: {"total": N, "uploaded": X, "done": bool}}
# ---------------------------------------------------------------------------
_batch_progress: Dict[str, Dict] = {}
_MAX_TRACKED_BATCHES = 20  # garde en mémoire les 20 derniers batches


def _update_progress(batch_id: str, total: int = None, uploaded_delta: int = 0, done: bool = False):
    """Met à jour le progrès d'un batch en mémoire."""
    if batch_id not in _batch_progress:
        _batch_progress[batch_id] = {"total": 0, "uploaded": 0, "done": False}
    if total is not None:
        _batch_progress[batch_id]["total"] = total
    if uploaded_delta:
        _batch_progress[batch_id]["uploaded"] += uploaded_delta
    if done:
        _batch_progress[batch_id]["done"] = True
    # Nettoyage : garde seulement les N derniers
    if len(_batch_progress) > _MAX_TRACKED_BATCHES:
        oldest_keys = list(_batch_progress.keys())[:-_MAX_TRACKED_BATCHES]
        for k in oldest_keys:
            _batch_progress.pop(k, None)


@router.get("/health")
async def health():
    return {
        "status": "ok",
        "app": settings.APP_NAME,
        "version": settings.VERSION,
        "drive_enabled": is_drive_enabled(),
        "discord_enabled": is_discord_enabled(),
    }


@router.get("/params")
async def get_param_ranges():
    """Expose les bornes min/max par défaut de chaque paramètre."""
    return {
        key: {"min": lo, "max": hi}
        for key, (lo, hi) in PARAM_RANGES.items()
    }


@router.post("/admin/cleanup-drive")
async def admin_cleanup_drive():
    """
    Déclenche manuellement le nettoyage du Drive (suppression des batches > 30j).
    Le cleanup tourne aussi auto chaque jour à 3h UTC.
    """
    try:
        from app.services.drive_cleanup import run_cleanup
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(None, run_cleanup)
        return result
    except Exception as e:
        logger.exception(f"Erreur cleanup manuel: {e}")
        return JSONResponse(
            status_code=500,
            content={"ok": False, "error": f"{type(e).__name__}: {str(e)[:300]}"},
        )


@router.get("/discord-test")
async def test_discord_notif():
    """Envoie une notif Discord de test, retourne le résultat."""
    from app.services.discord_service import send_batch_notification, is_discord_enabled
    from app.services.discord_va_sync import find_va_discord_id

    if not is_discord_enabled():
        return {"ok": False, "error": "DISCORD_WEBHOOK_URL non configuré"}

    faudel_id = find_va_discord_id("Faudel")
    try:
        ok = await send_batch_notification(
            va_name="Faudel",
            va_discord_id=faudel_id or "",
            batch_name="TEST_notif",
            total_requested=1,
            succeeded=1,
            failed=0,
            drive_uploaded=1,
            retries_used=0,
            duration_seconds=5.0,
            device_choice="iphone_random",
            drive_folder_url="https://drive.google.com",
        )
        return {
            "ok": ok,
            "va_discord_id_found": faudel_id,
            "detail": "Notif envoyée, check ton Discord" if ok else "Échec, check Railway Deploy Logs",
        }
    except Exception as e:
        return {"ok": False, "error": f"{type(e).__name__}: {str(e)[:300]}"}


@router.get("/vas/debug")
async def debug_va_sync():
    """Diagnostic complet de la sync VA Discord."""
    import os
    import aiohttp
    from app.services.discord_va_sync import load_cached_vas, find_va_discord_id
    result = {
        "env_vars": {
            "DISCORD_BOT_TOKEN": bool(os.getenv("DISCORD_BOT_TOKEN")),
            "DISCORD_GUILD_ID": os.getenv("DISCORD_GUILD_ID"),
            "DISCORD_VA_ROLE_ID": os.getenv("DISCORD_VA_ROLE_ID"),
            "DISCORD_VA_ROLE_NAME": os.getenv("DISCORD_VA_ROLE_NAME"),
        },
        "cache": load_cached_vas(),
        "lookup_test": {
            "Faudel": find_va_discord_id("Faudel"),
            "andrept30": find_va_discord_id("andrept30"),
        },
        "tests": {},
    }
    token = os.getenv("DISCORD_BOT_TOKEN")
    guild_id = os.getenv("DISCORD_GUILD_ID")
    role_id_env = os.getenv("DISCORD_VA_ROLE_ID")
    if not token or not guild_id:
        result["error"] = "Variables manquantes"
        return result

    headers = {"Authorization": f"Bot {token}"}
    try:
        async with aiohttp.ClientSession() as session:
            # Test 1 : Identité du bot
            async with session.get(
                "https://discord.com/api/v10/users/@me", headers=headers, timeout=10
            ) as r:
                if r.status == 200:
                    bot_info = await r.json()
                    result["tests"]["bot_identity"] = {
                        "ok": True,
                        "name": bot_info.get("username"),
                        "id": bot_info.get("id"),
                    }
                else:
                    result["tests"]["bot_identity"] = {
                        "ok": False,
                        "status": r.status,
                        "body": (await r.text())[:300],
                    }
                    return result

            # Test 2 : Infos du serveur
            async with session.get(
                f"https://discord.com/api/v10/guilds/{guild_id}", headers=headers, timeout=10
            ) as r:
                if r.status == 200:
                    g = await r.json()
                    result["tests"]["guild"] = {
                        "ok": True,
                        "name": g.get("name"),
                        "member_count": g.get("approximate_member_count"),
                    }
                else:
                    result["tests"]["guild"] = {
                        "ok": False,
                        "status": r.status,
                        "body": (await r.text())[:300],
                    }
                    return result

            # Test 3 : Tous les rôles
            async with session.get(
                f"https://discord.com/api/v10/guilds/{guild_id}/roles", headers=headers, timeout=10
            ) as r:
                roles = await r.json() if r.status == 200 else []
                result["tests"]["all_roles"] = [
                    {"id": x["id"], "name": x["name"]} for x in roles
                ]
                if role_id_env:
                    match = next((x for x in roles if x["id"] == role_id_env), None)
                    result["tests"]["target_role_by_id"] = match

            # Test 4 : Membres (max 1000 pour le diag)
            async with session.get(
                f"https://discord.com/api/v10/guilds/{guild_id}/members?limit=1000",
                headers=headers,
                timeout=20,
            ) as r:
                if r.status != 200:
                    result["tests"]["members"] = {
                        "ok": False,
                        "status": r.status,
                        "body": (await r.text())[:500],
                        "hint": "Si 403 : active 'SERVER MEMBERS INTENT' dans le portail Discord",
                    }
                    return result
                members = await r.json()
                result["tests"]["members"] = {
                    "ok": True,
                    "total_fetched": len(members),
                }
                # Filtre par role_id
                if role_id_env:
                    matching = []
                    for m in members:
                        if role_id_env in m.get("roles", []):
                            u = m.get("user", {})
                            matching.append({
                                "name": m.get("nick") or u.get("global_name") or u.get("username"),
                                "user_id": u.get("id"),
                                "roles": m.get("roles"),
                            })
                    result["tests"]["members_with_target_role"] = matching
                # Échantillon des 5 premiers membres avec leurs rôles
                result["tests"]["sample_members"] = [
                    {
                        "name": m.get("nick") or m.get("user", {}).get("global_name") or m.get("user", {}).get("username"),
                        "roles": m.get("roles"),
                    }
                    for m in members[:5]
                ]
    except Exception as e:
        result["error"] = f"{type(e).__name__}: {str(e)[:300]}"
    return result


@router.get("/vas")
async def list_vas(team: str = ""):
    """
    Retourne la liste des VA actuellement en cache (sync Discord).
    Utilisé par le frontend pour peupler le dropdown.
    Filtrable par équipe via ?team=geelark ou ?team=instagram
    """
    try:
        from app.services.discord_va_sync import load_cached_vas, is_va_sync_enabled, get_teams_config
        data = load_cached_vas()
        vas = data.get("vas", [])

        # Filtre par équipe si demandé
        if team:
            team = team.lower().strip()
            filtered = []
            for v in vas:
                if isinstance(v, dict):
                    v_teams = v.get("teams") or [v.get("team")] or []
                    if team in [t for t in v_teams if t]:
                        filtered.append(v)
            vas = filtered

        # Liste des équipes disponibles
        teams_available = [t["name"] for t in get_teams_config()]

        return {
            "vas": vas,
            "last_sync": data.get("last_sync"),
            "sync_enabled": is_va_sync_enabled(),
            "teams_available": teams_available,
            "filtered_team": team or None,
        }
    except Exception as e:
        logger.exception(f"Erreur /api/vas: {e}")
        return {
            "vas": [],
            "last_sync": None,
            "sync_enabled": False,
            "error": f"{type(e).__name__}: {str(e)[:200]}",
        }


@router.post("/vas/sync")
async def force_va_sync():
    """
    Force une resync immédiate avec Discord.
    Utile pour tester ou rafraîchir manuellement.
    """
    try:
        from app.services.discord_va_sync import sync_va_list, is_va_sync_enabled
        if not is_va_sync_enabled():
            return JSONResponse(
                status_code=400,
                content={
                    "error": "Discord VA sync non configuré",
                    "detail": "Variables manquantes: DISCORD_BOT_TOKEN et/ou DISCORD_GUILD_ID",
                },
            )
        result = await sync_va_list()
        return result
    except Exception as e:
        logger.exception(f"Erreur /api/vas/sync: {e}")
        return JSONResponse(
            status_code=500,
            content={
                "error": type(e).__name__,
                "detail": str(e)[:500],
            },
        )


@router.post("/vas/email")
async def update_va_email_endpoint(
    discord_id: str = Form(...),
    email: str = Form(""),
):
    """Met à jour (ou supprime si vide) l'email d'un VA."""
    from app.services.discord_va_sync import update_va_email
    ok = update_va_email(discord_id, email)
    if ok:
        return {"ok": True, "discord_id": discord_id, "email": email}
    return JSONResponse(
        status_code=404,
        content={"ok": False, "error": "VA introuvable pour ce discord_id"},
    )


@router.get("/progress/{batch_id}")
async def get_batch_progress(batch_id: str):
    """
    Retourne le progrès d'upload Drive pour un batch en cours.
    Utilisé par le frontend pour afficher une barre de progression.
    """
    info = _batch_progress.get(batch_id)
    if not info:
        return {"found": False, "total": 0, "uploaded": 0, "done": False}
    return {
        "found": True,
        "total": info["total"],
        "uploaded": info["uploaded"],
        "done": info["done"],
        "percent": round(100 * info["uploaded"] / max(1, info["total"]), 1),
    }


@router.get("/progress-current")
async def get_current_batch_progress():
    """
    Retourne le progrès du batch le plus récent non terminé.
    Permet au frontend de polling sans connaître l'ID à l'avance.
    """
    # Cherche le dernier batch non terminé
    for batch_id in reversed(list(_batch_progress.keys())):
        info = _batch_progress[batch_id]
        if not info["done"]:
            return {
                "found": True,
                "batch_id": batch_id,
                "total": info["total"],
                "uploaded": info["uploaded"],
                "done": False,
                "percent": round(100 * info["uploaded"] / max(1, info["total"]), 1),
            }
    # Sinon retourne le tout dernier (terminé)
    if _batch_progress:
        batch_id = list(_batch_progress.keys())[-1]
        info = _batch_progress[batch_id]
        return {
            "found": True,
            "batch_id": batch_id,
            "total": info["total"],
            "uploaded": info["uploaded"],
            "done": info["done"],
            "percent": round(100 * info["uploaded"] / max(1, info["total"]), 1),
        }
    return {"found": False, "total": 0, "uploaded": 0, "done": False}


@router.get("/drive-debug")
async def drive_debug():
    """
    Teste la configuration Drive et fait un upload de test.
    Utilisé pour diagnostiquer les problèmes d'upload.
    """
    import os
    from app.services.drive_service import get_drive_client, get_auth_mode

    oauth_raw = os.getenv("GOOGLE_OAUTH_TOKEN_JSON") or ""
    creds_raw = os.getenv("GOOGLE_CREDENTIALS_JSON") or ""

    result = {
        "env_vars": {
            "GOOGLE_OAUTH_TOKEN_JSON": {
                "present": bool(oauth_raw),
                "length": len(oauth_raw),
                "starts_with": oauth_raw[:20] if oauth_raw else None,
            },
            "GOOGLE_CREDENTIALS_JSON": {
                "present": bool(creds_raw),
                "length": len(creds_raw),
            },
            "GOOGLE_DRIVE_PARENT_ID": os.getenv("GOOGLE_DRIVE_PARENT_ID") or None,
        },
        "drive_enabled": is_drive_enabled(),
        "client_initialized": False,
        "auth_mode": None,
        "service_account_email": None,
        "test_folder_creation": None,
        "test_file_upload": None,
        "errors": [],
    }

    # Test 1 : client initialisé
    try:
        client = get_drive_client()
        if client is None:
            result["errors"].append("get_drive_client() a retourné None")
            return result
        result["client_initialized"] = True
        result["auth_mode"] = get_auth_mode()  # "oauth" ou "service_account"
    except Exception as e:
        result["errors"].append(f"Erreur init client: {type(e).__name__}: {e}")
        return result

    # Test 2 : récup email du service account
    try:
        creds_raw = os.getenv("GOOGLE_CREDENTIALS_JSON", "{}")
        creds_dict = json.loads(creds_raw) if creds_raw.startswith("{") else {}
        result["service_account_email"] = creds_dict.get("client_email")
    except Exception as e:
        result["errors"].append(f"Parse credentials: {e}")

    # Test 3 : création d'un dossier test
    try:
        test_folder_id = create_batch_folder("drive_test_diagnostic")
        if test_folder_id:
            result["test_folder_creation"] = {
                "success": True,
                "folder_id": test_folder_id,
                "folder_url": get_folder_link(test_folder_id),
            }
        else:
            result["test_folder_creation"] = {"success": False}
            result["errors"].append("create_batch_folder() a retourné None")
    except Exception as e:
        result["test_folder_creation"] = {"success": False, "error": str(e)}
        result["errors"].append(f"Création dossier: {type(e).__name__}: {e}")

    # Test 4 : upload d'un petit fichier texte test
    if result["test_folder_creation"] and result["test_folder_creation"].get("success"):
        try:
            test_file = OUTPUT_DIR / "drive_test_file.txt"
            test_file.write_text("Ceci est un fichier de test Drive")
            upload_result = upload_file(
                test_file,
                result["test_folder_creation"]["folder_id"],
                mime_type="text/plain",
            )
            if upload_result:
                result["test_file_upload"] = {
                    "success": True,
                    "file_id": upload_result.get("id"),
                    "file_url": upload_result.get("webViewLink"),
                }
            else:
                result["test_file_upload"] = {"success": False}
                result["errors"].append(
                    "upload_file() a retourné None. Cause probable : "
                    "le Service Account n'a pas de quota storage. "
                    "Solution : partager le dossier Drive parent avec l'email "
                    f"du service account ({result['service_account_email']}) "
                    "en tant qu'Editor, OU utiliser un Shared Drive."
                )
            test_file.unlink(missing_ok=True)
        except Exception as e:
            result["test_file_upload"] = {"success": False, "error": str(e)}
            result["errors"].append(f"Upload test: {type(e).__name__}: {e}")

    return result


def _sanitize_batch_name(name: str) -> str:
    """Nettoie un nom de batch pour qu'il soit safe dans Drive / FS."""
    name = name.strip()
    if not name:
        name = f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    name = re.sub(r"[^\w\s\-]", "_", name, flags=re.UNICODE)
    name = re.sub(r"\s+", "_", name)
    return name[:80]


async def _async_drive_upload(result: dict, folder_id: str, batch_id: str = None) -> None:
    """Upload un fichier vers Drive en arrière-plan. Modifie result in-place."""
    try:
        loop = asyncio.get_event_loop()
        # Utilise le pool dédié pour VRAIMENT paralléliser (pas bloquer le pool default)
        res = await loop.run_in_executor(
            _drive_executor, upload_file, Path(result["path"]), folder_id
        )
        if res:
            result["drive_url"] = res.get("webViewLink")
            result["drive_id"] = res.get("id")
            if batch_id:
                _update_progress(batch_id, uploaded_delta=1)
                logger.info(f"[{batch_id}] Drive upload done: {result.get('filename')}")
    except Exception as e:
        logger.warning(f"Drive upload échoué pour {result.get('filename')}: {e}")


@router.post("/process")
async def process_endpoint(
    files: List[UploadFile] = File(..., description="Une ou plusieurs vidéos sources"),
    batch_name: str = Form("", description="Nom du batch (sous-dossier Drive)"),
    copies_per_video: int = Form(1, ge=1, description="Nombre de variantes par vidéo"),
    concurrency: int = Form(4, ge=1, le=6, description="Processus ffmpeg parallèles"),
    upload_to_drive: bool = Form(True, description="Envoyer sur Google Drive"),
    device_choice: str = Form("mix_random", description="Type de device à simuler"),
    va_name: str = Form("", description="Nom du VA qui lance le batch"),
    team: str = Form("", description="Équipe du VA (geelark, instagram)"),
    custom_ranges: Optional[str] = Form(None),
    enabled_filters: Optional[str] = Form(None),
):
    """
    Upload une ou plusieurs vidéos, génère des variantes randomisées,
    et (optionnellement) uploade le tout sur Google Drive en pipeline parallèle :
    dès qu'une vidéo est encodée, elle part sur Drive pendant que ffmpeg
    continue à traiter les suivantes.
    """
    # -- Validations ---------------------------------------------------------
    if not files:
        raise HTTPException(status_code=400, detail="Aucun fichier fourni.")
    if len(files) > settings.MAX_FILES_PER_REQUEST:
        raise HTTPException(
            status_code=400,
            detail=f"Max {settings.MAX_FILES_PER_REQUEST} vidéos par requête.",
        )
    if copies_per_video > settings.MAX_COPIES_PER_REQUEST:
        raise HTTPException(
            status_code=400,
            detail=f"Max {settings.MAX_COPIES_PER_REQUEST} copies par vidéo.",
        )

    for f in files:
        ext = Path(f.filename or "").suffix.lower()
        if ext not in settings.ALLOWED_EXTENSIONS:
            raise HTTPException(
                status_code=400,
                detail=f"Extension {ext!r} non supportée ({f.filename}).",
            )

    # -- Parse JSON options --------------------------------------------------
    parsed_ranges = None
    parsed_filters = None
    try:
        if custom_ranges:
            raw = json.loads(custom_ranges)
            parsed_ranges = {k: tuple(v) for k, v in raw.items() if k in PARAM_RANGES}
        if enabled_filters:
            parsed_filters = [f for f in json.loads(enabled_filters) if f in PARAM_RANGES]
    except (json.JSONDecodeError, ValueError, TypeError) as e:
        raise HTTPException(status_code=400, detail=f"JSON invalide: {e}") from e

    # -- Préparation batch ---------------------------------------------------
    import time
    batch_start_time = time.time()
    va_slug = _sanitize_batch_name(va_name) if va_name else ""
    team_slug = _sanitize_batch_name(team) if team else ""
    base_slug = _sanitize_batch_name(batch_name)
    # Construit le nom complet : [VA_][Team_] nom_batch
    parts = [p for p in [va_slug, team_slug, base_slug] if p]
    batch_slug = "_".join(parts) if parts else "batch"
    job_id = uuid.uuid4().hex[:8]
    full_batch_id = f"{batch_slug}_{job_id}"

    drive_folder_id = None
    drive_folder_link = None
    if upload_to_drive and is_drive_enabled():
        drive_folder_id = create_batch_folder(batch_slug)
        if drive_folder_id:
            drive_folder_link = get_folder_link(drive_folder_id)
            logger.info(f"[{full_batch_id}] Drive folder: {drive_folder_link}")
            # Partage auto avec les emails des VA
            try:
                from app.services.discord_va_sync import get_all_va_emails
                from app.services.drive_service import share_folder_with_users
                va_emails = get_all_va_emails()
                if va_emails:
                    loop = asyncio.get_event_loop()
                    await loop.run_in_executor(
                        None, share_folder_with_users, drive_folder_id, va_emails, "reader"
                    )
            except Exception as e:
                logger.warning(f"Drive auto-share failed: {e}")

    # -- Sauvegarde des fichiers sources (EN PARALLÈLE) ----------------------
    src_paths: List[Path] = []
    max_bytes = settings.MAX_UPLOAD_MB * 1024 * 1024

    async def _save_one(idx: int, f: UploadFile) -> Path:
        """Sauvegarde un fichier source en streaming."""
        ext = Path(f.filename or "").suffix.lower()
        safe_orig = re.sub(r"[^\w\-.]", "_", Path(f.filename or f"src{idx}").stem)
        src_path = UPLOAD_DIR / f"{full_batch_id}_{idx:03d}_{safe_orig}{ext}"
        written = 0
        async with aiofiles.open(src_path, "wb") as out:
            while chunk := await f.read(1024 * 1024):
                written += len(chunk)
                if written > max_bytes:
                    await out.close()
                    src_path.unlink(missing_ok=True)
                    raise HTTPException(
                        status_code=413,
                        detail=f"Fichier trop volumineux ({f.filename}).",
                    )
                await out.write(chunk)
        return src_path

    try:
        # Parallélise l'écriture sur disque des fichiers sources (gain sur gros batchs)
        src_paths = await asyncio.gather(*[_save_one(i, f) for i, f in enumerate(files)])
        logger.info(f"[{full_batch_id}] {len(src_paths)} vidéo(s) uploadée(s)")
    except HTTPException:
        for p in src_paths:
            p.unlink(missing_ok=True)
        raise
    except Exception as e:
        logger.exception("Erreur upload")
        for p in src_paths:
            p.unlink(missing_ok=True)
        raise HTTPException(status_code=500, detail=f"Erreur upload: {e}") from e

    # -- PIPELINE : ffmpeg + Drive upload en parallèle ----------------------
    # Dès qu'une vidéo est encodée, on lance son upload Drive sans attendre
    # les autres. Gros gain de temps sur les gros batchs.
    all_results: List[dict] = []
    drive_upload_tasks: List[asyncio.Task] = []

    # Initialise le tracking du progrès Drive
    if drive_folder_id:
        total_expected = len(src_paths) * copies_per_video
        _update_progress(full_batch_id, total=total_expected)

    try:
        for src_idx, src in enumerate(src_paths):
            source_label = files[src_idx].filename or src.name
            per_video_job_id = f"{full_batch_id}_v{src_idx:03d}"

            # Callback : dès qu'une copie est finie, on lance son upload Drive
            def _on_copy_ready(r: dict, src_label=source_label, src_i=src_idx):
                r["source_file"] = src_label
                r["source_index"] = src_i + 1
                if r.get("success") and drive_folder_id:
                    task = asyncio.create_task(
                        _async_drive_upload(r, drive_folder_id, full_batch_id)
                    )
                    drive_upload_tasks.append(task)

            results = await process_video(
                source=src,
                copies=copies_per_video,
                job_id=per_video_job_id,
                concurrency=concurrency,
                custom_ranges=parsed_ranges,
                enabled_filters=parsed_filters,
                device_choice=device_choice,
                on_copy_done=_on_copy_ready,
            )
            all_results.extend(results)
    except RuntimeError as e:
        logger.error(f"[{full_batch_id}] {e}")
        raise HTTPException(status_code=500, detail=str(e)) from e
    except Exception as e:
        logger.exception(f"[{full_batch_id}] Erreur traitement")
        raise HTTPException(status_code=500, detail=f"Erreur traitement: {e}") from e
    finally:
        for p in src_paths:
            p.unlink(missing_ok=True)

    # Attendre que TOUS les uploads Drive en cours soient terminés
    if drive_upload_tasks:
        logger.info(f"[{full_batch_id}] Finalisation {len(drive_upload_tasks)} uploads Drive")
        await asyncio.gather(*drive_upload_tasks, return_exceptions=True)

    # Marque le batch comme terminé pour le polling
    if drive_folder_id:
        _update_progress(full_batch_id, done=True)

    success = [r for r in all_results if r.get("success")]
    failed = [r for r in all_results if not r.get("success")]
    retries_used = sum(1 for r in success if r.get("was_retried"))

    # Upload CSV de métadonnées (après tout le reste)
    drive_uploads_count = sum(1 for r in success if r.get("drive_url"))
    if drive_folder_id and success:
        csv_rows = []
        for r in all_results:
            row = {
                "va_name": va_name or "",
                "source_file": r.get("source_file"),
                "copy_index": r.get("copy_index"),
                "success": r.get("success"),
                "attempt": r.get("attempt", 1),
                "was_retried": r.get("was_retried", False),
                "output_filename": r.get("filename", ""),
                "drive_url": r.get("drive_url", ""),
                "size_bytes": r.get("size_bytes", ""),
                "error": (r.get("error", "") or "")[:300],
            }
            params = r.get("params") or {}
            for k, v in params.items():
                row[f"param_{k}"] = v if v is not None else ""
            csv_rows.append(row)

        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            None, upload_csv, drive_folder_id, csv_rows, "metadata.csv"
        )

    # Partage le dossier Drive avec l'email du VA + DM privé
    if drive_folder_id and va_name:
        try:
            from app.services.discord_va_sync import find_va_by_discord_id, find_va_discord_id
            from app.services.drive_service import share_folder_with_users

            va_did = find_va_discord_id(va_name)
            va_info = find_va_by_discord_id(va_did) if va_did else None
            va_email = va_info.get("email") if va_info else None

            if va_email:
                loop = asyncio.get_event_loop()
                # role="writer" = download + modify/delete autorisés
                share_result = await loop.run_in_executor(
                    None,
                    share_folder_with_users,
                    drive_folder_id,
                    [va_email],
                    "writer",
                )
                logger.info(f"[{full_batch_id}] Drive partagé avec {va_email}: {share_result}")

                # DM privé au VA
                try:
                    from app.services.discord_bot import notify_va_drive_ready
                    await notify_va_drive_ready(va_did, drive_folder_link or "")
                except Exception as e:
                    logger.warning(f"DM VA Drive ready échoué: {e}")
            else:
                logger.info(
                    f"[{full_batch_id}] Pas d'email enregistré pour {va_name}, pas de partage auto"
                )
        except Exception as e:
            logger.warning(f"Erreur partage Drive avec VA: {e}")

    # Notif Discord (non bloquante)
    duration = time.time() - batch_start_time
    try:
        # Récupère l'ID Discord du VA pour le mentionner
        from app.services.discord_va_sync import find_va_discord_id
        va_discord_id = find_va_discord_id(va_name) if va_name else ""
        await send_batch_notification(
            va_name=va_name,
            va_discord_id=va_discord_id or "",
            batch_name=batch_slug,
            total_requested=len(files) * copies_per_video,
            succeeded=len(success),
            failed=len(failed),
            drive_uploaded=drive_uploads_count,
            retries_used=retries_used,
            duration_seconds=duration,
            device_choice=device_choice,
            drive_folder_url=drive_folder_link,
        )
    except Exception as e:
        logger.warning(f"Discord notif failed: {e}")

    return JSONResponse(
        content={
            "batch_id": full_batch_id,
            "batch_name": batch_slug,
            "va_name": va_name,
            "sources_count": len(files),
            "copies_per_video": copies_per_video,
            "total_requested": len(files) * copies_per_video,
            "succeeded": len(success),
            "failed": len(failed),
            "retries_used": retries_used,
            "duration_seconds": round(duration, 1),
            "drive": {
                "enabled": bool(drive_folder_id),
                "folder_id": drive_folder_id,
                "folder_url": drive_folder_link,
                "uploaded": drive_uploads_count,
            },
            "results": all_results,
            "download_base_url": "/api/download/",
        },
    )


@router.get("/download/{filename}")
async def download(filename: str):
    """Sert un fichier généré."""
    if "/" in filename or "\\" in filename or ".." in filename:
        raise HTTPException(status_code=400, detail="Nom de fichier invalide.")
    path = OUTPUT_DIR / filename
    if not path.exists() or not path.is_file():
        raise HTTPException(status_code=404, detail="Fichier introuvable.")
    return FileResponse(path, media_type="video/mp4", filename=filename)


@router.get("/outputs")
async def list_outputs():
    """Liste les vidéos disponibles dans /outputs."""
    files = []
    for p in sorted(OUTPUT_DIR.glob("*.mp4"), key=lambda x: x.stat().st_mtime, reverse=True):
        files.append({
            "filename": p.name,
            "size_bytes": p.stat().st_size,
            "url": f"/api/download/{p.name}",
        })
    return {"count": len(files), "files": files}
