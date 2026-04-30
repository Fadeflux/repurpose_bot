"""
Service Geelark API — gestion des cloud phones et upload de vidéos.

Authentification : signature SHA256 (Key verification, plus stable que Bearer token).
Doc : https://open.geelark.com/api/cloud-phone-request-instructions

Variables d'environnement requises :
  GEELARK_APP_ID    : Team App ID (visible dans le dashboard Geelark > API)
  GEELARK_API_KEY   : Team API Key (visible dans le dashboard Geelark > API)

Rate limit : 200 req/min, 24000 req/h (au-delà → blocage 2h).
"""
import asyncio
import hashlib
import os
import random
import time
import uuid
from typing import Any, Dict, List, Optional

import aiohttp

from app.utils.logger import get_logger

logger = get_logger("geelark")

BASE_URL = "https://openapi.geelark.com/open/v1"

# Statuts cloud phone (cf doc Get all cloud phones)
PHONE_STATUS_STARTED = 0
PHONE_STATUS_STARTING = 1
PHONE_STATUS_SHUTDOWN = 2


# ---------------------------------------------------------------------------
# Authentification
# ---------------------------------------------------------------------------
def is_geelark_enabled() -> bool:
    """Retourne True si Geelark est configuré côté env."""
    return bool(os.getenv("GEELARK_APP_ID") and os.getenv("GEELARK_API_KEY"))


def _build_headers() -> Dict[str, str]:
    """
    Construit les headers d'auth pour chaque requête Geelark.
    sign = SHA256(appId + traceId + ts + nonce + apiKey).upper()
    """
    app_id = os.getenv("GEELARK_APP_ID", "")
    api_key = os.getenv("GEELARK_API_KEY", "")

    if not app_id or not api_key:
        raise RuntimeError("GEELARK_APP_ID et GEELARK_API_KEY doivent être configurés")

    trace_id = str(uuid.uuid4())
    nonce = trace_id[:6]  # 6 premiers chars du traceId (cf doc)
    ts = str(int(time.time() * 1000))

    sign_str = f"{app_id}{trace_id}{ts}{nonce}{api_key}"
    sign = hashlib.sha256(sign_str.encode()).hexdigest().upper()

    return {
        "Content-Type": "application/json",
        "appId": app_id,
        "traceId": trace_id,
        "ts": ts,
        "nonce": nonce,
        "sign": sign,
    }


# ---------------------------------------------------------------------------
# Helper requête générique
# ---------------------------------------------------------------------------
async def _post(
    endpoint: str,
    payload: Optional[Dict[str, Any]] = None,
    timeout: int = 30,
) -> Dict[str, Any]:
    """
    POST vers l'API Geelark avec retry simple.
    Retourne le champ 'data' de la réponse, ou raise RuntimeError.
    """
    url = f"{BASE_URL}{endpoint}"
    payload = payload or {}

    last_err: Optional[Exception] = None
    for attempt in range(3):
        try:
            headers = _build_headers()
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    url, json=payload, headers=headers, timeout=timeout
                ) as resp:
                    body = await resp.json()
                    code = body.get("code")
                    if code == 0:
                        return body.get("data", {}) or {}
                    msg = body.get("msg", "unknown")
                    raise RuntimeError(f"Geelark API error code={code} msg={msg}")
        except Exception as e:
            last_err = e
            if attempt < 2:
                await asyncio.sleep(0.5 * (attempt + 1))
                continue
            break
    raise RuntimeError(f"Geelark POST {endpoint} failed: {last_err}")


# ---------------------------------------------------------------------------
# Cloud Phone Management
# ---------------------------------------------------------------------------
async def list_phones_by_group(
    group_name: str, page_size: int = 100
) -> List[Dict[str, Any]]:
    """
    Retourne tous les phones d'un groupe Geelark (paginé automatiquement).
    group_name : nom exact du groupe Geelark (ex: 'Ghost 👻').
    """
    all_phones: List[Dict[str, Any]] = []
    page = 1
    while True:
        data = await _post(
            "/phone/list",
            {"page": page, "pageSize": page_size, "groupName": group_name},
        )
        items = data.get("items", []) or []
        all_phones.extend(items)
        total = data.get("total", 0)
        if len(all_phones) >= total or not items:
            break
        page += 1
    logger.info(f"Phones trouvés pour groupe '{group_name}' : {len(all_phones)}")
    return all_phones


async def list_groups() -> List[str]:
    """
    Retourne la liste des noms de groupes uniques en parcourant tous les phones.
    Utile pour valider que les noms de groupes Discord matchent côté Geelark.
    """
    data = await _post("/phone/list", {"page": 1, "pageSize": 100})
    items = data.get("items", []) or []
    groups = set()
    for ph in items:
        grp = ph.get("group") or {}
        name = grp.get("name") if isinstance(grp, dict) else None
        if name:
            groups.add(name)
    return sorted(groups)


async def start_phone(phone_id: str) -> bool:
    """Démarre un phone. Idempotent : ne fait rien s'il est déjà démarré."""
    try:
        await _post("/phone/start", {"ids": [phone_id]})
        return True
    except Exception as e:
        logger.warning(f"start_phone {phone_id} failed: {e}")
        return False


async def stop_phone(phone_id: str) -> bool:
    """Éteint un phone."""
    try:
        await _post("/phone/stop", {"ids": [phone_id]})
        return True
    except Exception as e:
        logger.warning(f"stop_phone {phone_id} failed: {e}")
        return False


async def start_phones(phone_ids: List[str]) -> int:
    """Démarre plusieurs phones en parallèle. Retourne le nb de succès."""
    results = await asyncio.gather(
        *[start_phone(pid) for pid in phone_ids], return_exceptions=True
    )
    return sum(1 for r in results if r is True)


async def stop_phones(phone_ids: List[str]) -> int:
    """Éteint plusieurs phones en parallèle. Retourne le nb de succès."""
    results = await asyncio.gather(
        *[stop_phone(pid) for pid in phone_ids], return_exceptions=True
    )
    return sum(1 for r in results if r is True)


async def wait_phones_ready(
    phone_ids: List[str], timeout_s: int = 120, poll_interval_s: int = 5
) -> List[str]:
    """
    Attend que les phones soient en status STARTED (0).
    Retourne la liste des phone_ids prêts (ceux pas prêts au timeout sont skip).
    """
    deadline = time.time() + timeout_s
    ready: set = set()
    pending = set(phone_ids)

    while pending and time.time() < deadline:
        try:
            data = await _post(
                "/phone/list", {"page": 1, "pageSize": 100, "ids": list(pending)}
            )
            items = data.get("items", []) or []
            for ph in items:
                if ph.get("status") == PHONE_STATUS_STARTED:
                    ready.add(ph["id"])
            pending -= ready
            if not pending:
                break
        except Exception as e:
            logger.warning(f"wait_phones_ready check failed: {e}")
        await asyncio.sleep(poll_interval_s)

    if pending:
        logger.warning(f"{len(pending)} phones pas prêts après {timeout_s}s : {pending}")
    return list(ready)


# ---------------------------------------------------------------------------
# Upload de fichiers
# ---------------------------------------------------------------------------
async def _get_upload_url(file_type: str = "mp4") -> Dict[str, str]:
    """Étape 1 : récupère uploadUrl + resourceUrl pour un fichier temporaire."""
    return await _post("/upload/getUrl", {"fileType": file_type})


async def _put_file_to_url(
    upload_url: str, file_path: str, content_type: str = "application/octet-stream"
) -> bool:
    """Étape 2 : PUT le fichier sur l'uploadUrl signée."""
    try:
        with open(file_path, "rb") as f:
            data = f.read()
        headers = {"Content-Type": content_type}
        async with aiohttp.ClientSession() as session:
            async with session.put(
                upload_url, data=data, headers=headers, timeout=300
            ) as resp:
                if resp.status not in (200, 204):
                    txt = await resp.text()
                    # Log COMPLET du body XML pour diagnostiquer (S3/OSS error codes)
                    logger.warning(
                        f"PUT upload failed status={resp.status} ct={content_type} "
                        f"size={len(data)} url_host={upload_url[:80]}..."
                    )
                    logger.warning(f"PUT response body FULL: {txt}")
                    logger.warning(f"PUT response headers: {dict(resp.headers)}")
                    return False
        return True
    except Exception as e:
        logger.warning(f"_put_file_to_url exception: {e}")
        return False


async def upload_temp_file(file_path: str, file_type: str = "mp4") -> Optional[str]:
    """
    Upload un fichier vers le storage temporaire Geelark (3 jours de validité).
    Retourne le resourceUrl (à utiliser ensuite pour pousser sur les phones).
    """
    # Map fileType -> MIME type pour respecter le Content-Type de la signature S3
    mime_map = {
        "mp4": "video/mp4",
        "webm": "video/webm",
        "jpg": "image/jpeg",
        "jpeg": "image/jpeg",
        "png": "image/png",
        "gif": "image/gif",
        "bmp": "image/bmp",
        "webp": "image/webp",
        "heic": "image/heic",
        "xml": "application/xml",
        "apk": "application/vnd.android.package-archive",
        "xapk": "application/vnd.android.package-archive",
    }
    content_type = mime_map.get(file_type.lower(), "application/octet-stream")

    try:
        urls = await _get_upload_url(file_type=file_type)
        upload_url = urls.get("uploadUrl")
        resource_url = urls.get("resourceUrl")
        if not upload_url or not resource_url:
            logger.error(f"upload/getUrl response sans uploadUrl/resourceUrl : {urls}")
            return None

        ok = await _put_file_to_url(upload_url, file_path, content_type=content_type)
        if not ok:
            return None
        logger.info(f"Fichier upload OK sur Geelark : {file_path} → {resource_url}")
        return resource_url
    except Exception as e:
        logger.exception(f"upload_temp_file failed: {e}")
        return None


async def push_file_to_phone(phone_id: str, resource_url: str) -> Optional[str]:
    """
    Push un fichier (déjà uploadé via upload_temp_file) sur un cloud phone.
    Le fichier arrive dans le dossier Downloads du phone.
    Retourne le taskId, ou None si fail.
    """
    try:
        data = await _post(
            "/phone/uploadFile", {"id": phone_id, "fileUrl": resource_url}
        )
        return data.get("taskId")
    except Exception as e:
        logger.warning(f"push_file_to_phone {phone_id} failed: {e}")
        return None


# ---------------------------------------------------------------------------
# Distribution intelligente
# ---------------------------------------------------------------------------
def distribute_videos(
    video_resource_urls: List[str],
    phone_ids: List[str],
    mode: str = "round_robin",
) -> Dict[str, List[str]]:
    """
    Décide quelle vidéo va sur quel phone selon le mode choisi.

    mode = 'round_robin' : 1 vidéo par phone, en boucle si plus de phones que de vidéos
    mode = 'random'      : chaque vidéo va sur un phone aléatoire (1 vidéo = 1 phone)
    mode = 'all_to_all'  : chaque phone reçoit toutes les vidéos (mode pool)

    Retourne un dict {phone_id: [resource_url, ...]}.
    """
    distribution: Dict[str, List[str]] = {pid: [] for pid in phone_ids}
    if not phone_ids or not video_resource_urls:
        return distribution

    if mode == "all_to_all":
        for pid in phone_ids:
            distribution[pid] = list(video_resource_urls)
    elif mode == "random":
        for url in video_resource_urls:
            pid = random.choice(phone_ids)
            distribution[pid].append(url)
    else:  # round_robin par défaut
        for i, url in enumerate(video_resource_urls):
            pid = phone_ids[i % len(phone_ids)]
            distribution[pid].append(url)

    return distribution
