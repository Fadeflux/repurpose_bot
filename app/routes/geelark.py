"""
Routes de test Geelark — à utiliser pour valider la connexion avant
d'intégrer Geelark dans le flow de batch.
"""
import asyncio
from pathlib import Path
from typing import List, Optional

from fastapi import APIRouter, File, Form, HTTPException, Query, UploadFile

from app.services import geelark_service
from app.utils.logger import get_logger

logger = get_logger("geelark_routes")

router = APIRouter(prefix="/api/geelark", tags=["geelark"])

UPLOAD_TMP_DIR = Path("/tmp/geelark_uploads")
UPLOAD_TMP_DIR.mkdir(parents=True, exist_ok=True)


@router.get("/test")
async def test_connection():
    """
    Test la connexion à l'API Geelark.
    Liste les groupes existants pour valider auth + accès.
    """
    if not geelark_service.is_geelark_enabled():
        raise HTTPException(
            status_code=400,
            detail="Geelark non configuré. Définis GEELARK_APP_ID et GEELARK_API_KEY.",
        )
    try:
        groups = await geelark_service.list_groups()
        return {"ok": True, "groups": groups, "count": len(groups)}
    except Exception as e:
        logger.exception("Geelark test failed")
        raise HTTPException(status_code=500, detail=f"Geelark test failed: {e}") from e


@router.get("/phones")
async def list_phones(group: str = Query(..., description="Nom du groupe Geelark")):
    """
    Liste les phones d'un groupe précis.
    Utile pour vérifier que les noms de groupes Discord matchent côté Geelark.
    """
    if not geelark_service.is_geelark_enabled():
        raise HTTPException(status_code=400, detail="Geelark non configuré.")
    try:
        phones = await geelark_service.list_phones_by_group(group)
        # Renvoie une vue simplifiée pour le debug
        simplified = [
            {
                "id": p.get("id"),
                "serialName": p.get("serialName"),
                "status": p.get("status"),
                "group": (p.get("group") or {}).get("name"),
            }
            for p in phones
        ]
        return {"ok": True, "group": group, "count": len(simplified), "phones": simplified}
    except Exception as e:
        logger.exception("Geelark list_phones failed")
        raise HTTPException(status_code=500, detail=str(e)) from e


# ---------------------------------------------------------------------------
# Endpoint de test : push 1 vidéo sur 1 phone précis
# ---------------------------------------------------------------------------
@router.post("/push-test")
async def push_test(
    file: UploadFile = File(..., description="Fichier vidéo à push"),
    phone_id: str = Form(..., description="ID du cloud phone cible"),
    auto_start: bool = Form(True, description="Démarre le phone si éteint"),
    auto_stop: bool = Form(False, description="Éteint le phone après upload"),
):
    """
    Push UN fichier sur UN phone précis. Sert à valider tout le pipeline
    avant de tester le push de batch complet.
    """
    if not geelark_service.is_geelark_enabled():
        raise HTTPException(status_code=400, detail="Geelark non configuré.")

    # Sauvegarde le fichier en temp
    tmp_path = UPLOAD_TMP_DIR / f"test_{file.filename}"
    try:
        with open(tmp_path, "wb") as f:
            f.write(await file.read())

        # 1. Démarre le phone si demandé
        if auto_start:
            logger.info(f"Démarrage phone {phone_id}...")
            await geelark_service.start_phone(phone_id)
            ready = await geelark_service.wait_phones_ready([phone_id], timeout_s=120)
            if phone_id not in ready:
                raise HTTPException(
                    status_code=500,
                    detail=f"Phone {phone_id} pas démarré après 2 min",
                )

        # 2. Upload vers Geelark storage
        resource_url = await geelark_service.upload_temp_file(str(tmp_path))
        if not resource_url:
            raise HTTPException(status_code=500, detail="Upload temp Geelark échoué")

        # 3. Push sur le phone
        task_id = await geelark_service.push_file_to_phone(phone_id, resource_url)
        if not task_id:
            raise HTTPException(status_code=500, detail="Push sur phone échoué")

        # 4. Stop si demandé
        if auto_stop:
            await geelark_service.stop_phone(phone_id)

        return {
            "ok": True,
            "phone_id": phone_id,
            "task_id": task_id,
            "resource_url": resource_url,
            "filename": file.filename,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("push_test failed")
        raise HTTPException(status_code=500, detail=str(e)) from e
    finally:
        tmp_path.unlink(missing_ok=True)


# ---------------------------------------------------------------------------
# Endpoint principal : push d'un batch de vidéos vers un VA
# ---------------------------------------------------------------------------
@router.post("/push-batch")
async def push_batch(
    files: List[UploadFile] = File(..., description="Vidéos à distribuer"),
    group_name: str = Form(..., description="Nom du groupe Geelark (= VA)"),
    mode: str = Form(
        "round_robin",
        description="round_robin | random | all_to_all",
    ),
    auto_stop: bool = Form(True, description="Éteint les phones après upload"),
):
    """
    Push un batch de vidéos sur tous les phones d'un VA.

    Workflow :
    1. Liste les phones du groupe
    2. Démarre tous les phones du groupe
    3. Attend qu'ils soient prêts (status=0)
    4. Upload chaque vidéo vers le storage Geelark
    5. Distribue les vidéos selon le mode choisi
    6. Push chaque (vidéo, phone) en parallèle (concurrence limitée)
    7. Éteint tous les phones si auto_stop=True
    """
    if not geelark_service.is_geelark_enabled():
        raise HTTPException(status_code=400, detail="Geelark non configuré.")

    if mode not in ("round_robin", "random", "all_to_all"):
        raise HTTPException(status_code=400, detail=f"mode '{mode}' invalide")

    # 1. Liste les phones du groupe
    phones = await geelark_service.list_phones_by_group(group_name)
    if not phones:
        raise HTTPException(
            status_code=404,
            detail=f"Aucun phone trouvé pour le groupe '{group_name}'",
        )
    phone_ids = [p["id"] for p in phones]
    logger.info(f"[{group_name}] {len(phone_ids)} phones trouvés")

    # 2. Sauvegarde des fichiers en local temp
    saved_paths: List[Path] = []
    try:
        for f in files:
            p = UPLOAD_TMP_DIR / f.filename
            with open(p, "wb") as out:
                out.write(await f.read())
            saved_paths.append(p)
        logger.info(f"[{group_name}] {len(saved_paths)} fichiers sauvegardés en local")

        # 3. Démarre les phones (obligatoire : "env not running" sinon)
        logger.info(f"[{group_name}] Démarrage de {len(phone_ids)} phones...")
        start_result = await geelark_service.start_phones(phone_ids)
        active_phones = start_result["success_ids"]
        if not active_phones:
            details = "; ".join(
                f"{f['id']}: code={f['code']} {f['msg']}"
                for f in start_result["failures"][:3]
            )
            raise HTTPException(
                status_code=500,
                detail=f"Aucun phone démarré. Erreurs : {details}",
            )

        # On attend que les phones soient vraiment "Started" (status=0)
        ready = await geelark_service.wait_phones_ready(active_phones, timeout_s=180)
        if not ready:
            raise HTTPException(
                status_code=500, detail="Phones démarrés mais pas prêts après 3 min"
            )
        active_phones = ready
        logger.info(
            f"[{group_name}] {len(active_phones)} phones prêts "
            f"({len(start_result['failures'])} échecs au start, "
            f"{len(start_result['success_ids']) - len(active_phones)} jamais ready)"
        )

        # 4. Upload des vidéos vers le storage Geelark (parallèle, max 4 à la fois)
        upload_sem = asyncio.Semaphore(4)

        async def _upload_one(path: Path) -> Optional[str]:
            async with upload_sem:
                return await geelark_service.upload_temp_file(str(path))

        resource_urls = await asyncio.gather(
            *[_upload_one(p) for p in saved_paths]
        )
        resource_urls = [u for u in resource_urls if u]
        if not resource_urls:
            raise HTTPException(
                status_code=500, detail="Aucune vidéo uploadée sur Geelark"
            )
        logger.info(
            f"[{group_name}] {len(resource_urls)}/{len(saved_paths)} vidéos uploadées"
        )

        # 5. Distribue selon le mode
        distribution = geelark_service.distribute_videos(
            resource_urls, active_phones, mode=mode
        )

        # 6. Push chaque (phone, vidéo) en parallèle (max 8 à la fois)
        push_sem = asyncio.Semaphore(8)
        results = []

        async def _push_one(pid: str, url: str):
            async with push_sem:
                tid = await geelark_service.push_file_to_phone(pid, url)
                results.append({"phone_id": pid, "resource_url": url, "task_id": tid})

        push_tasks = []
        for pid, urls in distribution.items():
            for url in urls:
                push_tasks.append(_push_one(pid, url))
        await asyncio.gather(*push_tasks, return_exceptions=True)

        success = [r for r in results if r.get("task_id")]
        failed = [r for r in results if not r.get("task_id")]
        logger.info(
            f"[{group_name}] Push terminé : {len(success)} OK / {len(failed)} fail"
        )

        # 7. Éteint les phones après push si demandé (pour économiser les minutes)
        if auto_stop:
            logger.info(f"[{group_name}] Extinction de {len(active_phones)} phones...")
            await geelark_service.stop_phones(active_phones)

        return {
            "ok": True,
            "group": group_name,
            "mode": mode,
            "phones_total": len(phone_ids),
            "phones_active": len(active_phones),
            "videos_uploaded": len(resource_urls),
            "push_success": len(success),
            "push_failed": len(failed),
            "auto_stop": auto_stop,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("push_batch failed")
        raise HTTPException(status_code=500, detail=str(e)) from e
    finally:
        for p in saved_paths:
            p.unlink(missing_ok=True)
