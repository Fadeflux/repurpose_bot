"""
Webhook endpoints pour intégration cross-bot avec Lola (shinra-discord-bot).

Quand Lola ban un VA (commande /banva), elle ping ces endpoints pour
notifier repurpose_bot. Évite que des batchs continuent à tourner sur
les comptes d'un VA banni.

Auth : header `X-Lola-Secret: <CF_LOLA_WEBHOOK_SECRET>` (shared secret).
Sans cet env var côté Railway, les endpoints sont DÉSACTIVÉS (return 503).
"""
import os
from typing import Any, Dict

from fastapi import APIRouter, Header, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from app.services import cf_storage
from app.utils.logger import get_logger

logger = get_logger("cf_lola_webhook")

router = APIRouter(prefix="/api/lola", tags=["lola-webhook"])


def _check_auth(provided_secret: str) -> bool:
    """Vérifie le header X-Lola-Secret contre CF_LOLA_WEBHOOK_SECRET."""
    expected = os.environ.get("CF_LOLA_WEBHOOK_SECRET", "").strip()
    if not expected:
        return False  # secret pas configuré → endpoint désactivé
    if not provided_secret:
        return False
    # Comparaison constant-time pour pas leaker le secret via timing
    import hmac
    return hmac.compare_digest(expected, provided_secret)


class VaBannedPayload(BaseModel):
    discord_id: str
    reason: str = ""


@router.post("/va_banned")
async def va_banned(
    payload: VaBannedPayload,
    x_lola_secret: str = Header(default=""),
) -> Dict[str, Any]:
    """
    Lola signale que ce VA a été ban → on archive tous ses comptes.
    Les comptes archivés ne peuvent plus être utilisés via /request.

    Body :
      {
        "discord_id": "1234567890",
        "reason": "VA inactif 30j" (optionnel)
      }

    Headers :
      X-Lola-Secret: <CF_LOLA_WEBHOOK_SECRET>
    """
    if not _check_auth(x_lola_secret):
        raise HTTPException(
            status_code=401,
            detail="Authentification webhook échouée (CF_LOLA_WEBHOOK_SECRET manquant ou incorrect)",
        )
    if not payload.discord_id.isdigit():
        raise HTTPException(
            status_code=400,
            detail="discord_id doit être un ID Discord numérique",
        )
    result = cf_storage.archive_accounts_for_va(
        va_discord_id=payload.discord_id,
        reason=payload.reason or "Lola ban (auto)",
    )
    logger.info(
        f"Webhook Lola va_banned : discord_id={payload.discord_id} "
        f"→ {result.get('archived', 0)} comptes archivés"
    )
    return JSONResponse(content=result)


@router.get("/ping")
async def ping(x_lola_secret: str = Header(default="")) -> Dict[str, str]:
    """
    Endpoint de test pour vérifier que le secret est bien configuré
    des deux côtés. Lola peut hit ça au démarrage pour valider la
    connexion.
    """
    if not _check_auth(x_lola_secret):
        raise HTTPException(status_code=401, detail="Auth failed")
    return {"status": "ok", "service": "repurpose_bot"}
