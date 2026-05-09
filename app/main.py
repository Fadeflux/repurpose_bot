"""Point d'entrée FastAPI du repurpose bot."""
from pathlib import Path

from fastapi import FastAPI, Form, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles

from app.config import settings
from app.routes.video import router as video_router
from app.routes.cf_templates import router as cf_templates_router
from app.routes.cf_content import router as cf_content_router
from app.routes.cf_music import router as cf_music_router
from app.routes.cf_extractor import router as cf_extractor_router
from app.routes.cf_mixer import router as cf_mixer_router
from app.routes.cf_vas import router as cf_vas_router
from app.services.auth import (
    check_password,
    is_authenticated,
    is_auth_enabled,
    make_login_response,
    make_logout_response,
    render_login_page,
)
from app.utils.logger import get_logger

logger = get_logger("main")

app = FastAPI(
    title=settings.APP_NAME,
    version=settings.VERSION,
    description="API de repurposing vidéo (TikFusion-like) avec FFmpeg.",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(video_router)

# === ClipFusion routes (préfixe /api/clipfusion/...) ===
app.include_router(cf_templates_router)
app.include_router(cf_content_router)
app.include_router(cf_music_router)
app.include_router(cf_extractor_router)
app.include_router(cf_mixer_router)
app.include_router(cf_vas_router)

# Sert les fichiers statiques (CSS, JS, images si besoin)
STATIC_DIR = Path(__file__).resolve().parent / "static"
if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


@app.on_event("startup")
async def on_startup():
    logger.info(f"{settings.APP_NAME} v{settings.VERSION} démarré.")
    if is_auth_enabled():
        logger.info("Login activé (TOOL_PASSWORD configuré)")
    else:
        logger.warning("Login désactivé (TOOL_PASSWORD non défini) - accès libre au tools")
    # Initialise la DB Postgres pour les emails (table créée si absente)
    try:
        from app.services.va_emails_db import init_schema, is_db_enabled
        if is_db_enabled():
            init_schema()
            logger.info("Postgres pour emails VA : OK")
        else:
            logger.warning("DATABASE_URL non défini, emails VA stockés uniquement en cache éphémère")
    except Exception as e:
        logger.warning(f"Init Postgres échoué: {e}")
    # Initialise la DB Postgres pour ClipFusion (4 tables cf_*)
    try:
        from app.services.cf_storage import init_schema as cf_init_schema, is_db_enabled as cf_db_enabled
        if cf_db_enabled():
            cf_init_schema()
            logger.info("Postgres pour ClipFusion : OK")
        else:
            logger.warning("ClipFusion : DATABASE_URL absent, fonctionnalités persistantes désactivées")
    except Exception as e:
        logger.warning(f"Init ClipFusion DB échoué: {e}")
    # Lance la sync automatique des VA Discord en arrière-plan
    try:
        from app.services.discord_va_sync import start_periodic_sync
        start_periodic_sync()
    except Exception as e:
        logger.warning(f"Impossible de démarrer la sync VA Discord: {e}")
    # Lance le bot Discord Gateway (onboarding emails)
    try:
        from app.services.discord_bot import start_discord_bot
        start_discord_bot()
    except Exception as e:
        logger.warning(f"Impossible de démarrer le bot Discord Gateway: {e}")
    # Lance le job de nettoyage Drive (suppression des batches anciens)
    try:
        from app.services.drive_cleanup import start_periodic_cleanup
        start_periodic_cleanup()
    except Exception as e:
        logger.warning(f"Impossible de démarrer le cleanup Drive: {e}")


# =============================================================================
# Routes auth
# =============================================================================
@app.get("/login")
async def login_page(request: Request):
    """Affiche la page de login."""
    if not is_auth_enabled():
        return RedirectResponse(url="/", status_code=302)
    if is_authenticated(request):
        return RedirectResponse(url="/", status_code=302)
    return render_login_page()


@app.post("/login")
async def login_submit(password: str = Form(...)):
    """Vérifie le mot de passe et crée la session."""
    if not is_auth_enabled():
        return RedirectResponse(url="/", status_code=302)
    if check_password(password):
        return make_login_response("/")
    return render_login_page(error="❌ Mot de passe incorrect")


@app.get("/logout")
async def logout():
    """Déconnecte l'utilisateur."""
    return make_logout_response()


# =============================================================================
# Page principale (protégée si auth activée)
# =============================================================================
@app.get("/clipfusion")
async def clipfusion_page(request: Request):
    """Sert l'interface ClipFusion (étapes Extracteur → Templates → Mix)."""
    if is_auth_enabled() and not is_authenticated(request):
        return RedirectResponse(url="/login", status_code=302)
    cf_index = STATIC_DIR / "clipfusion" / "index.html"
    if cf_index.exists():
        return FileResponse(cf_index)
    return {"error": "ClipFusion frontend not found", "expected_at": str(cf_index)}


@app.get("/")
async def root(request: Request):
    """Sert l'interface HTML, ou redirige vers /login si pas authentifié."""
    if is_auth_enabled() and not is_authenticated(request):
        return RedirectResponse(url="/login", status_code=302)
    index = STATIC_DIR / "index.html"
    if index.exists():
        return FileResponse(index)
    return {
        "app": settings.APP_NAME,
        "version": settings.VERSION,
        "docs": "/docs",
    }


@app.get("/api")
async def api_info():
    return {
        "app": settings.APP_NAME,
        "version": settings.VERSION,
        "docs": "/docs",
        "endpoints": {
            "POST /api/process": "upload + génération de N copies randomisées",
            "GET /api/params": "bornes des paramètres",
            "GET /api/outputs": "liste des vidéos générées",
            "GET /api/download/{filename}": "téléchargement d'une vidéo",
            "GET /api/health": "healthcheck",
        },
    }
