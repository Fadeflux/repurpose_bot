"""Point d'entrée FastAPI du repurpose bot."""
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from app.config import settings
from app.routes.video import router as video_router
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

# Sert les fichiers statiques (CSS, JS, images si besoin)
STATIC_DIR = Path(__file__).resolve().parent / "static"
if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


@app.on_event("startup")
async def on_startup():
    logger.info(f"{settings.APP_NAME} v{settings.VERSION} démarré.")
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


@app.get("/")
async def root():
    """Sert l'interface HTML."""
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
