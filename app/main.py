"""Point d'entrée FastAPI du repurpose bot."""
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

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


@app.on_event("startup")
async def on_startup():
    logger.info(f"{settings.APP_NAME} v{settings.VERSION} démarré.")


@app.get("/")
async def root():
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
