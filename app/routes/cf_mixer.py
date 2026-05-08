"""
ClipFusion — Mixer : combine templates + vidéos + musique en outputs.
Routes montées sous /api/clipfusion/mixer/...
"""
import json
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Body, HTTPException, Query
from fastapi.responses import FileResponse, StreamingResponse

from app.services import cf_storage as storage
from app.services import cf_mixer as mixer_service
from app.utils.logger import get_logger

logger = get_logger("cf_mixer_route")

router = APIRouter(prefix="/api/clipfusion/mixer", tags=["clipfusion-mixer"])

# Le mixer écrit les outputs dans /tmp/clipfusion/output (cf cf_mixer.OUTPUT_DIR)
OUTPUT_DIR = Path("/tmp/clipfusion/output")


def _selected_templates() -> List[Dict[str, Any]]:
    """Garde uniquement les templates avec is_selected=True."""
    return [t for t in storage.list_templates() if t.get("is_selected", True)]


# Sert les vidéos générées (preview avant upload Drive)
@router.get("/output/{filename}")
async def serve_output(filename: str):
    """Sert un fichier de sortie pour preview dans l'UI."""
    safe_name = Path(filename).name
    fpath = OUTPUT_DIR / safe_name
    if not fpath.exists():
        raise HTTPException(404, "Fichier introuvable")
    return FileResponse(fpath, media_type="video/mp4")


@router.get("/preview")
async def preview_counts():
    templates = _selected_templates()
    all_tpls = storage.list_templates()
    videos = storage.list_videos()
    music = storage.list_music()
    return {
        "templates": len(templates),
        "templates_total": len(all_tpls),
        "videos": len(videos),
        "music": len(music),
        "max_possible": len(templates) * len(videos),
    }


@router.get("/preview-selection")
async def preview_selection(max_variants: int = Query(1)):
    """Retourne quels templates seront utilisés pour les N premières variantes."""
    templates = _selected_templates()
    videos = storage.list_videos()
    if not templates or not videos:
        return {"templates": []}

    pairs = []
    for t in templates:
        for _ in videos:
            pairs.append(t)
    pairs = pairs[:max_variants] if max_variants > 0 else pairs

    used_ids: List[str] = []
    used_tpls: List[Dict[str, Any]] = []
    for t in pairs:
        if t["id"] not in used_ids:
            used_ids.append(t["id"])
            used_tpls.append(t)
    return {"templates": used_tpls}


@router.post("/run")
async def run_mix(payload: Dict[str, Any] = Body(...)):
    """Run synchrone (gardé pour compat). Préférer /run-stream."""
    max_variants = int(payload.get("max_variants", 1))
    size_label = payload.get("size_label", "L")
    audio_priority = payload.get("audio_priority", "template")

    templates = _selected_templates()
    videos = storage.list_videos()
    music = storage.list_music() if audio_priority == "music" else None

    if not templates:
        raise HTTPException(400, "Aucun template sélectionné")
    if not videos:
        raise HTTPException(400, "Aucune vidéo brute uploadée")

    output_paths = mixer_service.mix_batch(
        templates=templates, videos=videos, music_list=music,
        max_variants=max_variants, size_label=size_label,
        audio_priority=audio_priority,
    )

    results = []
    for p in output_paths:
        fn = Path(p).name
        url = f"/api/clipfusion/mixer/output/{fn}"
        meta = storage.add_output(filename=fn, path=p, url=url)
        if meta:
            results.append(meta)

    return {"count": len(results), "outputs": results}


@router.get("/run-stream")
async def run_mix_stream(
    max_variants: int = Query(1),
    size_label: str = Query("L"),
    audio_priority: str = Query("template"),
    position_pct: Optional[float] = Query(None),
    font_size_px: Optional[int] = Query(None),
    max_duration: Optional[float] = Query(None),
    caption_style: str = Query("outlined"),
    device_choice: str = Query("smart_mix"),
    va_name: str = Query(""),
    team: str = Query(""),
    enabled_filters: Optional[str] = Query(None),  # JSON list ex: ["brightness","contrast"]
    custom_ranges: Optional[str] = Query(None),    # JSON dict ex: {"speed":[1.05,1.10]}
):
    """Stream SSE pour progression en temps réel."""
    templates = _selected_templates()
    videos = storage.list_videos()
    music = storage.list_music() if audio_priority == "music" else None

    # Parse JSON params (sinon None -> tous activés, plages par défaut)
    parsed_filters: Optional[List[str]] = None
    parsed_ranges: Optional[Dict[str, Any]] = None
    if enabled_filters:
        try:
            parsed_filters = json.loads(enabled_filters)
            if not isinstance(parsed_filters, list):
                parsed_filters = None
        except Exception:
            parsed_filters = None
    if custom_ranges:
        try:
            raw = json.loads(custom_ranges)
            if isinstance(raw, dict):
                parsed_ranges = {k: tuple(v) for k, v in raw.items() if isinstance(v, (list, tuple)) and len(v) == 2}
        except Exception:
            parsed_ranges = None

    if not templates or not videos:
        async def err_gen():
            err = {"type": "error", "message": "Aucun template ou vidéo"}
            yield f"data: {json.dumps(err)}\n\n"
            yield f"data: {json.dumps({'type': 'done', 'outputs': []})}\n\n"
        return StreamingResponse(err_gen(), media_type="text/event-stream")

    def event_gen():
        try:
            for ev in mixer_service.mix_batch_stream(
                templates=templates, videos=videos, music_list=music,
                max_variants=max_variants, size_label=size_label,
                audio_priority=audio_priority,
                position_pct=position_pct,
                font_size_px=font_size_px,
                max_duration=max_duration,
                caption_style=caption_style,
                device_choice=device_choice,
                va_name=va_name,
                team=team,
                enabled_filters=parsed_filters,
                custom_ranges=parsed_ranges,
            ):
                if ev.get("type") == "item_done":
                    out = ev["output"]
                    # Réécriture de l'URL pour matcher notre prefix d'app
                    out_url = f"/api/clipfusion/mixer/output/{out['filename']}"
                    out["url"] = out_url
                    storage.add_output(
                        filename=out["filename"], path=out["path"], url=out_url
                    )
                yield f"data: {json.dumps(ev, ensure_ascii=False)}\n\n"
        except Exception as e:
            logger.exception("event_gen exception")
            yield f"data: {json.dumps({'type':'error','message':str(e)})}\n\n"

    return StreamingResponse(
        event_gen(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )


@router.get("/outputs")
async def list_outputs():
    return storage.list_outputs()


@router.delete("/outputs")
async def clear_outputs():
    count = storage.clear_outputs()
    return {"ok": True, "deleted": count}
