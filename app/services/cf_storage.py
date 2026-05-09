"""
Persistance Postgres pour ClipFusion (templates, vidéos brutes, musiques, outputs).

Reprend les mêmes patterns que va_emails_db.py : DATABASE_URL injectée par Railway,
init_schema au démarrage, fallback silencieux si Postgres pas dispo.

Tables créées (préfixe cf_ pour distinguer des tables Repurpose existantes) :
  - cf_templates : captions + emoji + position + favori + sélection
  - cf_videos    : vidéos brutes uploadées (référence chemin disque)
  - cf_music     : pistes audio uploadées (référence chemin disque)
  - cf_outputs   : vidéos générées par le mix (historique)

Note : les fichiers eux-mêmes restent sur disque (storage/clipfusion/...).
La DB ne stocke que les métadonnées + chemins.
"""
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from app.utils.logger import get_logger

logger = get_logger("cf_storage")


# ---------------------------------------------------------------------------
# Helpers communs
# ---------------------------------------------------------------------------
def _get_database_url() -> Optional[str]:
    return os.getenv("DATABASE_URL")


def is_db_enabled() -> bool:
    return bool(_get_database_url())


def _get_connection():
    import psycopg2
    url = _get_database_url()
    if not url:
        raise RuntimeError("DATABASE_URL non configuré")
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql://", 1)
    return psycopg2.connect(url)


def gen_id() -> str:
    return uuid.uuid4().hex[:12]


def _now() -> datetime:
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# Init schema (à appeler au démarrage de l'app)
# ---------------------------------------------------------------------------
def init_schema() -> bool:
    """Crée les 4 tables ClipFusion si elles n'existent pas."""
    if not is_db_enabled():
        logger.warning("DATABASE_URL absent, ClipFusion DB non initialisée")
        return False

    create_sql = """
    CREATE TABLE IF NOT EXISTS cf_templates (
        id TEXT PRIMARY KEY,
        caption TEXT NOT NULL,
        music_name TEXT DEFAULT '',
        align TEXT DEFAULT 'center',
        thumbnail_path TEXT,
        is_favorite BOOLEAN DEFAULT FALSE,
        is_selected BOOLEAN DEFAULT TRUE,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS idx_cf_templates_selected ON cf_templates(is_selected);
    CREATE INDEX IF NOT EXISTS idx_cf_templates_favorite ON cf_templates(is_favorite);

    CREATE TABLE IF NOT EXISTS cf_videos (
        id TEXT PRIMARY KEY,
        filename TEXT NOT NULL,
        path TEXT NOT NULL,
        original_name TEXT NOT NULL,
        size_bytes BIGINT DEFAULT 0,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS idx_cf_videos_created ON cf_videos(created_at DESC);

    CREATE TABLE IF NOT EXISTS cf_music (
        id TEXT PRIMARY KEY,
        filename TEXT NOT NULL,
        path TEXT NOT NULL,
        original_name TEXT NOT NULL,
        size_bytes BIGINT DEFAULT 0,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS cf_outputs (
        id TEXT PRIMARY KEY,
        filename TEXT NOT NULL,
        path TEXT NOT NULL,
        url TEXT,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS idx_cf_outputs_created ON cf_outputs(created_at DESC);

    CREATE TABLE IF NOT EXISTS cf_batches (
        id TEXT PRIMARY KEY,
        va_name TEXT DEFAULT '',
        team TEXT DEFAULT '',
        device_choice TEXT DEFAULT '',
        videos_count INTEGER DEFAULT 0,
        videos_uploaded INTEGER DEFAULT 0,
        drive_folder_id TEXT DEFAULT '',
        drive_folder_url TEXT DEFAULT '',
        drive_folder_name TEXT DEFAULT '',
        va_email TEXT DEFAULT '',
        discord_notified BOOLEAN DEFAULT FALSE,
        duration_seconds REAL DEFAULT 0,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS idx_cf_batches_created ON cf_batches(created_at DESC);
    CREATE INDEX IF NOT EXISTS idx_cf_batches_va ON cf_batches(va_name);
    CREATE INDEX IF NOT EXISTS idx_cf_batches_team ON cf_batches(team);
    """

    try:
        with _get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(create_sql)
            conn.commit()
        logger.info("ClipFusion DB schema initialisé")
        return True
    except Exception as e:
        logger.error(f"init_schema ClipFusion failed: {e}")
        return False


# ---------------------------------------------------------------------------
# TEMPLATES
# ---------------------------------------------------------------------------
def _row_to_template(row) -> Dict[str, Any]:
    return {
        "id": row[0],
        "caption": row[1],
        "music_name": row[2] or "",
        "align": row[3] or "center",
        "thumbnail_path": row[4],
        "is_favorite": bool(row[5]),
        "is_selected": bool(row[6]),
        "created_at": row[7].isoformat() if row[7] else None,
    }


def list_templates() -> List[Dict[str, Any]]:
    if not is_db_enabled():
        return []
    try:
        with _get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT id, caption, music_name, align, thumbnail_path, "
                    "is_favorite, is_selected, created_at "
                    "FROM cf_templates ORDER BY created_at DESC"
                )
                return [_row_to_template(r) for r in cur.fetchall()]
    except Exception as e:
        logger.error(f"list_templates failed: {e}")
        return []


def add_template(
    caption: str,
    music_name: str = "",
    align: str = "center",
    thumbnail_path: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    if not is_db_enabled():
        return None
    tpl_id = gen_id()
    try:
        with _get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO cf_templates (id, caption, music_name, align, thumbnail_path) "
                    "VALUES (%s, %s, %s, %s, %s) "
                    "RETURNING id, caption, music_name, align, thumbnail_path, "
                    "is_favorite, is_selected, created_at",
                    (tpl_id, caption, music_name, align, thumbnail_path),
                )
                row = cur.fetchone()
            conn.commit()
        return _row_to_template(row) if row else None
    except Exception as e:
        logger.error(f"add_template failed: {e}")
        return None


def update_template(tpl_id: str, fields: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Update partiel d'un template. Champs autorisés : caption, align, music_name,
    thumbnail_path, is_favorite, is_selected."""
    if not is_db_enabled():
        return None
    allowed = {"caption", "align", "music_name", "thumbnail_path", "is_favorite", "is_selected"}
    set_clauses = []
    values: List[Any] = []
    for k, v in fields.items():
        if k in allowed:
            set_clauses.append(f"{k} = %s")
            values.append(v)
    if not set_clauses:
        return None
    values.append(tpl_id)
    try:
        with _get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"UPDATE cf_templates SET {', '.join(set_clauses)} WHERE id = %s "
                    "RETURNING id, caption, music_name, align, thumbnail_path, "
                    "is_favorite, is_selected, created_at",
                    values,
                )
                row = cur.fetchone()
            conn.commit()
        return _row_to_template(row) if row else None
    except Exception as e:
        logger.error(f"update_template failed: {e}")
        return None


def delete_template(tpl_id: str) -> bool:
    if not is_db_enabled():
        return False
    try:
        with _get_connection() as conn:
            with conn.cursor() as cur:
                # Supprime le fichier thumbnail si présent
                cur.execute("SELECT thumbnail_path FROM cf_templates WHERE id = %s", (tpl_id,))
                row = cur.fetchone()
                if row and row[0]:
                    try:
                        Path(row[0]).unlink(missing_ok=True)
                    except Exception:
                        pass
                cur.execute("DELETE FROM cf_templates WHERE id = %s", (tpl_id,))
                deleted = cur.rowcount > 0
            conn.commit()
        return deleted
    except Exception as e:
        logger.error(f"delete_template failed: {e}")
        return False


def clear_templates() -> int:
    if not is_db_enabled():
        return 0
    try:
        with _get_connection() as conn:
            with conn.cursor() as cur:
                # Récup chemins thumbnails à supprimer
                cur.execute("SELECT thumbnail_path FROM cf_templates WHERE thumbnail_path IS NOT NULL")
                for (path,) in cur.fetchall():
                    try:
                        Path(path).unlink(missing_ok=True)
                    except Exception:
                        pass
                cur.execute("DELETE FROM cf_templates")
                count = cur.rowcount
            conn.commit()
        return count
    except Exception as e:
        logger.error(f"clear_templates failed: {e}")
        return 0


def select_templates_bulk(mode: str) -> int:
    """mode = 'all' | 'none' | 'favorites' | 'reset' (alias de 'all')."""
    if not is_db_enabled():
        return 0
    if mode in ("all", "reset"):
        sql = "UPDATE cf_templates SET is_selected = TRUE"
    elif mode == "none":
        sql = "UPDATE cf_templates SET is_selected = FALSE"
    elif mode == "favorites":
        sql = "UPDATE cf_templates SET is_selected = (is_favorite = TRUE)"
    else:
        return 0
    try:
        with _get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                count = cur.rowcount
            conn.commit()
        return count
    except Exception as e:
        logger.error(f"select_templates_bulk failed: {e}")
        return 0


# ---------------------------------------------------------------------------
# VIDEOS BRUTES
# ---------------------------------------------------------------------------
def _row_to_video(row) -> Dict[str, Any]:
    return {
        "id": row[0],
        "filename": row[1],
        "path": row[2],
        "original_name": row[3],
        "size": int(row[4] or 0),
        "created_at": row[5].isoformat() if row[5] else None,
    }


def list_videos() -> List[Dict[str, Any]]:
    if not is_db_enabled():
        return []
    try:
        with _get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT id, filename, path, original_name, size_bytes, created_at "
                    "FROM cf_videos ORDER BY created_at DESC"
                )
                return [_row_to_video(r) for r in cur.fetchall()]
    except Exception as e:
        logger.error(f"list_videos failed: {e}")
        return []


def add_video(filename: str, path: str, original_name: str = "", size_bytes: int = 0) -> Optional[Dict[str, Any]]:
    if not is_db_enabled():
        return None
    vid_id = gen_id()
    try:
        with _get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO cf_videos (id, filename, path, original_name, size_bytes) "
                    "VALUES (%s, %s, %s, %s, %s) "
                    "RETURNING id, filename, path, original_name, size_bytes, created_at",
                    (vid_id, filename, path, original_name or filename, size_bytes),
                )
                row = cur.fetchone()
            conn.commit()
        return _row_to_video(row) if row else None
    except Exception as e:
        logger.error(f"add_video failed: {e}")
        return None


def get_video(vid_id: str) -> Optional[Dict[str, Any]]:
    if not is_db_enabled():
        return None
    try:
        with _get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT id, filename, path, original_name, size_bytes, created_at "
                    "FROM cf_videos WHERE id = %s",
                    (vid_id,),
                )
                row = cur.fetchone()
        return _row_to_video(row) if row else None
    except Exception as e:
        logger.error(f"get_video failed: {e}")
        return None


def delete_video(vid_id: str) -> bool:
    if not is_db_enabled():
        return False
    try:
        with _get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT path FROM cf_videos WHERE id = %s", (vid_id,))
                row = cur.fetchone()
                if not row:
                    return False
                try:
                    Path(row[0]).unlink(missing_ok=True)
                except Exception:
                    pass
                cur.execute("DELETE FROM cf_videos WHERE id = %s", (vid_id,))
                deleted = cur.rowcount > 0
            conn.commit()
        return deleted
    except Exception as e:
        logger.error(f"delete_video failed: {e}")
        return False


def delete_videos_bulk(vid_ids: List[str]) -> int:
    """Supprime plusieurs vidéos en une fois (utilisé par le filtrage)."""
    if not is_db_enabled() or not vid_ids:
        return 0
    try:
        with _get_connection() as conn:
            with conn.cursor() as cur:
                # Récup les chemins pour les delete sur disque
                cur.execute(
                    "SELECT path FROM cf_videos WHERE id = ANY(%s)", (vid_ids,)
                )
                for (path,) in cur.fetchall():
                    try:
                        Path(path).unlink(missing_ok=True)
                    except Exception:
                        pass
                cur.execute("DELETE FROM cf_videos WHERE id = ANY(%s)", (vid_ids,))
                count = cur.rowcount
            conn.commit()
        return count
    except Exception as e:
        logger.error(f"delete_videos_bulk failed: {e}")
        return 0


def clear_videos() -> int:
    if not is_db_enabled():
        return 0
    try:
        with _get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT path FROM cf_videos")
                for (path,) in cur.fetchall():
                    try:
                        Path(path).unlink(missing_ok=True)
                    except Exception:
                        pass
                cur.execute("DELETE FROM cf_videos")
                count = cur.rowcount
            conn.commit()
        return count
    except Exception as e:
        logger.error(f"clear_videos failed: {e}")
        return 0


# ---------------------------------------------------------------------------
# MUSIC
# ---------------------------------------------------------------------------
def _row_to_music(row) -> Dict[str, Any]:
    return {
        "id": row[0],
        "filename": row[1],
        "path": row[2],
        "original_name": row[3],
        "size": int(row[4] or 0),
        "created_at": row[5].isoformat() if row[5] else None,
    }


def list_music() -> List[Dict[str, Any]]:
    if not is_db_enabled():
        return []
    try:
        with _get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT id, filename, path, original_name, size_bytes, created_at "
                    "FROM cf_music ORDER BY created_at DESC"
                )
                return [_row_to_music(r) for r in cur.fetchall()]
    except Exception as e:
        logger.error(f"list_music failed: {e}")
        return []


def add_music(filename: str, path: str, original_name: str = "", size_bytes: int = 0) -> Optional[Dict[str, Any]]:
    if not is_db_enabled():
        return None
    m_id = gen_id()
    try:
        with _get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO cf_music (id, filename, path, original_name, size_bytes) "
                    "VALUES (%s, %s, %s, %s, %s) "
                    "RETURNING id, filename, path, original_name, size_bytes, created_at",
                    (m_id, filename, path, original_name or filename, size_bytes),
                )
                row = cur.fetchone()
            conn.commit()
        return _row_to_music(row) if row else None
    except Exception as e:
        logger.error(f"add_music failed: {e}")
        return None


def delete_music(music_id: str) -> bool:
    if not is_db_enabled():
        return False
    try:
        with _get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT path FROM cf_music WHERE id = %s", (music_id,))
                row = cur.fetchone()
                if not row:
                    return False
                try:
                    Path(row[0]).unlink(missing_ok=True)
                except Exception:
                    pass
                cur.execute("DELETE FROM cf_music WHERE id = %s", (music_id,))
                deleted = cur.rowcount > 0
            conn.commit()
        return deleted
    except Exception as e:
        logger.error(f"delete_music failed: {e}")
        return False


def clear_music() -> int:
    if not is_db_enabled():
        return 0
    try:
        with _get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT path FROM cf_music")
                for (path,) in cur.fetchall():
                    try:
                        Path(path).unlink(missing_ok=True)
                    except Exception:
                        pass
                cur.execute("DELETE FROM cf_music")
                count = cur.rowcount
            conn.commit()
        return count
    except Exception as e:
        logger.error(f"clear_music failed: {e}")
        return 0


# ---------------------------------------------------------------------------
# OUTPUTS (historique des mix générés)
# ---------------------------------------------------------------------------
def _row_to_output(row) -> Dict[str, Any]:
    return {
        "id": row[0],
        "filename": row[1],
        "path": row[2],
        "url": row[3],
        "created_at": row[4].isoformat() if row[4] else None,
    }


def list_outputs() -> List[Dict[str, Any]]:
    if not is_db_enabled():
        return []
    try:
        with _get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT id, filename, path, url, created_at "
                    "FROM cf_outputs ORDER BY created_at DESC"
                )
                return [_row_to_output(r) for r in cur.fetchall()]
    except Exception as e:
        logger.error(f"list_outputs failed: {e}")
        return []


def add_output(filename: str, path: str, url: str = "") -> Optional[Dict[str, Any]]:
    if not is_db_enabled():
        return None
    o_id = gen_id()
    try:
        with _get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO cf_outputs (id, filename, path, url) "
                    "VALUES (%s, %s, %s, %s) "
                    "RETURNING id, filename, path, url, created_at",
                    (o_id, filename, path, url),
                )
                row = cur.fetchone()
            conn.commit()
        return _row_to_output(row) if row else None
    except Exception as e:
        logger.error(f"add_output failed: {e}")
        return None


def clear_outputs() -> int:
    if not is_db_enabled():
        return 0
    try:
        with _get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT path FROM cf_outputs")
                for (path,) in cur.fetchall():
                    try:
                        Path(path).unlink(missing_ok=True)
                    except Exception:
                        pass
                cur.execute("DELETE FROM cf_outputs")
                count = cur.rowcount
            conn.commit()
        return count
    except Exception as e:
        logger.error(f"clear_outputs failed: {e}")
        return 0


# ---------------------------------------------------------------------------
# BATCHES (historique des mix lancés)
# ---------------------------------------------------------------------------
def _row_to_batch(row) -> Dict[str, Any]:
    return {
        "id": row[0],
        "va_name": row[1] or "",
        "team": row[2] or "",
        "device_choice": row[3] or "",
        "videos_count": int(row[4] or 0),
        "videos_uploaded": int(row[5] or 0),
        "drive_folder_id": row[6] or "",
        "drive_folder_url": row[7] or "",
        "drive_folder_name": row[8] or "",
        "va_email": row[9] or "",
        "discord_notified": bool(row[10]),
        "duration_seconds": float(row[11] or 0),
        "created_at": row[12].isoformat() if row[12] else None,
    }


def add_batch(
    va_name: str = "",
    team: str = "",
    device_choice: str = "",
    videos_count: int = 0,
    videos_uploaded: int = 0,
    drive_folder_id: str = "",
    drive_folder_url: str = "",
    drive_folder_name: str = "",
    va_email: str = "",
    discord_notified: bool = False,
    duration_seconds: float = 0.0,
) -> Optional[Dict[str, Any]]:
    """Enregistre un batch de mix dans l'historique."""
    if not is_db_enabled():
        return None
    bid = gen_id()
    try:
        with _get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO cf_batches "
                    "(id, va_name, team, device_choice, videos_count, videos_uploaded, "
                    " drive_folder_id, drive_folder_url, drive_folder_name, va_email, "
                    " discord_notified, duration_seconds) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
                    "RETURNING id, va_name, team, device_choice, videos_count, videos_uploaded, "
                    "drive_folder_id, drive_folder_url, drive_folder_name, va_email, "
                    "discord_notified, duration_seconds, created_at",
                    (bid, va_name, team, device_choice, videos_count, videos_uploaded,
                     drive_folder_id, drive_folder_url, drive_folder_name, va_email,
                     discord_notified, duration_seconds),
                )
                row = cur.fetchone()
            conn.commit()
        return _row_to_batch(row) if row else None
    except Exception as e:
        logger.error(f"add_batch failed: {e}")
        return None


def list_batches(
    period_days: Optional[int] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    va_name: Optional[str] = None,
    team: Optional[str] = None,
    limit: int = 200,
) -> List[Dict[str, Any]]:
    """
    Liste les batches avec filtres optionnels.

    period_days : nb de jours en arrière depuis aujourd'hui (1 = aujourd'hui, 7 = 7 jours, etc.)
    start_date / end_date : ISO dates (YYYY-MM-DD) — surcharge period_days si fournis
    va_name : filtre par VA
    team : filtre par équipe
    """
    if not is_db_enabled():
        return []

    where = []
    params: List[Any] = []

    if start_date:
        where.append("created_at >= %s")
        params.append(start_date + " 00:00:00")
    if end_date:
        where.append("created_at <= %s")
        params.append(end_date + " 23:59:59")
    if not start_date and not end_date and period_days is not None and period_days > 0:
        where.append("created_at >= NOW() - INTERVAL '%s days'")
        params.append(period_days)
    if va_name:
        where.append("va_name = %s")
        params.append(va_name)
    if team:
        where.append("team = %s")
        params.append(team)

    where_sql = ("WHERE " + " AND ".join(where)) if where else ""
    sql = (
        "SELECT id, va_name, team, device_choice, videos_count, videos_uploaded, "
        "drive_folder_id, drive_folder_url, drive_folder_name, va_email, "
        "discord_notified, duration_seconds, created_at "
        f"FROM cf_batches {where_sql} "
        "ORDER BY created_at DESC LIMIT %s"
    )
    params.append(int(limit))

    try:
        with _get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                return [_row_to_batch(r) for r in cur.fetchall()]
    except Exception as e:
        logger.error(f"list_batches failed: {e}")
        return []


def delete_batch(batch_id: str) -> bool:
    if not is_db_enabled():
        return False
    try:
        with _get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM cf_batches WHERE id = %s", (batch_id,))
                deleted = cur.rowcount > 0
            conn.commit()
        return deleted
    except Exception as e:
        logger.error(f"delete_batch failed: {e}")
        return False


def get_batches_stats() -> Dict[str, Any]:
    """Stats globales pour le panneau historique."""
    if not is_db_enabled():
        return {"total": 0, "today": 0, "videos_total": 0}
    try:
        with _get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*), COALESCE(SUM(videos_uploaded),0) FROM cf_batches")
                total, vids = cur.fetchone()
                cur.execute(
                    "SELECT COUNT(*), COALESCE(SUM(videos_uploaded),0) "
                    "FROM cf_batches WHERE created_at >= CURRENT_DATE"
                )
                today, vids_today = cur.fetchone()
        return {
            "total": int(total or 0),
            "today": int(today or 0),
            "videos_total": int(vids or 0),
            "videos_today": int(vids_today or 0),
        }
    except Exception as e:
        logger.error(f"get_batches_stats failed: {e}")
        return {"total": 0, "today": 0, "videos_total": 0, "videos_today": 0}
