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
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from app.utils.logger import get_logger

logger = get_logger("cf_storage")


# In-memory cache de list_models() (TTL 30s).
# list_models est appelée à chaque /request (via get_model_by_label_number) et
# à chaque /models. La table cf_models change très rarement (création manuelle
# par admin via le site). Caché 30s = -100/200ms sur Railway sans inconsistance.
# Invalidé automatiquement quand add_model/delete_model/rename_model.
_models_cache: Optional[Tuple[float, List[Dict[str, Any]]]] = None
_MODELS_CACHE_TTL_SEC = 30


def _invalidate_models_cache() -> None:
    """Force le prochain list_models() à re-query la DB."""
    global _models_cache
    _models_cache = None


# ---------------------------------------------------------------------------
# Helpers communs
# ---------------------------------------------------------------------------
def _get_database_url() -> Optional[str]:
    return os.getenv("DATABASE_URL")


def is_db_enabled() -> bool:
    return bool(_get_database_url())


# Pool de connexions Postgres (psycopg2 ThreadedConnectionPool).
# Réutilise les sockets TCP → -30-100ms par query (sur Railway DB).
# minconn=1, maxconn=10 = sweet spot pour notre charge.
import threading as _threading
from contextlib import contextmanager as _contextmanager

_db_pool = None  # type: ignore
_db_pool_lock = _threading.Lock()


def _get_pool():
    """Lazy init du pool. Retourne None si DATABASE_URL pas configuré."""
    global _db_pool
    if _db_pool is not None:
        return _db_pool
    with _db_pool_lock:
        if _db_pool is not None:
            return _db_pool
        url = _get_database_url()
        if not url:
            return None
        if url.startswith("postgres://"):
            url = url.replace("postgres://", "postgresql://", 1)
        try:
            from psycopg2.pool import ThreadedConnectionPool
            _db_pool = ThreadedConnectionPool(minconn=1, maxconn=10, dsn=url)
            logger.info("✅ DB connection pool initialisé (min=1, max=10)")
        except Exception as e:
            logger.error(f"Pool init failed, fallback connexions directes: {e}")
            _db_pool = None
    return _db_pool


@_contextmanager
def _get_connection():
    """
    Context manager pour récupérer une connexion DB pooled.

    Compat 100% avec l'usage `with _get_connection() as conn:` existant.
    Gère commit/rollback auto + retour de la connexion au pool en fin de bloc.
    Si pool indispo, fallback sur connexion directe (one-shot, plus lent).
    """
    pool = _get_pool()
    if pool is None:
        # Fallback : connexion directe (pas configuré ou pool init failed)
        import psycopg2
        url = _get_database_url()
        if not url:
            raise RuntimeError("DATABASE_URL non configuré")
        if url.startswith("postgres://"):
            url = url.replace("postgres://", "postgresql://", 1)
        conn = psycopg2.connect(url)
        try:
            yield conn
            try:
                conn.commit()
            except Exception:
                pass
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
            raise
        finally:
            try:
                conn.close()
            except Exception:
                pass
        return

    # Path normal : pool
    conn = pool.getconn()
    try:
        yield conn
        try:
            conn.commit()
        except Exception:
            pass
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    finally:
        try:
            pool.putconn(conn)
        except Exception:
            # Si la connexion est crashée, on la close au lieu de la retourner au pool
            try:
                conn.close()
            except Exception:
                pass


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

    CREATE TABLE IF NOT EXISTS cf_models (
        id SERIAL PRIMARY KEY,
        label TEXT NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS idx_cf_models_id ON cf_models(id);

    CREATE TABLE IF NOT EXISTS cf_accounts (
        id SERIAL PRIMARY KEY,
        username TEXT NOT NULL,
        model_id INTEGER NOT NULL,
        va_discord_id TEXT DEFAULT '',
        va_name TEXT DEFAULT '',
        device_choice TEXT NOT NULL,
        gps_lat REAL NOT NULL,
        gps_lng REAL NOT NULL,
        gps_city TEXT NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        UNIQUE(username, model_id)
    );
    CREATE INDEX IF NOT EXISTS idx_cf_accounts_model ON cf_accounts(model_id);
    CREATE INDEX IF NOT EXISTS idx_cf_accounts_va ON cf_accounts(va_discord_id);
    CREATE INDEX IF NOT EXISTS idx_cf_accounts_username ON cf_accounts(username);
    """

    try:
        with _get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(create_sql)
                # Migration : ajout colonnes model_id / model_label sur cf_batches existant
                cur.execute("""
                    ALTER TABLE cf_batches
                    ADD COLUMN IF NOT EXISTS model_id INTEGER DEFAULT NULL,
                    ADD COLUMN IF NOT EXISTS model_label TEXT DEFAULT ''
                """)
                # Migration : ajout colonne model_id sur cf_videos (catégorie obligatoire)
                cur.execute("""
                    ALTER TABLE cf_videos
                    ADD COLUMN IF NOT EXISTS model_id INTEGER DEFAULT NULL
                """)
                cur.execute("CREATE INDEX IF NOT EXISTS idx_cf_videos_model ON cf_videos(model_id)")
                # Migration : ajout colonne account_username sur cf_batches
                # Permet de tracker quel compte Insta a été utilisé (pour intervalle min)
                cur.execute("""
                    ALTER TABLE cf_batches
                    ADD COLUMN IF NOT EXISTS account_username TEXT DEFAULT ''
                """)
                cur.execute("CREATE INDEX IF NOT EXISTS idx_cf_batches_account ON cf_batches(account_username)")
                # Migration : ajout colonnes ios_version + ios_set_at sur cf_accounts
                # Permet le drift iOS réaliste : un compte garde son iOS pendant
                # ~30-60j puis a une chance croissante de "mettre à jour" comme un vrai humain
                cur.execute("""
                    ALTER TABLE cf_accounts
                    ADD COLUMN IF NOT EXISTS ios_version TEXT DEFAULT '',
                    ADD COLUMN IF NOT EXISTS ios_set_at TIMESTAMPTZ DEFAULT NULL
                """)
                # Migration : ajout archived_at pour soft-delete via webhook Lola
                # (quand un VA est ban, ses comptes sont archivés au lieu de
                # supprimés → on garde l'historique batches/owner pour audit)
                cur.execute("""
                    ALTER TABLE cf_accounts
                    ADD COLUMN IF NOT EXISTS archived_at TIMESTAMPTZ DEFAULT NULL,
                    ADD COLUMN IF NOT EXISTS archive_reason TEXT DEFAULT ''
                """)
                cur.execute(
                    "CREATE INDEX IF NOT EXISTS idx_cf_accounts_archived "
                    "ON cf_accounts(archived_at)"
                )
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
        "model_id": int(row[6]) if len(row) > 6 and row[6] is not None else None,
    }


def list_videos(model_id: Optional[int] = None) -> List[Dict[str, Any]]:
    """Liste les vidéos. Filtre optionnel par model_id (catégorie)."""
    if not is_db_enabled():
        return []
    try:
        with _get_connection() as conn:
            with conn.cursor() as cur:
                if model_id is not None:
                    cur.execute(
                        "SELECT id, filename, path, original_name, size_bytes, created_at, model_id "
                        "FROM cf_videos WHERE model_id = %s ORDER BY created_at DESC",
                        (int(model_id),),
                    )
                else:
                    cur.execute(
                        "SELECT id, filename, path, original_name, size_bytes, created_at, model_id "
                        "FROM cf_videos ORDER BY created_at DESC"
                    )
                return [_row_to_video(r) for r in cur.fetchall()]
    except Exception as e:
        logger.error(f"list_videos failed: {e}")
        return []


def add_video(filename: str, path: str, original_name: str = "", size_bytes: int = 0,
              model_id: Optional[int] = None) -> Optional[Dict[str, Any]]:
    """Ajoute une vidéo. model_id = catégorie/modèle obligatoire pour les nouveaux uploads."""
    if not is_db_enabled():
        return None
    vid_id = gen_id()
    try:
        with _get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO cf_videos (id, filename, path, original_name, size_bytes, model_id) "
                    "VALUES (%s, %s, %s, %s, %s, %s) "
                    "RETURNING id, filename, path, original_name, size_bytes, created_at, model_id",
                    (vid_id, filename, path, original_name or filename, size_bytes,
                     int(model_id) if model_id else None),
                )
                row = cur.fetchone()
            conn.commit()
        return _row_to_video(row) if row else None
    except Exception as e:
        logger.error(f"add_video failed: {e}")
        return None


def update_video_model(vid_id: str, model_id: Optional[int]) -> bool:
    """Change la catégorie d'une vidéo existante."""
    if not is_db_enabled():
        return False
    try:
        with _get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE cf_videos SET model_id = %s WHERE id = %s",
                    (int(model_id) if model_id else None, vid_id),
                )
                ok = cur.rowcount > 0
            conn.commit()
        return ok
    except Exception as e:
        logger.error(f"update_video_model failed: {e}")
        return False


def get_video(vid_id: str) -> Optional[Dict[str, Any]]:
    if not is_db_enabled():
        return None
    try:
        with _get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT id, filename, path, original_name, size_bytes, created_at, model_id "
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
    model_id: Optional[int] = None,
    model_label: str = "",
    account_username: str = "",
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
                    " discord_notified, duration_seconds, model_id, model_label, account_username) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
                    "RETURNING id, va_name, team, device_choice, videos_count, videos_uploaded, "
                    "drive_folder_id, drive_folder_url, drive_folder_name, va_email, "
                    "discord_notified, duration_seconds, created_at",
                    (bid, va_name, team, device_choice, videos_count, videos_uploaded,
                     drive_folder_id, drive_folder_url, drive_folder_name, va_email,
                     discord_notified, duration_seconds,
                     int(model_id) if model_id else None, model_label or "",
                     account_username or ""),
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

    # Cache les batchs avec 0 vidéos générées (mix échoué, fichiers manquants, etc.)
    where.append("videos_count > 0")

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


def count_va_videos_recent(va_name: str, days: int = 3) -> int:
    """
    Compte le nombre total de vidéos générées avec succès par un VA
    sur les N derniers jours. Utilisé pour le rate limiting Discord.

    Compte uniquement videos_count > 0 (les mixes qui ont vraiment produit
    quelque chose, pas les batchs échoués).
    """
    if not is_db_enabled() or not va_name:
        return 0
    try:
        with _get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT COALESCE(SUM(videos_count), 0) "
                    "FROM cf_batches "
                    "WHERE va_name = %s "
                    "AND videos_count > 0 "
                    "AND created_at >= NOW() - INTERVAL '%s days'",
                    (va_name, days),
                )
                row = cur.fetchone()
                return int(row[0] or 0) if row else 0
    except Exception as e:
        logger.error(f"count_va_videos_recent failed: {e}")
        return 0


def get_last_batch_time_for_account(account_username: str) -> Optional[datetime]:
    """
    Retourne la datetime UTC du dernier batch (réussi) sur un compte donné.
    None si aucun batch trouvé.
    Utilisé pour empêcher le spam d'un même compte (anti-pattern non humain).
    """
    if not is_db_enabled() or not account_username:
        return None
    try:
        with _get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT MAX(created_at) "
                    "FROM cf_batches "
                    "WHERE account_username = %s "
                    "AND videos_count > 0",
                    (account_username,),
                )
                row = cur.fetchone()
                if row and row[0]:
                    return row[0]
                return None
    except Exception as e:
        logger.error(f"get_last_batch_time_for_account failed: {e}")
        return None


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


# ---------------------------------------------------------------------------
# MODELS (créatrices OnlyFans gérées par les VAs)
# ---------------------------------------------------------------------------
def list_models() -> List[Dict[str, Any]]:
    """
    Liste tous les modèles enregistrés, triés par ID.
    Caché en mémoire 30s, invalidé sur add/delete/rename.
    """
    global _models_cache
    if not is_db_enabled():
        return []
    now = time.monotonic()
    if _models_cache is not None and (now - _models_cache[0]) < _MODELS_CACHE_TTL_SEC:
        return _models_cache[1]
    try:
        with _get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT id, label, created_at FROM cf_models ORDER BY id ASC"
                )
                rows = cur.fetchall()
        result = [
            {
                "id": int(r[0]),
                "label": r[1] or f"Modele {r[0]}",
                "created_at": r[2].isoformat() if r[2] else None,
            }
            for r in rows
        ]
        _models_cache = (now, result)
        return result
    except Exception as e:
        logger.error(f"list_models failed: {e}")
        return []


def add_model(label: str = "") -> Optional[Dict[str, Any]]:
    """
    Crée un nouveau modèle.
    Si label est vide, génère "Modele {id}" automatiquement après l'INSERT.
    """
    if not is_db_enabled():
        return None
    try:
        with _get_connection() as conn:
            with conn.cursor() as cur:
                # INSERT avec label vide d'abord
                cur.execute(
                    "INSERT INTO cf_models (label) VALUES (%s) RETURNING id, label, created_at",
                    (label or "",),
                )
                row = cur.fetchone()
                model_id = int(row[0])
                # Si label vide, on met "Modele {id}"
                if not label:
                    final_label = f"Modele {model_id}"
                    cur.execute(
                        "UPDATE cf_models SET label = %s WHERE id = %s RETURNING id, label, created_at",
                        (final_label, model_id),
                    )
                    row = cur.fetchone()
            conn.commit()
        # Invalide le cache pour que /request voie le nouveau modèle direct
        _invalidate_models_cache()
        return {
            "id": int(row[0]),
            "label": row[1],
            "created_at": row[2].isoformat() if row[2] else None,
        }
    except Exception as e:
        logger.error(f"add_model failed: {e}")
        return None


def delete_model(model_id: int) -> bool:
    if not is_db_enabled():
        return False
    try:
        with _get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM cf_models WHERE id = %s", (int(model_id),))
                deleted = cur.rowcount > 0
            conn.commit()
        if deleted:
            _invalidate_models_cache()
        return deleted
    except Exception as e:
        logger.error(f"delete_model failed: {e}")
        return False


def rename_model(model_id: int, new_label: str) -> bool:
    """Renomme un modèle existant. Retourne True si modifié."""
    if not is_db_enabled() or not model_id:
        return False
    clean = (new_label or "").strip()
    if not clean:
        return False
    try:
        with _get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE cf_models SET label = %s WHERE id = %s",
                    (clean, int(model_id)),
                )
                updated = cur.rowcount > 0
            conn.commit()
        if updated:
            _invalidate_models_cache()
        return updated
    except Exception as e:
        logger.error(f"rename_model failed: {e}")
        return False


def get_model(model_id: int) -> Optional[Dict[str, Any]]:
    if not is_db_enabled() or not model_id:
        return None
    try:
        with _get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT id, label, created_at FROM cf_models WHERE id = %s",
                    (int(model_id),),
                )
                row = cur.fetchone()
        if not row:
            return None
        return {
            "id": int(row[0]),
            "label": row[1],
            "created_at": row[2].isoformat() if row[2] else None,
        }
    except Exception as e:
        logger.error(f"get_model failed: {e}")
        return None


def get_model_by_label_number(n: int) -> Optional[Dict[str, Any]]:
    """
    Trouve un modèle par le NUMÉRO contenu dans son label (pas par son DB id).

    Le site affiche aux VAs des labels du type "ID1", "ID8", "Modele 4", etc.
    Mais les IDs DB peuvent être décalés (suppressions = trous dans l'auto-increment),
    donc /request modele:8 ne doit PAS chercher l'id DB 8, mais le modèle dont
    le label contient 8 (ce que voit le VA sur le site).

    Patterns acceptés (exact match prioritaire) :
      "Modele 8", "Modèle 8", "ID8", "ID 8", "id8", "8"
    Fallback : tout label contenant 8 comme nombre isolé (word boundary).

    Retourne le 1er match ou None.
    """
    if not is_db_enabled() or not n:
        return None
    try:
        models = list_models()
    except Exception:
        return None
    if not models:
        return None

    n_str = str(int(n))
    exact_patterns = {
        f"Modele {n_str}", f"Modèle {n_str}",
        f"modele {n_str}", f"modèle {n_str}",
        f"ID{n_str}", f"ID {n_str}",
        f"id{n_str}", f"id {n_str}",
        n_str,
    }
    for m in models:
        label = (m.get("label") or "").strip()
        if label in exact_patterns:
            return m

    import re as _re
    pattern = _re.compile(rf"\b{n_str}\b")
    for m in models:
        label = (m.get("label") or "").strip()
        if pattern.search(label):
            return m

    return None


# ---------------------------------------------------------------------------
# COMPTES INSTAGRAM (cf_accounts)
# Chaque compte = 1 device locked + 1 GPS locked + 1 modèle associé
# ---------------------------------------------------------------------------
# Liste des 10 grandes villes US pour le tirage GPS au 1er /request
# Format : (label, lat, lng, altitude_meters)
# Altitudes réelles approximatives (centre-ville) pour cohérence
US_CITIES_GPS = [
    ("Miami, FL",        25.7617,  -80.1918,    2),
    ("New York, NY",     40.7128,  -74.0060,   10),
    ("Los Angeles, CA",  34.0522, -118.2437,   89),
    ("Chicago, IL",      41.8781,  -87.6298,  179),
    ("Houston, TX",      29.7604,  -95.3698,   13),
    ("Phoenix, AZ",      33.4484, -112.0740,  331),
    ("Philadelphia, PA", 39.9526,  -75.1652,   12),
    ("San Antonio, TX",  29.4241,  -98.4936,  198),
    ("Dallas, TX",       32.7767,  -96.7970,  131),
    ("Atlanta, GA",      33.7490,  -84.3880,  320),
]

# Liste des devices iPhone disponibles pour le tirage au 1er /request
# Note : iPhone 16e retiré (ne supportait que 30 fps, on force 60 fps partout)
IPHONE_DEVICE_CHOICES = [
    "iphone_17_pro_max", "iphone_17_pro", "iphone_17_air", "iphone_17",
    "iphone_16_pro_max", "iphone_16_pro", "iphone_16_plus", "iphone_16",
]


def _row_to_account(row) -> Dict[str, Any]:
    """
    Convertit une row DB en dict.
    Les colonnes ios_version + ios_set_at sont en position 10/11 si présentes
    (sinon vides — compatibilité avec les SELECT qui ne les fetchent pas).
    """
    base = {
        "id": int(row[0]),
        "username": row[1],
        "model_id": int(row[2]),
        "va_discord_id": row[3] or "",
        "va_name": row[4] or "",
        "device_choice": row[5],
        "gps_lat": float(row[6]),
        "gps_lng": float(row[7]),
        "gps_city": row[8],
        "created_at": row[9].isoformat() if row[9] else None,
        "ios_version": "",
        "ios_set_at": None,
        "archived_at": None,
        "archive_reason": "",
    }
    # Les colonnes 10 et 11 sont optionnelles (ajoutées par migration)
    if len(row) > 10:
        base["ios_version"] = row[10] or ""
    if len(row) > 11:
        base["ios_set_at"] = row[11].isoformat() if row[11] else None
    # Colonnes 12-13 : archived_at + archive_reason (migration récente)
    if len(row) > 12:
        base["archived_at"] = row[12].isoformat() if row[12] else None
    if len(row) > 13:
        base["archive_reason"] = row[13] or ""
    return base


def get_city_altitude(city_name: str) -> int:
    """
    Retourne l'altitude (mètres) pour une ville US connue.
    Utilisé pour cohérence GPS : un compte locké à Miami doit avoir
    altitude ~2m, pas 187m random.
    Fallback : 50m (moyenne plausible) si ville inconnue.
    """
    if not city_name:
        return 50
    for label, _lat, _lng, alt in US_CITIES_GPS:
        if label == city_name:
            return alt
    return 50


def find_account(username: str, model_id: int) -> Optional[Dict[str, Any]]:
    """Cherche un compte par username + model_id (clé unique)."""
    if not is_db_enabled() or not username or not model_id:
        return None
    try:
        with _get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT id, username, model_id, va_discord_id, va_name, "
                    "device_choice, gps_lat, gps_lng, gps_city, created_at, ios_version, ios_set_at "
                    "FROM cf_accounts WHERE username = %s AND model_id = %s",
                    (username.strip(), int(model_id)),
                )
                row = cur.fetchone()
        return _row_to_account(row) if row else None
    except Exception as e:
        logger.error(f"find_account failed: {e}")
        return None


def archive_accounts_for_va(
    va_discord_id: str,
    reason: str = "",
) -> Dict[str, Any]:
    """
    Soft-delete : marque tous les comptes d'un VA comme archivés.
    Utilisé quand Lola ban un VA → ses comptes ne peuvent plus servir.

    On garde la row pour l'historique (audit, debug, restore éventuel).
    /request rejette les comptes archivés.

    Retourne dict avec count des comptes archivés.
    """
    if not is_db_enabled() or not va_discord_id:
        return {"archived": 0, "error": "missing_input_or_db"}
    try:
        with _get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE cf_accounts
                    SET archived_at = NOW(),
                        archive_reason = %s
                    WHERE va_discord_id = %s
                      AND archived_at IS NULL
                    RETURNING id, username
                    """,
                    (reason[:200], str(va_discord_id)),
                )
                archived_rows = cur.fetchall()
        archived_list = [
            {"id": int(r[0]), "username": r[1]} for r in archived_rows
        ]
        logger.info(
            f"archive_accounts_for_va: {len(archived_list)} comptes archivés "
            f"pour discord_id={va_discord_id} (reason='{reason[:80]}')"
        )
        return {"archived": len(archived_list), "accounts": archived_list}
    except Exception as e:
        logger.error(f"archive_accounts_for_va failed: {e}")
        return {"archived": 0, "error": str(e)}


def get_account_stats(username: str) -> Optional[Dict[str, Any]]:
    """
    Retourne les stats agrégées d'un compte : total batches, total vidéos
    requested/uploaded, taux succès, derniers batch, etc.
    Utilisé par /admin_account_info pour debug rapide.
    """
    if not is_db_enabled() or not username:
        return None
    try:
        with _get_connection() as conn:
            with conn.cursor() as cur:
                # 1. Info compte
                cur.execute(
                    "SELECT id, username, model_id, va_discord_id, va_name, "
                    "device_choice, gps_lat, gps_lng, gps_city, created_at, "
                    "ios_version, ios_set_at "
                    "FROM cf_accounts WHERE username = %s LIMIT 1",
                    (username.strip(),),
                )
                acc_row = cur.fetchone()
                if not acc_row:
                    return None
                account = _row_to_account(acc_row)

                # 2. Stats agrégées des batches
                cur.execute(
                    """
                    SELECT
                        COUNT(*) AS total_batches,
                        COALESCE(SUM(videos_count), 0) AS total_requested,
                        COALESCE(SUM(videos_uploaded), 0) AS total_uploaded,
                        MAX(created_at) AS last_batch_at,
                        AVG(duration_seconds) AS avg_duration
                    FROM cf_batches
                    WHERE account_username = %s
                    """,
                    (username.strip(),),
                )
                stats_row = cur.fetchone() or (0, 0, 0, None, 0)
                total_batches = int(stats_row[0])
                requested = int(stats_row[1])
                uploaded = int(stats_row[2])
                success_rate = round(100.0 * uploaded / requested, 1) if requested > 0 else None

                # 3. Last 5 batches
                cur.execute(
                    """
                    SELECT created_at, videos_count, videos_uploaded, duration_seconds
                    FROM cf_batches
                    WHERE account_username = %s
                    ORDER BY created_at DESC
                    LIMIT 5
                    """,
                    (username.strip(),),
                )
                recent_batches = [
                    {
                        "created_at": r[0].isoformat() if r[0] else None,
                        "videos_count": int(r[1]),
                        "videos_uploaded": int(r[2]),
                        "duration_seconds": float(r[3] or 0),
                    }
                    for r in cur.fetchall()
                ]

        return {
            "account": account,
            "total_batches": total_batches,
            "total_requested": requested,
            "total_uploaded": uploaded,
            "success_rate_pct": success_rate,
            "last_batch_at": stats_row[3].isoformat() if stats_row[3] else None,
            "avg_duration_seconds": float(stats_row[4] or 0),
            "recent_batches": recent_batches,
        }
    except Exception as e:
        logger.error(f"get_account_stats failed: {e}")
        return None


def find_account_any_model(username: str) -> Optional[Dict[str, Any]]:
    """
    Cherche un compte par username uniquement (sans filtrer par modèle).
    Si plusieurs comptes avec le même username sur différents modèles (rare),
    retourne le premier trouvé (le plus récent).
    Utilisé par /respoof qui ne demande plus le modèle au VA.
    """
    if not is_db_enabled() or not username:
        return None
    try:
        with _get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT id, username, model_id, va_discord_id, va_name, "
                    "device_choice, gps_lat, gps_lng, gps_city, created_at, "
                    "ios_version, ios_set_at, archived_at, archive_reason "
                    "FROM cf_accounts WHERE username = %s "
                    "ORDER BY created_at DESC LIMIT 1",
                    (username.strip(),),
                )
                row = cur.fetchone()
        return _row_to_account(row) if row else None
    except Exception as e:
        logger.error(f"find_account_any_model failed: {e}")
        return None


def create_account(
    username: str,
    model_id: int,
    va_discord_id: str = "",
    va_name: str = "",
    device_choice: Optional[str] = None,
    gps_lat: Optional[float] = None,
    gps_lng: Optional[float] = None,
    gps_city: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """
    Crée un nouveau compte avec device + GPS.
    Si device_choice / gps_* ne sont pas fournis, tirage random USA.
    """
    if not is_db_enabled() or not username or not model_id:
        return None

    import random as _r

    # Tirage random pour les valeurs manquantes
    if not device_choice:
        device_choice = _r.choice(IPHONE_DEVICE_CHOICES)
    if gps_lat is None or gps_lng is None or not gps_city:
        city = _r.choice(US_CITIES_GPS)
        gps_city = city[0]
        gps_lat = city[1]
        gps_lng = city[2]
        # Note : altitude (city[3]) pas stockée en DB ; lookup dynamique via get_city_altitude()

    try:
        with _get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO cf_accounts "
                    "(username, model_id, va_discord_id, va_name, "
                    " device_choice, gps_lat, gps_lng, gps_city) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s) "
                    "ON CONFLICT (username, model_id) DO NOTHING "
                    "RETURNING id, username, model_id, va_discord_id, va_name, "
                    "device_choice, gps_lat, gps_lng, gps_city, created_at, ios_version, ios_set_at",
                    (
                        username.strip(),
                        int(model_id),
                        str(va_discord_id or ""),
                        str(va_name or ""),
                        device_choice,
                        float(gps_lat),
                        float(gps_lng),
                        gps_city,
                    ),
                )
                row = cur.fetchone()
            conn.commit()
        if row:
            return _row_to_account(row)
        # Si conflit (compte déjà existant), on le retourne tel quel
        return find_account(username, model_id)
    except Exception as e:
        logger.error(f"create_account failed: {e}")
        return None


def get_or_create_account(
    username: str,
    model_id: int,
    va_discord_id: str = "",
    va_name: str = "",
) -> Optional[Dict[str, Any]]:
    """
    Récupère le compte s'il existe, sinon le crée avec device + GPS random.
    Méthode pratique pour /request Discord.
    """
    existing = find_account(username, model_id)
    if existing:
        return existing
    return create_account(
        username=username,
        model_id=model_id,
        va_discord_id=va_discord_id,
        va_name=va_name,
    )


def list_accounts(
    model_id: Optional[int] = None,
    va_discord_id: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Liste tous les comptes, optionnellement filtrés par modèle ou VA."""
    if not is_db_enabled():
        return []

    where = []
    params: List[Any] = []
    if model_id:
        where.append("model_id = %s")
        params.append(int(model_id))
    if va_discord_id:
        where.append("va_discord_id = %s")
        params.append(str(va_discord_id))
    where_sql = ("WHERE " + " AND ".join(where)) if where else ""

    try:
        with _get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT id, username, model_id, va_discord_id, va_name, "
                    "device_choice, gps_lat, gps_lng, gps_city, created_at, ios_version, ios_set_at "
                    f"FROM cf_accounts {where_sql} "
                    "ORDER BY created_at DESC",
                    params,
                )
                return [_row_to_account(r) for r in cur.fetchall()]
    except Exception as e:
        logger.error(f"list_accounts failed: {e}")
        return []


def delete_account(account_id: int) -> bool:
    if not is_db_enabled() or not account_id:
        return False
    try:
        with _get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM cf_accounts WHERE id = %s", (int(account_id),))
                deleted = cur.rowcount > 0
            conn.commit()
        return deleted
    except Exception as e:
        logger.error(f"delete_account failed: {e}")
        return False


def update_account_ios(account_id: int, ios_version: str) -> bool:
    """
    Met à jour la version iOS d'un compte et le timestamp de cette mise à jour.
    Utilisé par la logique de drift iOS (un compte change d'iOS de temps en temps
    comme un vrai humain qui met à jour son téléphone).
    """
    if not is_db_enabled() or not account_id or not ios_version:
        return False
    try:
        with _get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE cf_accounts SET ios_version = %s, ios_set_at = NOW() "
                    "WHERE id = %s",
                    (ios_version, int(account_id)),
                )
                updated = cur.rowcount > 0
            conn.commit()
        return updated
    except Exception as e:
        logger.error(f"update_account_ios failed: {e}")
        return False


# ---------------------------------------------------------------------------
# CLEANUP COMPTES ORPHELINS
# ---------------------------------------------------------------------------
def cleanup_orphan_accounts(days: int = 30, dry_run: bool = False) -> Dict[str, Any]:
    """
    Supprime les comptes cf_accounts qui n'ont eu AUCUN batch dans les `days`
    derniers jours ET dont la création remonte à > `days` jours.

    Logique conservative (pas de faux positifs) :
    - created_at < NOW() - days → le compte a eu le temps d'être utilisé
    - Aucune entrée dans cf_batches avec ce username dans les `days` derniers jours

    Cas couverts :
    - Typos VA (ex: créé "sara_official_226" au lieu de "sara_official_2026")
    - Comptes test qui n'ont jamais servi
    - Comptes abandonnés (VA parti, modèle obsolète, etc.)

    Cas NON touchés (sûrs) :
    - Comptes utilisés récemment (batch < days jours)
    - Comptes créés très récemment (< days jours, peut-être pas encore utilisés)

    Args:
        days: Seuil en jours pour la création + le dernier batch (default 30)
        dry_run: Si True, liste juste les comptes éligibles sans rien supprimer

    Returns:
        Dict avec keys: deleted (int), candidates (list[dict]), dry_run (bool)
    """
    if not is_db_enabled():
        return {"deleted": 0, "candidates": [], "dry_run": dry_run, "error": "db_disabled"}

    select_sql = """
        SELECT id, username, model_id, va_name, created_at
        FROM cf_accounts
        WHERE created_at < NOW() - (%s || ' days')::INTERVAL
        AND NOT EXISTS (
            SELECT 1 FROM cf_batches
            WHERE cf_batches.account_username = cf_accounts.username
            AND cf_batches.created_at > NOW() - (%s || ' days')::INTERVAL
        )
    """
    delete_sql = """
        DELETE FROM cf_accounts
        WHERE created_at < NOW() - (%s || ' days')::INTERVAL
        AND NOT EXISTS (
            SELECT 1 FROM cf_batches
            WHERE cf_batches.account_username = cf_accounts.username
            AND cf_batches.created_at > NOW() - (%s || ' days')::INTERVAL
        )
    """

    try:
        candidates: List[Dict[str, Any]] = []
        with _get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(select_sql, (str(days), str(days)))
                rows = cur.fetchall()
                for r in rows:
                    candidates.append({
                        "id": int(r[0]),
                        "username": r[1],
                        "model_id": int(r[2]) if r[2] is not None else None,
                        "va_name": r[3] or "",
                        "created_at": r[4].isoformat() if r[4] else None,
                    })

                deleted = 0
                if not dry_run and candidates:
                    cur.execute(delete_sql, (str(days), str(days)))
                    deleted = cur.rowcount
            conn.commit()

        logger.info(
            f"cleanup_orphan_accounts: {len(candidates)} candidats, "
            f"{deleted} supprimés (dry_run={dry_run}, seuil={days}j)"
        )
        return {
            "deleted": deleted,
            "candidates": candidates,
            "dry_run": dry_run,
            "threshold_days": days,
        }
    except Exception as e:
        logger.error(f"cleanup_orphan_accounts failed: {e}")
        return {"deleted": 0, "candidates": [], "dry_run": dry_run, "error": str(e)}


# ---------------------------------------------------------------------------
# DÉTECTION D'ANOMALIES DE VOLUME (VA qui post 3x+ son habitude)
# ---------------------------------------------------------------------------
def get_va_volume_anomalies(
    multiplier_threshold: float = 3.0,
    min_today_videos: int = 50,
    baseline_window_days: int = 14,
) -> List[Dict[str, Any]]:
    """
    Retourne les VAs dont le volume des dernières 24h dépasse
    `multiplier_threshold` × leur moyenne journalière des `baseline_window_days`
    derniers jours (en excluant les 24h actuelles).

    Filtre min_today_videos : on ignore les "petits" jours pour éviter le bruit
    (ex: VA qui passe de 5 à 20 vidéos = 4x mais pas un signal).

    Use case typique :
    - VA habituellement à 50 vidéos/jour → soudain 200 vidéos en 24h
    - Compte compromis, panique, ou VA en train de spammer pour mauvaises raisons
    - Pinger l'admin pour qu'il check rapidement
    """
    if not is_db_enabled():
        return []
    sql = """
        WITH today_volumes AS (
            SELECT
                va_name,
                COALESCE(SUM(videos_uploaded), 0) AS today_videos,
                COUNT(*) AS today_batches
            FROM cf_batches
            WHERE va_name != ''
                AND created_at >= NOW() - INTERVAL '24 hours'
            GROUP BY va_name
            HAVING COALESCE(SUM(videos_uploaded), 0) >= %s
        ),
        baseline_per_day AS (
            SELECT
                va_name,
                DATE_TRUNC('day', created_at) AS day,
                COALESCE(SUM(videos_uploaded), 0) AS day_videos
            FROM cf_batches
            WHERE va_name != ''
                AND created_at < NOW() - INTERVAL '24 hours'
                AND created_at >= NOW() - (%s || ' days')::INTERVAL
            GROUP BY va_name, DATE_TRUNC('day', created_at)
        ),
        baseline_avg AS (
            SELECT va_name, AVG(day_videos) AS avg_daily
            FROM baseline_per_day
            GROUP BY va_name
        )
        SELECT
            t.va_name,
            t.today_videos,
            t.today_batches,
            COALESCE(b.avg_daily, 0) AS baseline_avg
        FROM today_volumes t
        LEFT JOIN baseline_avg b ON b.va_name = t.va_name
        WHERE
            -- Anomalie si today > threshold × baseline (et baseline > 0)
            (COALESCE(b.avg_daily, 0) > 0
             AND t.today_videos > %s * COALESCE(b.avg_daily, 1))
            -- OU si baseline = 0 (= VA jamais vu avant) ET today très haut
            OR (COALESCE(b.avg_daily, 0) = 0 AND t.today_videos > %s)
        ORDER BY t.today_videos DESC
    """
    # Threshold pour les VAs sans historique (baseline=0) : 2x le min_today
    new_va_threshold = max(min_today_videos * 2, 100)
    try:
        with _get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    sql,
                    (
                        min_today_videos,
                        str(baseline_window_days),
                        multiplier_threshold,
                        new_va_threshold,
                    ),
                )
                rows = cur.fetchall()
        out: List[Dict[str, Any]] = []
        for r in rows:
            today = int(r[1])
            baseline = float(r[3] or 0)
            ratio = (today / baseline) if baseline > 0 else None
            out.append(
                {
                    "va_name": r[0],
                    "today_videos": today,
                    "today_batches": int(r[2]),
                    "baseline_avg": round(baseline, 1),
                    "ratio": round(ratio, 2) if ratio else None,
                    "is_new_va": baseline == 0,
                }
            )
        return out
    except Exception as e:
        logger.error(f"get_va_volume_anomalies failed: {e}")
        return []
