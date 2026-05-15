"""
Module de persistance PostgreSQL pour les emails VA.

Variables d'environnement :
  DATABASE_URL : URL de connexion Postgres (auto-injectée par Railway si DB attachée)
                 Format: postgresql://user:pass@host:port/dbname

La table 'va_emails' est créée automatiquement au démarrage si elle n'existe pas.
Elle stocke {discord_id -> email} de manière permanente.

Si DATABASE_URL n'est pas défini, le module est désactivé silencieusement
(le cache JSON continue de marcher seul, en mode éphémère).
"""
import os
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

from app.utils.logger import get_logger

logger = get_logger("va_emails_db")


# In-memory cache de load_all_emails() (TTL 30s).
# Évite de lire toute la table à chaque /request (table appelée à chaque check email).
# Invalidé automatiquement quand save_email() ou delete_email() est appelé.
_emails_cache: Optional[Tuple[float, Dict[str, str]]] = None
_EMAILS_CACHE_TTL_SEC = 30


def _invalidate_emails_cache() -> None:
    """Force le prochain load_all_emails() à re-query la DB."""
    global _emails_cache
    _emails_cache = None


def _get_database_url() -> Optional[str]:
    """Retourne l'URL Postgres si Railway l'a injectée."""
    return os.getenv("DATABASE_URL")


def is_db_enabled() -> bool:
    return bool(_get_database_url())


import threading as _threading
from contextlib import contextmanager as _contextmanager

_db_pool = None  # type: ignore
_db_pool_lock = _threading.Lock()


def _get_pool():
    """Lazy init du pool. None si DATABASE_URL pas set."""
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
            _db_pool = ThreadedConnectionPool(minconn=1, maxconn=5, dsn=url)
            logger.info("✅ va_emails_db pool initialisé (min=1, max=5)")
        except Exception as e:
            logger.error(f"Pool init failed: {e}")
            _db_pool = None
    return _db_pool


@_contextmanager
def _get_connection():
    """Context manager pooled (compat avec usage existant)."""
    pool = _get_pool()
    if pool is None:
        # Fallback : connexion directe
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
            try:
                conn.close()
            except Exception:
                pass


def init_schema() -> bool:
    """
    Crée la table va_emails si elle n'existe pas.
    À appeler au démarrage de l'app.
    """
    if not is_db_enabled():
        logger.info("Postgres non configuré, init_schema skip")
        return False

    create_sql = """
    CREATE TABLE IF NOT EXISTS va_emails (
        discord_id TEXT PRIMARY KEY,
        email TEXT NOT NULL,
        name TEXT,
        team TEXT,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS idx_va_emails_team ON va_emails(team);
    """
    try:
        with _get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(create_sql)
            conn.commit()
        logger.info("Table va_emails OK (créée ou déjà existante)")
        return True
    except Exception as e:
        logger.exception(f"init_schema ÉCHEC: {e}")
        return False


def save_email(discord_id: str, email: str, name: str = "", team: str = "") -> bool:
    """
    Sauvegarde l'email d'un VA (UPSERT).
    Retourne True si OK.
    """
    if not is_db_enabled():
        return False
    if not discord_id or not email:
        return False

    sql = """
    INSERT INTO va_emails (discord_id, email, name, team, updated_at)
    VALUES (%s, %s, %s, %s, NOW())
    ON CONFLICT (discord_id) DO UPDATE SET
        email = EXCLUDED.email,
        name = EXCLUDED.name,
        team = EXCLUDED.team,
        updated_at = NOW();
    """
    try:
        with _get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (str(discord_id), email.lower().strip(), name or "", team or ""))
            conn.commit()
        # Invalide le cache pour que le prochain load_all_emails() voie le nouvel email
        _invalidate_emails_cache()
        logger.info(f"DB: email enregistré pour discord_id={discord_id}")
        return True
    except Exception as e:
        logger.exception(f"save_email ÉCHEC pour {discord_id}: {e}")
        return False


def load_all_emails() -> Dict[str, str]:
    """
    Retourne un dict {discord_id: email} de tous les VA enregistrés.

    Caché en mémoire 30s (invalidé sur save/delete). Évite de scanner toute
    la table à chaque /request (gain ~100-200ms sur Railway).
    """
    global _emails_cache
    if not is_db_enabled():
        return {}
    now = time.monotonic()
    if _emails_cache is not None and (now - _emails_cache[0]) < _EMAILS_CACHE_TTL_SEC:
        return _emails_cache[1]
    try:
        with _get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT discord_id, email FROM va_emails;")
                rows = cur.fetchall()
        result = {str(row[0]): row[1] for row in rows}
        _emails_cache = (now, result)
        return result
    except Exception as e:
        logger.exception(f"load_all_emails ÉCHEC: {e}")
        return {}


def delete_email(discord_id: str) -> bool:
    """Supprime l'entrée d'un VA (utile si le VA quitte)."""
    if not is_db_enabled():
        return False
    try:
        with _get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM va_emails WHERE discord_id = %s;", (str(discord_id),))
            conn.commit()
        _invalidate_emails_cache()
        return True
    except Exception as e:
        logger.warning(f"delete_email ÉCHEC pour {discord_id}: {e}")
        return False
