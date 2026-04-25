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
from datetime import datetime, timezone
from typing import Dict, List, Optional

from app.utils.logger import get_logger

logger = get_logger("va_emails_db")


def _get_database_url() -> Optional[str]:
    """Retourne l'URL Postgres si Railway l'a injectée."""
    return os.getenv("DATABASE_URL")


def is_db_enabled() -> bool:
    return bool(_get_database_url())


def _get_connection():
    """Ouvre une connexion Postgres. Lève si pas configuré."""
    import psycopg2
    url = _get_database_url()
    if not url:
        raise RuntimeError("DATABASE_URL non configuré")
    # Railway donne parfois postgres:// au lieu de postgresql://
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql://", 1)
    return psycopg2.connect(url)


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
        logger.info(f"DB: email enregistré pour discord_id={discord_id}")
        return True
    except Exception as e:
        logger.exception(f"save_email ÉCHEC pour {discord_id}: {e}")
        return False


def load_all_emails() -> Dict[str, str]:
    """
    Retourne un dict {discord_id: email} de tous les VA enregistrés.
    """
    if not is_db_enabled():
        return {}
    try:
        with _get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT discord_id, email FROM va_emails;")
                rows = cur.fetchall()
        return {str(row[0]): row[1] for row in rows}
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
        return True
    except Exception as e:
        logger.warning(f"delete_email ÉCHEC pour {discord_id}: {e}")
        return False
