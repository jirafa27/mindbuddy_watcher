"""Общие SQL-хелперы для слоя локальной БД watcher."""

import logging
import sqlite3
from pathlib import Path
from typing import Dict, Optional, Protocol


logger = logging.getLogger("app.core.database")


FILE_STATE_COLUMNS = (
    "relative_path, content_hash, last_uploaded_at, last_downloaded_at, "
    "last_seen_mtime, server_user_file_id, namespace_id, "
    "is_applying_remote, last_command_id, updated_at"
)

FOLDER_STATE_COLUMNS = (
    "relative_path, namespace_id, parent_relative_path, kind, "
    "is_applying_remote, last_command_id, updated_at"
)


class DBContext(Protocol):
    db_path: Path
    token: str


def connect(db_path: Path) -> sqlite3.Connection:
    return sqlite3.connect(db_path)


def prefix_like(relative_prefix: str) -> str:
    return f"{relative_prefix}/%"


def file_state_from_row(row) -> Dict:
    return {
        "relative_path": row[0],
        "content_hash": row[1],
        "last_uploaded_at": row[2],
        "last_downloaded_at": row[3],
        "last_seen_mtime": row[4],
        "server_user_file_id": row[5],
        "namespace_id": row[6],
        "is_applying_remote": bool(row[7]),
        "last_command_id": row[8],
        "updated_at": row[9],
    }


def folder_state_from_row(row) -> Dict:
    return {
        "relative_path": row[0],
        "namespace_id": row[1],
        "parent_relative_path": row[2],
        "kind": row[3],
        "is_applying_remote": bool(row[4]),
        "last_command_id": row[5],
        "updated_at": row[6],
    }


def log_error(message: str, error: Exception, *args: object) -> None:
    logger.error(message, *args, error)


def fetch_existing_file_state(db: DBContext, relative_path: str) -> Optional[Dict]:
    try:
        with connect(db.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                f"""
                    SELECT {FILE_STATE_COLUMNS}
                    FROM files
                    WHERE token = ? AND relative_path = ?
                """,
                (db.token, relative_path),
            )
            row = cursor.fetchone()
            return file_state_from_row(row) if row else None
    except Exception as error:
        logger.error("Ошибка получения file_state: %s", error)
        return None


def fetch_existing_folder_state(db: DBContext, relative_path: str) -> Optional[Dict]:
    try:
        with connect(db.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                f"""
                    SELECT {FOLDER_STATE_COLUMNS}
                    FROM folders
                    WHERE token = ? AND relative_path = ?
                """,
                (db.token, relative_path),
            )
            row = cursor.fetchone()
            return folder_state_from_row(row) if row else None
    except Exception as error:
        logger.error("Ошибка получения folder_state: %s", error)
        return None
