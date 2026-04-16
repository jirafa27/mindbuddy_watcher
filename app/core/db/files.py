"""Операции над таблицей files."""

import logging
from typing import Dict, List, Optional

from .common import (
    DBContext,
    FILE_STATE_COLUMNS,
    connect,
    fetch_existing_file_state,
    file_state_from_row,
    prefix_like,
)


logger = logging.getLogger("app.core.database")


def get_file_state(db: DBContext, relative_path: str) -> Optional[Dict]:
    """Возвращает состояние файла по относительному пути."""
    return fetch_existing_file_state(db, relative_path)


def get_all_file_states(db: DBContext) -> List[Dict]:
    """Возвращает все file-state записи текущего токена."""
    try:
        with connect(db.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                f"""
                    SELECT {FILE_STATE_COLUMNS}
                    FROM files
                    WHERE token = ?
                """,
                (db.token,),
            )
            return [file_state_from_row(row) for row in cursor.fetchall()]
    except Exception as error:
        logger.error("Ошибка получения списка file_state: %s", error)
        return []


def get_file_states_under_prefix(db: DBContext, relative_prefix: str) -> List[Dict]:
    """Возвращает все file-state записи под указанным префиксом."""
    if not relative_prefix:
        return get_all_file_states(db)

    try:
        with connect(db.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                f"""
                    SELECT {FILE_STATE_COLUMNS}
                    FROM files
                    WHERE token = ? AND (relative_path = ? OR relative_path LIKE ?)
                """,
                (db.token, relative_prefix, prefix_like(relative_prefix)),
            )
            return [file_state_from_row(row) for row in cursor.fetchall()]
    except Exception as error:
        logger.error("Ошибка получения file_state под префиксом %s: %s", relative_prefix, error)
        return []


def get_file_state_by_server_user_file_id(db: DBContext, file_id: int) -> Optional[Dict]:
    """Возвращает file-state запись по server_user_file_id."""
    try:
        with connect(db.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                f"""
                    SELECT {FILE_STATE_COLUMNS}
                    FROM files
                    WHERE token = ? AND server_user_file_id = ?
                    LIMIT 1
                """,
                (db.token, file_id),
            )
            row = cursor.fetchone()
            return file_state_from_row(row) if row else None
    except Exception as error:
        logger.error("Ошибка получения file_state по file_id=%s: %s", file_id, error)
        return None


def upsert_file_state(
    db: DBContext,
    relative_path: str,
    content_hash: Optional[str] = None,
    last_uploaded_at: Optional[str] = None,
    last_downloaded_at: Optional[str] = None,
    last_seen_mtime: Optional[float] = None,
    server_user_file_id: Optional[int] = None,
    namespace_id: Optional[int] = None,
    is_applying_remote: Optional[int] = None,
    last_command_id: Optional[str] = None,
) -> bool:
    """Создаёт или обновляет состояние файла."""
    try:
        existing = get_file_state(db, relative_path) or {}
        with connect(db.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                    INSERT OR REPLACE INTO files (
                        token, relative_path, content_hash, last_uploaded_at, last_downloaded_at,
                        last_seen_mtime, server_user_file_id, namespace_id,
                        is_applying_remote, last_command_id, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """,
                (
                    db.token,
                    relative_path,
                    content_hash if content_hash is not None else existing.get("content_hash"),
                    last_uploaded_at if last_uploaded_at is not None else existing.get("last_uploaded_at"),
                    last_downloaded_at if last_downloaded_at is not None else existing.get("last_downloaded_at"),
                    last_seen_mtime if last_seen_mtime is not None else existing.get("last_seen_mtime"),
                    server_user_file_id if server_user_file_id is not None else existing.get("server_user_file_id"),
                    namespace_id if namespace_id is not None else existing.get("namespace_id"),
                    int(is_applying_remote) if is_applying_remote is not None else int(existing.get("is_applying_remote", False)),
                    last_command_id if last_command_id is not None else existing.get("last_command_id"),
                ),
            )
            conn.commit()
            return True
    except Exception as error:
        logger.error("Ошибка сохранения file_state: %s", error)
        return False


def delete_file_state(db: DBContext, relative_path: str) -> bool:
    """Удаляет состояние файла."""
    try:
        with connect(db.db_path) as conn:
            conn.cursor().execute(
                "DELETE FROM files WHERE token = ? AND relative_path = ?",
                (db.token, relative_path),
            )
            conn.commit()
            return True
    except Exception as error:
        logger.error("Ошибка удаления file_state для %s: %s", relative_path, error)
        return False


def delete_file_states_under_prefix(db: DBContext, relative_prefix: str) -> bool:
    """Удаляет все состояния файлов под префиксом."""
    try:
        with connect(db.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "DELETE FROM files WHERE token = ? AND (relative_path = ? OR relative_path LIKE ?)",
                (db.token, relative_prefix, prefix_like(relative_prefix)),
            )
            conn.commit()
            return True
    except Exception as error:
        logger.error("Ошибка удаления file_state под префиксом %s: %s", relative_prefix, error)
        return False


def rename_file_state(db: DBContext, old_path: str, new_path: str) -> bool:
    """Переименовывает file-state запись."""
    if old_path == new_path:
        return True

    try:
        with connect(db.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                    SELECT content_hash, last_uploaded_at, last_downloaded_at,
                           last_seen_mtime, server_user_file_id, namespace_id,
                           is_applying_remote, last_command_id
                    FROM files
                    WHERE token = ? AND relative_path = ?
                """,
                (db.token, old_path),
            )
            row = cursor.fetchone()
            if not row:
                return False

            cursor.execute(
                "DELETE FROM files WHERE token = ? AND relative_path = ?",
                (db.token, old_path),
            )
            cursor.execute(
                """
                    INSERT OR REPLACE INTO files (
                        token, relative_path, content_hash, last_uploaded_at, last_downloaded_at,
                        last_seen_mtime, server_user_file_id, namespace_id,
                        is_applying_remote, last_command_id, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """,
                (
                    db.token,
                    new_path,
                    row[0],
                    row[1],
                    row[2],
                    row[3],
                    row[4],
                    row[5],
                    row[6],
                    row[7],
                ),
            )
            conn.commit()
            return True
    except Exception as error:
        logger.error("Ошибка rename_file_state %s -> %s: %s", old_path, new_path, error)
        return False


def rename_file_prefix(db: DBContext, old_prefix: str, new_prefix: str) -> bool:
    """Переименовывает файл(ы) под префиксом в таблице files."""
    if old_prefix == new_prefix:
        return True

    try:
        file_states = get_file_states_under_prefix(db, old_prefix)
        if not file_states:
            return False

        updated_states = []
        old_prefix_with_slash = f"{old_prefix}/"
        for state in file_states:
            old_path = state["relative_path"]
            new_path = new_prefix if old_path == old_prefix else old_path.replace(old_prefix_with_slash, f"{new_prefix}/", 1)
            updated_states.append({
                "relative_path": new_path,
                "content_hash": state.get("content_hash"),
                "last_uploaded_at": state.get("last_uploaded_at"),
                "last_downloaded_at": state.get("last_downloaded_at"),
                "last_seen_mtime": state.get("last_seen_mtime"),
                "server_user_file_id": state.get("server_user_file_id"),
                "namespace_id": state.get("namespace_id"),
                "is_applying_remote": state.get("is_applying_remote", False),
                "last_command_id": state.get("last_command_id"),
            })

        with connect(db.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "DELETE FROM files WHERE token = ? AND (relative_path = ? OR relative_path LIKE ?)",
                (db.token, old_prefix, prefix_like(old_prefix)),
            )
            cursor.executemany(
                """
                    INSERT INTO files (
                        token, relative_path, content_hash, last_uploaded_at, last_downloaded_at,
                        last_seen_mtime, server_user_file_id, namespace_id,
                        is_applying_remote, last_command_id, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """,
                [
                    (
                        db.token,
                        state["relative_path"],
                        state.get("content_hash"),
                        state.get("last_uploaded_at"),
                        state.get("last_downloaded_at"),
                        state.get("last_seen_mtime"),
                        state.get("server_user_file_id"),
                        state.get("namespace_id"),
                        int(state.get("is_applying_remote", False)),
                        state.get("last_command_id"),
                    )
                    for state in updated_states
                ],
            )
            conn.commit()
            return True
    except Exception as error:
        logger.error("Ошибка rename_file_prefix %s -> %s: %s", old_prefix, new_prefix, error)
        return False


def mark_file_remote_apply(db: DBContext, relative_path: str, is_applying_remote: bool) -> bool:
    """Помечает файл как временно обновляемый со стороны сервера."""
    if not is_applying_remote:
        existing = get_file_state(db, relative_path)
        if not existing:
            return True
        if existing and not any(
            existing.get(field) is not None
            for field in (
                "content_hash",
                "last_uploaded_at",
                "last_downloaded_at",
                "last_seen_mtime",
                "server_user_file_id",
                "namespace_id",
                "last_command_id",
            )
        ):
            return delete_file_state(db, relative_path)
    return upsert_file_state(
        db,
        relative_path,
        is_applying_remote=1 if is_applying_remote else 0,
    )
