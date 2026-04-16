"""Операции над таблицей folders."""

import logging
from typing import Dict, List, Optional

from .common import (
    DBContext,
    FOLDER_STATE_COLUMNS,
    connect,
    fetch_existing_folder_state,
    folder_state_from_row,
    prefix_like,
)


logger = logging.getLogger("app.core.database")


def get_folder_state(db: DBContext, relative_path: str) -> Optional[Dict]:
    """Возвращает состояние папки по относительному пути."""
    return fetch_existing_folder_state(db, relative_path)


def has_folder_path(db: DBContext, relative_path: str) -> bool:
    """Проверяет, известен ли путь как папка."""
    return get_folder_state(db, relative_path) is not None


def get_all_folder_states(db: DBContext) -> List[Dict]:
    """Возвращает все folder-state записи текущего токена."""
    try:
        with connect(db.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                f"""
                    SELECT {FOLDER_STATE_COLUMNS}
                    FROM folders
                    WHERE token = ?
                """,
                (db.token,),
            )
            return [folder_state_from_row(row) for row in cursor.fetchall()]
    except Exception as error:
        logger.error("Ошибка получения списка folder_state: %s", error)
        return []


def get_folder_states_under_prefix(db: DBContext, relative_prefix: str) -> List[Dict]:
    """Возвращает все folder-state записи под указанным префиксом."""
    if not relative_prefix:
        return get_all_folder_states(db)

    try:
        with connect(db.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                f"""
                    SELECT {FOLDER_STATE_COLUMNS}
                    FROM folders
                    WHERE token = ? AND (relative_path = ? OR relative_path LIKE ?)
                """,
                (db.token, relative_prefix, prefix_like(relative_prefix)),
            )
            return [folder_state_from_row(row) for row in cursor.fetchall()]
    except Exception as error:
        logger.error("Ошибка получения folder_state под префиксом %s: %s", relative_prefix, error)
        return []


def upsert_folder_state(
    db: DBContext,
    relative_path: str,
    namespace_id: Optional[int] = None,
    parent_relative_path: Optional[str] = None,
    kind: Optional[str] = None,
    is_applying_remote: Optional[int] = None,
    last_command_id: Optional[str] = None,
) -> bool:
    """Создаёт или обновляет состояние папки."""
    try:
        existing = get_folder_state(db, relative_path) or {}
        with connect(db.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                    INSERT OR REPLACE INTO folders (
                        token, relative_path, namespace_id, parent_relative_path,
                        kind, is_applying_remote, last_command_id, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """,
                (
                    db.token,
                    relative_path,
                    namespace_id if namespace_id is not None else existing.get("namespace_id"),
                    parent_relative_path if parent_relative_path is not None else existing.get("parent_relative_path"),
                    kind if kind is not None else existing.get("kind"),
                    int(is_applying_remote) if is_applying_remote is not None else int(existing.get("is_applying_remote", False)),
                    last_command_id if last_command_id is not None else existing.get("last_command_id"),
                ),
            )
            conn.commit()
            return True
    except Exception as error:
        logger.error("Ошибка сохранения folder_state: %s", error)
        return False


def replace_folder_states(db: DBContext, folder_states: List[Dict]) -> bool:
    """Полностью заменяет folder-state для текущего токена."""
    try:
        with connect(db.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM folders WHERE token = ?", (db.token,))
            cursor.executemany(
                """
                    INSERT INTO folders (
                        token, relative_path, namespace_id, parent_relative_path,
                        kind, is_applying_remote, last_command_id, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """,
                [
                    (
                        db.token,
                        state["relative_path"],
                        state.get("namespace_id"),
                        state.get("parent_relative_path"),
                        state.get("kind"),
                        int(state.get("is_applying_remote", False)),
                        state.get("last_command_id"),
                    )
                    for state in folder_states
                ],
            )
            conn.commit()
            return True
    except Exception as error:
        logger.error("Ошибка замены folder_state: %s", error)
        return False


def delete_folder_state(db: DBContext, relative_path: str) -> bool:
    """Удаляет состояние папки."""
    try:
        with connect(db.db_path) as conn:
            conn.cursor().execute(
                "DELETE FROM folders WHERE token = ? AND relative_path = ?",
                (db.token, relative_path),
            )
            conn.commit()
            return True
    except Exception as error:
        logger.error("Ошибка удаления folder_state для %s: %s", relative_path, error)
        return False


def delete_folder_states_under_prefix(db: DBContext, relative_prefix: str) -> bool:
    """Удаляет все состояния папок под префиксом."""
    try:
        with connect(db.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "DELETE FROM folders WHERE token = ? AND (relative_path = ? OR relative_path LIKE ?)",
                (db.token, relative_prefix, prefix_like(relative_prefix)),
            )
            conn.commit()
            return True
    except Exception as error:
        logger.error("Ошибка удаления folder_state под префиксом %s: %s", relative_prefix, error)
        return False


def rename_folder_prefix(db: DBContext, old_prefix: str, new_prefix: str) -> bool:
    """Переименовывает папку и все дочерние пути в таблице folders."""
    if old_prefix == new_prefix:
        return True

    try:
        folder_states = get_folder_states_under_prefix(db, old_prefix)
        if not folder_states:
            return False

        updated_states = []
        old_prefix_with_slash = f"{old_prefix}/"
        for state in folder_states:
            old_path = state["relative_path"]
            new_path = new_prefix if old_path == old_prefix else old_path.replace(old_prefix_with_slash, f"{new_prefix}/", 1)
            parent_relative_path = state.get("parent_relative_path")
            if parent_relative_path == old_prefix:
                new_parent_relative_path = new_prefix
            elif parent_relative_path and parent_relative_path.startswith(old_prefix_with_slash):
                new_parent_relative_path = parent_relative_path.replace(old_prefix_with_slash, f"{new_prefix}/", 1)
            else:
                new_parent_relative_path = parent_relative_path

            updated_states.append({
                "relative_path": new_path,
                "namespace_id": state.get("namespace_id"),
                "parent_relative_path": new_parent_relative_path,
                "kind": state.get("kind"),
                "is_applying_remote": state.get("is_applying_remote", False),
                "last_command_id": state.get("last_command_id"),
            })

        with connect(db.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "DELETE FROM folders WHERE token = ? AND (relative_path = ? OR relative_path LIKE ?)",
                (db.token, old_prefix, prefix_like(old_prefix)),
            )
            cursor.executemany(
                """
                    INSERT INTO folders (
                        token, relative_path, namespace_id, parent_relative_path,
                        kind, is_applying_remote, last_command_id, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """,
                [
                    (
                        db.token,
                        state["relative_path"],
                        state.get("namespace_id"),
                        state.get("parent_relative_path"),
                        state.get("kind"),
                        int(state.get("is_applying_remote", False)),
                        state.get("last_command_id"),
                    )
                    for state in updated_states
                ],
            )
            conn.commit()
            return True
    except Exception as error:
        logger.error("Ошибка rename_folder_prefix %s -> %s: %s", old_prefix, new_prefix, error)
        return False


def mark_folder_remote_apply(db: DBContext, relative_path: str, is_applying_remote: bool) -> bool:
    """Помечает папку как временно обновляемую со стороны сервера."""
    if not is_applying_remote:
        existing = get_folder_state(db, relative_path)
        if not existing:
            return True
        if existing and not any(
            existing.get(field) is not None
            for field in (
                "namespace_id",
                "parent_relative_path",
                "kind",
                "last_command_id",
            )
        ):
            return delete_folder_state(db, relative_path)
    return upsert_folder_state(
        db,
        relative_path,
        is_applying_remote=1 if is_applying_remote else 0,
    )
