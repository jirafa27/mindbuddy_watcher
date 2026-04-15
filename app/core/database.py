"""Модуль для работы с базой данных настроек"""

import json
import logging
import sqlite3
from pathlib import Path
from typing import Dict, List, Optional

from app.core.contracts import NamespaceStructureItem

logger = logging.getLogger(__name__)


class SettingsDB:
    """База данных для хранения настроек и состояния синхронизации."""

    def __init__(self, db_path="watcher_settings.db", token: str = ""):
        self.db_path = Path(db_path)
        self.token = token
        self.init_db()

    def set_token(self, token: str) -> None:
        """Устанавливает активный токен для всех sync-операций."""
        self.token = token

    def init_db(self):
        """Создание и миграция таблиц БД"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS settings (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        token TEXT,
                        folder_path TEXT,
                        last_initialized_token TEXT,
                        last_initialized_folder_path TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                ''')

                cursor.execute("PRAGMA table_info(settings)")
                settings_cols = {row[1] for row in cursor.fetchall()}
                if settings_cols and "last_initialized_token" not in settings_cols:
                    cursor.execute("ALTER TABLE settings ADD COLUMN last_initialized_token TEXT")
                if settings_cols and "last_initialized_folder_path" not in settings_cols:
                    cursor.execute("ALTER TABLE settings ADD COLUMN last_initialized_folder_path TEXT")

                cursor.execute("DROP TABLE IF EXISTS sync_state")

                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS files (
                        token TEXT NOT NULL,
                        relative_path TEXT NOT NULL,
                        content_hash TEXT,
                        last_uploaded_at TEXT,
                        last_downloaded_at TEXT,
                        last_seen_mtime REAL,
                        server_user_file_id INTEGER,
                        namespace_id INTEGER,
                        is_applying_remote INTEGER DEFAULT 0,
                        last_command_id TEXT,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        PRIMARY KEY (token, relative_path)
                    )
                ''')

                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS folders (
                        token TEXT NOT NULL,
                        relative_path TEXT NOT NULL,
                        namespace_id INTEGER,
                        parent_relative_path TEXT,
                        kind TEXT,
                        is_applying_remote INTEGER DEFAULT 0,
                        last_command_id TEXT,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        PRIMARY KEY (token, relative_path)
                    )
                ''')

                cursor.execute("PRAGMA table_info(applied_commands)")
                cmd_cols = {row[1] for row in cursor.fetchall()}
                if cmd_cols and "token" not in cmd_cols:
                    cursor.execute("DROP TABLE applied_commands")
                    logger.info("Старая таблица applied_commands удалена для миграции")

                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS applied_commands (
                        token TEXT NOT NULL,
                        command_id TEXT NOT NULL,
                        relative_path TEXT NOT NULL,
                        applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        PRIMARY KEY (token, command_id)
                    )
                ''')

                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS last_sync_snapshot (
                        token TEXT PRIMARY KEY,
                        snapshot_json TEXT NOT NULL,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                ''')

                cursor.execute(
                    "CREATE INDEX IF NOT EXISTS idx_files_remote "
                    "ON files(token, is_applying_remote)"
                )
                cursor.execute(
                    "CREATE INDEX IF NOT EXISTS idx_files_server_user_file_id "
                    "ON files(token, server_user_file_id)"
                )
                cursor.execute(
                    "CREATE INDEX IF NOT EXISTS idx_folders_remote "
                    "ON folders(token, is_applying_remote)"
                )
                cursor.execute(
                    "CREATE INDEX IF NOT EXISTS idx_folders_parent "
                    "ON folders(token, parent_relative_path)"
                )
                conn.commit()
                logger.info("База данных инициализирована: %s", self.db_path)
        except Exception as error:
            logger.error("Ошибка инициализации БД: %s", error)

    def save_settings(self, token=None, folder_path=None):
        """Сохраняет настройки приложения."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT id FROM settings LIMIT 1")
                existing = cursor.fetchone()

                if existing:
                    cursor.execute('''
                        UPDATE settings
                        SET token = COALESCE(?, token),
                            folder_path = COALESCE(?, folder_path),
                            updated_at = CURRENT_TIMESTAMP
                        WHERE id = ?
                    ''', (token, folder_path, existing[0]))
                else:
                    cursor.execute('''
                        INSERT INTO settings (token, folder_path)
                        VALUES (?, ?)
                    ''', (token, folder_path))

                conn.commit()
                logger.info("Настройки сохранены")
                return True
        except Exception as error:
            logger.error("Ошибка сохранения настроек: %s", error)
            return False

    def load_settings(self):
        """Загружает настройки приложения."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT token, folder_path
                    FROM settings
                    ORDER BY id DESC
                    LIMIT 1
                ''')
                row = cursor.fetchone()

                if row:
                    settings = {
                        "token": row[0],
                        "folder_path": row[1],
                    }
                    logger.info("Настройки загружены из БД")
                    return settings

                logger.info("Настройки не найдены в БД")
                return None
        except Exception as error:
            logger.error("Ошибка загрузки настроек: %s", error)
            return None

    def clear_settings(self):
        """Очищает все настройки."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.cursor().execute("DELETE FROM settings")
                conn.commit()
                logger.info("Настройки очищены")
                return True
        except Exception as error:
            logger.error("Ошибка очистки настроек: %s", error)
            return False

    def get_last_initialized_binding(self) -> Optional[Dict[str, Optional[str]]]:
        """Возвращает последнюю успешно инициализированную связку token + folder_path."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT last_initialized_token, last_initialized_folder_path
                    FROM settings
                    ORDER BY id DESC
                    LIMIT 1
                ''')
                row = cursor.fetchone()
                if not row or (not row[0] and not row[1]):
                    return None
                return {
                    "token": row[0],
                    "folder_path": row[1],
                }
        except Exception as error:
            logger.error("Ошибка получения initialized binding: %s", error)
            return None

    def save_last_initialized_binding(self, token: str, folder_path: str) -> bool:
        """Сохраняет последнюю успешно инициализированную связку token + folder_path."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT id FROM settings ORDER BY id DESC LIMIT 1")
                existing = cursor.fetchone()
                if existing:
                    cursor.execute('''
                        UPDATE settings
                        SET last_initialized_token = ?,
                            last_initialized_folder_path = ?,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE id = ?
                    ''', (token, folder_path, existing[0]))
                else:
                    cursor.execute('''
                        INSERT INTO settings (
                            token, folder_path, last_initialized_token, last_initialized_folder_path
                        )
                        VALUES (?, ?, ?, ?)
                    ''', (token, folder_path, token, folder_path))
                conn.commit()
                return True
        except Exception as error:
            logger.error("Ошибка сохранения initialized binding: %s", error)
            return False

    @staticmethod
    def _file_state_from_row(row) -> Dict:
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

    @staticmethod
    def _folder_state_from_row(row) -> Dict:
        return {
            "relative_path": row[0],
            "namespace_id": row[1],
            "parent_relative_path": row[2],
            "kind": row[3],
            "is_applying_remote": bool(row[4]),
            "last_command_id": row[5],
            "updated_at": row[6],
        }

    def get_file_state(self, relative_path: str) -> Optional[Dict]:
        """Возвращает состояние файла по относительному пути."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT relative_path, content_hash, last_uploaded_at, last_downloaded_at,
                           last_seen_mtime, server_user_file_id, namespace_id,
                           is_applying_remote, last_command_id, updated_at
                    FROM files
                    WHERE token = ? AND relative_path = ?
                ''', (self.token, relative_path))
                row = cursor.fetchone()
                return self._file_state_from_row(row) if row else None
        except Exception as error:
            logger.error("Ошибка получения file_state: %s", error)
            return None

    def get_all_file_states(self) -> List[Dict]:
        """Возвращает все file-state записи текущего токена."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT relative_path, content_hash, last_uploaded_at, last_downloaded_at,
                           last_seen_mtime, server_user_file_id, namespace_id,
                           is_applying_remote, last_command_id, updated_at
                    FROM files
                    WHERE token = ?
                ''', (self.token,))
                return [self._file_state_from_row(row) for row in cursor.fetchall()]
        except Exception as error:
            logger.error("Ошибка получения списка file_state: %s", error)
            return []

    def get_file_states_under_prefix(self, relative_prefix: str) -> List[Dict]:
        """Возвращает все file-state записи под указанным префиксом."""
        if not relative_prefix:
            return self.get_all_file_states()

        prefix = f"{relative_prefix}/%"
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT relative_path, content_hash, last_uploaded_at, last_downloaded_at,
                           last_seen_mtime, server_user_file_id, namespace_id,
                           is_applying_remote, last_command_id, updated_at
                    FROM files
                    WHERE token = ? AND (relative_path = ? OR relative_path LIKE ?)
                ''', (self.token, relative_prefix, prefix))
                return [self._file_state_from_row(row) for row in cursor.fetchall()]
        except Exception as error:
            logger.error("Ошибка получения file_state под префиксом %s: %s", relative_prefix, error)
            return []

    def get_file_state_by_server_user_file_id(self, file_id: int) -> Optional[Dict]:
        """Возвращает file-state запись по server_user_file_id."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT relative_path, content_hash, last_uploaded_at, last_downloaded_at,
                           last_seen_mtime, server_user_file_id, namespace_id,
                           is_applying_remote, last_command_id, updated_at
                    FROM files
                    WHERE token = ? AND server_user_file_id = ?
                    LIMIT 1
                ''', (self.token, file_id))
                row = cursor.fetchone()
                return self._file_state_from_row(row) if row else None
        except Exception as error:
            logger.error("Ошибка получения file_state по file_id=%s: %s", file_id, error)
            return None

    def upsert_file_state(
        self,
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
            existing = self.get_file_state(relative_path) or {}
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT OR REPLACE INTO files (
                        token, relative_path, content_hash, last_uploaded_at, last_downloaded_at,
                        last_seen_mtime, server_user_file_id, namespace_id,
                        is_applying_remote, last_command_id, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ''', (
                    self.token,
                    relative_path,
                    content_hash if content_hash is not None else existing.get("content_hash"),
                    last_uploaded_at if last_uploaded_at is not None else existing.get("last_uploaded_at"),
                    last_downloaded_at if last_downloaded_at is not None else existing.get("last_downloaded_at"),
                    last_seen_mtime if last_seen_mtime is not None else existing.get("last_seen_mtime"),
                    server_user_file_id if server_user_file_id is not None else existing.get("server_user_file_id"),
                    namespace_id if namespace_id is not None else existing.get("namespace_id"),
                    int(is_applying_remote) if is_applying_remote is not None else int(existing.get("is_applying_remote", False)),
                    last_command_id if last_command_id is not None else existing.get("last_command_id"),
                ))
                conn.commit()
                return True
        except Exception as error:
            logger.error("Ошибка сохранения file_state: %s", error)
            return False

    def delete_file_state(self, relative_path: str) -> bool:
        """Удаляет состояние файла."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.cursor().execute(
                    "DELETE FROM files WHERE token = ? AND relative_path = ?",
                    (self.token, relative_path),
                )
                conn.commit()
                return True
        except Exception as error:
            logger.error("Ошибка удаления file_state для %s: %s", relative_path, error)
            return False

    def delete_file_states_under_prefix(self, relative_prefix: str) -> bool:
        """Удаляет все состояния файлов под префиксом."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "DELETE FROM files WHERE token = ? AND (relative_path = ? OR relative_path LIKE ?)",
                    (self.token, relative_prefix, f"{relative_prefix}/%"),
                )
                conn.commit()
                return True
        except Exception as error:
            logger.error("Ошибка удаления file_state под префиксом %s: %s", relative_prefix, error)
            return False

    def rename_file_state(self, old_path: str, new_path: str) -> bool:
        """Переименовывает file-state запись."""
        if old_path == new_path:
            return True

        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT content_hash, last_uploaded_at, last_downloaded_at,
                           last_seen_mtime, server_user_file_id, namespace_id,
                           is_applying_remote, last_command_id
                    FROM files
                    WHERE token = ? AND relative_path = ?
                ''', (self.token, old_path))
                row = cursor.fetchone()
                if not row:
                    return False

                cursor.execute(
                    "DELETE FROM files WHERE token = ? AND relative_path = ?",
                    (self.token, old_path),
                )
                cursor.execute('''
                    INSERT OR REPLACE INTO files (
                        token, relative_path, content_hash, last_uploaded_at, last_downloaded_at,
                        last_seen_mtime, server_user_file_id, namespace_id,
                        is_applying_remote, last_command_id, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ''', (
                    self.token,
                    new_path,
                    row[0],
                    row[1],
                    row[2],
                    row[3],
                    row[4],
                    row[5],
                    row[6],
                    row[7],
                ))
                conn.commit()
                return True
        except Exception as error:
            logger.error("Ошибка rename_file_state %s -> %s: %s", old_path, new_path, error)
            return False

    def rename_file_prefix(self, old_prefix: str, new_prefix: str) -> bool:
        """Переименовывает файл(ы) под префиксом в таблице files."""
        if old_prefix == new_prefix:
            return True

        try:
            file_states = self.get_file_states_under_prefix(old_prefix)
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

            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "DELETE FROM files WHERE token = ? AND (relative_path = ? OR relative_path LIKE ?)",
                    (self.token, old_prefix, f"{old_prefix}/%"),
                )
                cursor.executemany('''
                    INSERT INTO files (
                        token, relative_path, content_hash, last_uploaded_at, last_downloaded_at,
                        last_seen_mtime, server_user_file_id, namespace_id,
                        is_applying_remote, last_command_id, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ''', [
                    (
                        self.token,
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
                ])
                conn.commit()
                return True
        except Exception as error:
            logger.error("Ошибка rename_file_prefix %s -> %s: %s", old_prefix, new_prefix, error)
            return False

    def mark_file_remote_apply(self, relative_path: str, is_applying_remote: bool) -> bool:
        """Помечает файл как временно обновляемый со стороны сервера."""
        if not is_applying_remote:
            existing = self.get_file_state(relative_path)
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
                return self.delete_file_state(relative_path)
        return self.upsert_file_state(
            relative_path,
            is_applying_remote=1 if is_applying_remote else 0,
        )

    def get_folder_state(self, relative_path: str) -> Optional[Dict]:
        """Возвращает состояние папки по относительному пути."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT relative_path, namespace_id, parent_relative_path, kind,
                           is_applying_remote, last_command_id, updated_at
                    FROM folders
                    WHERE token = ? AND relative_path = ?
                ''', (self.token, relative_path))
                row = cursor.fetchone()
                return self._folder_state_from_row(row) if row else None
        except Exception as error:
            logger.error("Ошибка получения folder_state: %s", error)
            return None

    def has_folder_path(self, relative_path: str) -> bool:
        """Проверяет, известен ли путь как папка."""
        return self.get_folder_state(relative_path) is not None

    def get_all_folder_states(self) -> List[Dict]:
        """Возвращает все folder-state записи текущего токена."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT relative_path, namespace_id, parent_relative_path, kind,
                           is_applying_remote, last_command_id, updated_at
                    FROM folders
                    WHERE token = ?
                ''', (self.token,))
                return [self._folder_state_from_row(row) for row in cursor.fetchall()]
        except Exception as error:
            logger.error("Ошибка получения списка folder_state: %s", error)
            return []

    def get_folder_states_under_prefix(self, relative_prefix: str) -> List[Dict]:
        """Возвращает все folder-state записи под указанным префиксом."""
        if not relative_prefix:
            return self.get_all_folder_states()

        prefix = f"{relative_prefix}/%"
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT relative_path, namespace_id, parent_relative_path, kind,
                           is_applying_remote, last_command_id, updated_at
                    FROM folders
                    WHERE token = ? AND (relative_path = ? OR relative_path LIKE ?)
                ''', (self.token, relative_prefix, prefix))
                return [self._folder_state_from_row(row) for row in cursor.fetchall()]
        except Exception as error:
            logger.error("Ошибка получения folder_state под префиксом %s: %s", relative_prefix, error)
            return []

    def upsert_folder_state(
        self,
        relative_path: str,
        namespace_id: Optional[int] = None,
        parent_relative_path: Optional[str] = None,
        kind: Optional[str] = None,
        is_applying_remote: Optional[int] = None,
        last_command_id: Optional[str] = None,
    ) -> bool:
        """Создаёт или обновляет состояние папки."""
        try:
            existing = self.get_folder_state(relative_path) or {}
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT OR REPLACE INTO folders (
                        token, relative_path, namespace_id, parent_relative_path,
                        kind, is_applying_remote, last_command_id, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ''', (
                    self.token,
                    relative_path,
                    namespace_id if namespace_id is not None else existing.get("namespace_id"),
                    parent_relative_path if parent_relative_path is not None else existing.get("parent_relative_path"),
                    kind if kind is not None else existing.get("kind"),
                    int(is_applying_remote) if is_applying_remote is not None else int(existing.get("is_applying_remote", False)),
                    last_command_id if last_command_id is not None else existing.get("last_command_id"),
                ))
                conn.commit()
                return True
        except Exception as error:
            logger.error("Ошибка сохранения folder_state: %s", error)
            return False

    def replace_folder_states(self, folder_states: List[Dict]) -> bool:
        """Полностью заменяет folder-state для текущего токена."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("DELETE FROM folders WHERE token = ?", (self.token,))
                cursor.executemany('''
                    INSERT INTO folders (
                        token, relative_path, namespace_id, parent_relative_path,
                        kind, is_applying_remote, last_command_id, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ''', [
                    (
                        self.token,
                        state["relative_path"],
                        state.get("namespace_id"),
                        state.get("parent_relative_path"),
                        state.get("kind"),
                        int(state.get("is_applying_remote", False)),
                        state.get("last_command_id"),
                    )
                    for state in folder_states
                ])
                conn.commit()
                return True
        except Exception as error:
            logger.error("Ошибка замены folder_state: %s", error)
            return False

    def delete_folder_state(self, relative_path: str) -> bool:
        """Удаляет состояние папки."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.cursor().execute(
                    "DELETE FROM folders WHERE token = ? AND relative_path = ?",
                    (self.token, relative_path),
                )
                conn.commit()
                return True
        except Exception as error:
            logger.error("Ошибка удаления folder_state для %s: %s", relative_path, error)
            return False

    def delete_folder_states_under_prefix(self, relative_prefix: str) -> bool:
        """Удаляет все состояния папок под префиксом."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "DELETE FROM folders WHERE token = ? AND (relative_path = ? OR relative_path LIKE ?)",
                    (self.token, relative_prefix, f"{relative_prefix}/%"),
                )
                conn.commit()
                return True
        except Exception as error:
            logger.error("Ошибка удаления folder_state под префиксом %s: %s", relative_prefix, error)
            return False

    def rename_folder_prefix(self, old_prefix: str, new_prefix: str) -> bool:
        """Переименовывает папку и все дочерние пути в таблице folders."""
        if old_prefix == new_prefix:
            return True

        try:
            folder_states = self.get_folder_states_under_prefix(old_prefix)
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

            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "DELETE FROM folders WHERE token = ? AND (relative_path = ? OR relative_path LIKE ?)",
                    (self.token, old_prefix, f"{old_prefix}/%"),
                )
                cursor.executemany('''
                    INSERT INTO folders (
                        token, relative_path, namespace_id, parent_relative_path,
                        kind, is_applying_remote, last_command_id, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ''', [
                    (
                        self.token,
                        state["relative_path"],
                        state.get("namespace_id"),
                        state.get("parent_relative_path"),
                        state.get("kind"),
                        int(state.get("is_applying_remote", False)),
                        state.get("last_command_id"),
                    )
                    for state in updated_states
                ])
                conn.commit()
                return True
        except Exception as error:
            logger.error("Ошибка rename_folder_prefix %s -> %s: %s", old_prefix, new_prefix, error)
            return False

    def mark_folder_remote_apply(self, relative_path: str, is_applying_remote: bool) -> bool:
        """Помечает папку как временно обновляемую со стороны сервера."""
        if not is_applying_remote:
            existing = self.get_folder_state(relative_path)
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
                return self.delete_folder_state(relative_path)
        return self.upsert_folder_state(
            relative_path,
            is_applying_remote=1 if is_applying_remote else 0,
        )

    def save_applied_command(self, command_id: str, relative_path: str) -> bool:
        """Сохраняет факт применения команды для idempotency после рестарта."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT OR REPLACE INTO applied_commands (token, command_id, relative_path, applied_at)
                    VALUES (?, ?, ?, CURRENT_TIMESTAMP)
                ''', (self.token, command_id, relative_path))
                conn.commit()
                return True
        except Exception as error:
            logger.error("Ошибка сохранения applied_command: %s", error)
            return False

    def has_applied_command(self, command_id: str) -> bool:
        """Проверяет, применялась ли команда ранее."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT 1 FROM applied_commands WHERE token = ? AND command_id = ?",
                    (self.token, command_id),
                )
                return cursor.fetchone() is not None
        except Exception as error:
            logger.error("Ошибка проверки applied_command: %s", error)
            return False

    def save_last_sync_snapshot(self, structure: List[NamespaceStructureItem]) -> bool:
        """Сохраняет last_sync_snapshot текущего токена одним JSON-снимком."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT OR REPLACE INTO last_sync_snapshot (
                        token, snapshot_json, updated_at
                    ) VALUES (?, ?, CURRENT_TIMESTAMP)
                ''', (
                    self.token,
                    json.dumps([entry.to_dict() for entry in structure], ensure_ascii=True),
                ))
                conn.commit()
                return True
        except Exception as error:
            logger.error("Ошибка сохранения last_sync_snapshot: %s", error)
            return False

    def get_last_sync_snapshot(self) -> List[NamespaceStructureItem]:
        """Возвращает сохранённый last_sync_snapshot текущего токена."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT snapshot_json
                    FROM last_sync_snapshot
                    WHERE token = ?
                    LIMIT 1
                ''', (self.token,))
                row = cursor.fetchone()
                if not row or not row[0]:
                    return []
                payload = json.loads(row[0])
                if not isinstance(payload, list):
                    return []
                return [NamespaceStructureItem.from_dict(item) for item in payload if item]
        except Exception as error:
            logger.error("Ошибка получения last_sync_snapshot: %s", error)
            return []

    def clear_last_sync_snapshot(self) -> bool:
        """Удаляет last_sync_snapshot текущего токена."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.cursor().execute(
                    "DELETE FROM last_sync_snapshot WHERE token = ?",
                    (self.token,),
                )
                conn.commit()
                return True
        except Exception as error:
            logger.error("Ошибка очистки last_sync_snapshot: %s", error)
            return False

    def clear_sync_data(self) -> bool:
        """Очищает file/folder state и историю команд для текущего токена."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("DELETE FROM files WHERE token = ?", (self.token,))
                cursor.execute("DELETE FROM folders WHERE token = ?", (self.token,))
                cursor.execute("DELETE FROM applied_commands WHERE token = ?", (self.token,))
                conn.commit()
                logger.info("Состояние синхронизации очищено")
                return True
        except Exception as error:
            logger.error("Ошибка очистки состояния синхронизации: %s", error)
            return False
