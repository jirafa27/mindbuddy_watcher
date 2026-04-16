"""Фасад локальной SQLite БД watcher."""

import logging
from pathlib import Path
from typing import Dict, List, Optional

from app.core.contracts import NamespaceStructureItem

from . import applied_commands, files, folders, last_sync_snapshot, settings
from .common import connect
from .schema import init_db as init_db_schema


logger = logging.getLogger("app.core.database")


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
        """Создание и миграция таблиц БД."""
        init_db_schema(self.db_path)

    def save_settings(self, token=None, folder_path=None):
        return settings.save_settings(self, token=token, folder_path=folder_path)

    def load_settings(self):
        return settings.load_settings(self)

    def clear_settings(self):
        return settings.clear_settings(self)

    def get_last_initialized_binding(self) -> Optional[Dict[str, Optional[str]]]:
        return settings.get_last_initialized_binding(self)

    def save_last_initialized_binding(self, token: str, folder_path: str) -> bool:
        return settings.save_last_initialized_binding(self, token, folder_path)

    def get_file_state(self, relative_path: str) -> Optional[Dict]:
        return files.get_file_state(self, relative_path)

    def get_all_file_states(self) -> List[Dict]:
        return files.get_all_file_states(self)

    def get_file_states_under_prefix(self, relative_prefix: str) -> List[Dict]:
        return files.get_file_states_under_prefix(self, relative_prefix)

    def get_file_state_by_server_user_file_id(self, file_id: int) -> Optional[Dict]:
        return files.get_file_state_by_server_user_file_id(self, file_id)

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
        return files.upsert_file_state(
            self,
            relative_path,
            content_hash=content_hash,
            last_uploaded_at=last_uploaded_at,
            last_downloaded_at=last_downloaded_at,
            last_seen_mtime=last_seen_mtime,
            server_user_file_id=server_user_file_id,
            namespace_id=namespace_id,
            is_applying_remote=is_applying_remote,
            last_command_id=last_command_id,
        )

    def delete_file_state(self, relative_path: str) -> bool:
        return files.delete_file_state(self, relative_path)

    def delete_file_states_under_prefix(self, relative_prefix: str) -> bool:
        return files.delete_file_states_under_prefix(self, relative_prefix)

    def rename_file_state(self, old_path: str, new_path: str) -> bool:
        return files.rename_file_state(self, old_path, new_path)

    def rename_file_prefix(self, old_prefix: str, new_prefix: str) -> bool:
        return files.rename_file_prefix(self, old_prefix, new_prefix)

    def mark_file_remote_apply(self, relative_path: str, is_applying_remote: bool) -> bool:
        return files.mark_file_remote_apply(self, relative_path, is_applying_remote)

    def get_folder_state(self, relative_path: str) -> Optional[Dict]:
        return folders.get_folder_state(self, relative_path)

    def has_folder_path(self, relative_path: str) -> bool:
        return folders.has_folder_path(self, relative_path)

    def get_all_folder_states(self) -> List[Dict]:
        return folders.get_all_folder_states(self)

    def get_folder_states_under_prefix(self, relative_prefix: str) -> List[Dict]:
        return folders.get_folder_states_under_prefix(self, relative_prefix)

    def upsert_folder_state(
        self,
        relative_path: str,
        namespace_id: Optional[int] = None,
        parent_relative_path: Optional[str] = None,
        kind: Optional[str] = None,
        is_applying_remote: Optional[int] = None,
        last_command_id: Optional[str] = None,
    ) -> bool:
        return folders.upsert_folder_state(
            self,
            relative_path,
            namespace_id=namespace_id,
            parent_relative_path=parent_relative_path,
            kind=kind,
            is_applying_remote=is_applying_remote,
            last_command_id=last_command_id,
        )

    def replace_folder_states(self, folder_states: List[Dict]) -> bool:
        return folders.replace_folder_states(self, folder_states)

    def delete_folder_state(self, relative_path: str) -> bool:
        return folders.delete_folder_state(self, relative_path)

    def delete_folder_states_under_prefix(self, relative_prefix: str) -> bool:
        return folders.delete_folder_states_under_prefix(self, relative_prefix)

    def rename_folder_prefix(self, old_prefix: str, new_prefix: str) -> bool:
        return folders.rename_folder_prefix(self, old_prefix, new_prefix)

    def mark_folder_remote_apply(self, relative_path: str, is_applying_remote: bool) -> bool:
        return folders.mark_folder_remote_apply(self, relative_path, is_applying_remote)

    def save_applied_command(self, command_id: str, relative_path: str) -> bool:
        return applied_commands.save_applied_command(self, command_id, relative_path)

    def has_applied_command(self, command_id: str) -> bool:
        return applied_commands.has_applied_command(self, command_id)

    def save_last_sync_snapshot(self, structure: List[NamespaceStructureItem]) -> bool:
        return last_sync_snapshot.save_last_sync_snapshot(self, structure)

    def get_last_sync_snapshot(self) -> List[NamespaceStructureItem]:
        return last_sync_snapshot.get_last_sync_snapshot(self)

    def clear_last_sync_snapshot(self) -> bool:
        return last_sync_snapshot.clear_last_sync_snapshot(self)

    def clear_sync_data(self) -> bool:
        """Очищает file/folder state и историю команд для текущего токена."""
        try:
            with connect(self.db_path) as conn:
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
