"""Polling-компонент для применения серверных команд локально на ПК."""

import base64
import logging
import shutil
import threading
import time
from pathlib import Path
from typing import Callable, Optional

from app.core.api_client import SyncAPIClient
from app.core.contracts import SyncCommand
from app.core.database import SettingsDB
from app.core.file_utils import compute_file_hash
from app.core.namespace_constants import (
    INBOX_DIRNAME,
    INBOX_KIND,
    REGULAR_KIND,
    TRASH_DIRNAME,
    TRASH_KIND,
)

logger = logging.getLogger(__name__)


_DELETE_TYPES = {"delete", "delete_file", "remove_file"}
_RENAME_TYPES = {"rename", "rename_file"}
_MOVE_TYPES = {"move", "move_file"}
_TRASH_TYPES = {"trash", "trash_file"}


class CommandPoller:
    """Опрашивает backend и тихо применяет изменения в локальную папку."""

    def __init__(
        self,
        api_client: SyncAPIClient,
        db: SettingsDB,
        local_folder: Path,
        poll_interval: int,
        batch_size: int = 100,
        log_callback: Optional[Callable[[str], None]] = None,
    ):
        self.api_client: SyncAPIClient = api_client
        self.db: SettingsDB = db
        self.local_folder: Path = Path(local_folder)
        self.poll_interval = poll_interval
        self.batch_size = batch_size
        self.log_callback = log_callback
        self._stop_event = threading.Event()

    def log(self, message: str):
        logger.info(message)
        if self.log_callback:
            self.log_callback(message)

    def stop(self):
        """Останавливает polling-цикл."""
        self._stop_event.set()

    def run_forever(self):
        """Основной цикл опроса команд сервера."""
        self.log("Polling команд синхронизации запущен")
        while not self._stop_event.is_set():
            commands = self.api_client.get_pending_commands(limit=self.batch_size)
            for command in commands:
                if self._stop_event.is_set():
                    break
                self._apply_command(command)

            self._stop_event.wait(self.poll_interval)

        self.log("Polling команд синхронизации остановлен")

    def _folder_kind_from_path(self, relative_path: str) -> str:
        if relative_path == INBOX_DIRNAME:
            return INBOX_KIND
        if relative_path == TRASH_DIRNAME:
            return TRASH_KIND
        return REGULAR_KIND

    def _ensure_folder_state(self, relative_path: str, namespace_id: Optional[int] = None, last_command_id: Optional[str] = None):
        if not relative_path:
            return
        parent_path = Path(relative_path).parent.as_posix()
        self.db.upsert_folder_state(
            relative_path,
            namespace_id=namespace_id,
            parent_relative_path=None if parent_path in {".", ""} else parent_path,
            kind=self._folder_kind_from_path(relative_path),
            last_command_id=last_command_id
        )

    def _ensure_folder_chain(self, relative_path: str, namespace_id: Optional[int] = None, last_command_id: Optional[str] = None):
        parts = Path(relative_path).parts
        for index in range(len(parts)):
            folder_path = Path(*parts[: index + 1]).as_posix()
            self._ensure_folder_state(
                folder_path,
                namespace_id=namespace_id if index == len(parts) - 1 else None,
                last_command_id=last_command_id,
            )

    def _resolve_existing_relative_path(self, command: SyncCommand) -> Optional[str]:
        if command.file_id:
            file_state = self.db.get_file_state_by_server_user_file_id(command.file_id)
            if file_state:
                return file_state["relative_path"]
        if command.old_relative_path:
            return command.old_relative_path
        return None

    def _mark_file_paths_remote_apply(self, *relative_paths: Optional[str], is_remote: bool):
        for relative_path in {path for path in relative_paths if path}:
            self.db.mark_file_remote_apply(relative_path, is_remote)

    def _mark_folder_paths_remote_apply(self, *relative_paths: Optional[str], is_remote: bool):
        for relative_path in {path for path in relative_paths if path}:
            self.db.mark_folder_remote_apply(relative_path, is_remote)

    def _set_remote_apply_for_subtree(self, subtree_root_path: str, is_remote: bool):
        self._mark_folder_paths_remote_apply(subtree_root_path, is_remote=is_remote)
        for folder_state in self.db.get_folder_states_under_prefix(subtree_root_path):
            self.db.mark_folder_remote_apply(folder_state["relative_path"], is_remote)
        for file_state in self.db.get_file_states_under_prefix(subtree_root_path):
            self.db.mark_file_remote_apply(file_state["relative_path"], is_remote)

    def _write_command_content(self, command: SyncCommand, target_path: Path):
        if command.content is not None:
            target_path.write_text(command.content, encoding="utf-8")
            return

        if command.content_base64 is not None:
            target_path.write_bytes(base64.b64decode(command.content_base64))
            return

        downloaded = False
        if command.file_id:
            downloaded = self.api_client.download_file_by_id(command.file_id, target_path)
        if not downloaded:
            downloaded = self.api_client.download_file(
                command.relative_path,
                target_path,
                download_url=command.download_url,
            )
        if not downloaded:
            raise RuntimeError(f"Не удалось скачать {command.relative_path}")

    def _update_file_state_from_disk(self, relative_path: str, target_path: Path, command: SyncCommand):
        parent_path = Path(relative_path).parent.as_posix()
        if parent_path not in {".", ""}:
            self._ensure_folder_chain(
                parent_path,
                namespace_id=command.namespace_id,
                last_command_id=command.command_id,
            )
        self.db.upsert_file_state(
            relative_path,
            content_hash=compute_file_hash(target_path),
            last_downloaded_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            last_seen_mtime=target_path.stat().st_mtime,
            server_user_file_id=command.file_id,
            namespace_id=command.namespace_id,
            is_applying_remote=0,
            last_command_id=command.command_id,
        )

    def _apply_command(self, command: SyncCommand):
        if not command.command_id or not command.relative_path:
            return

        if self.db.has_applied_command(command.command_id):
            self.api_client.ack_command(command.command_id, status="already_applied")
            return

        try:
            if command.command_type in _DELETE_TYPES:
                self._apply_delete(command)
            elif command.command_type in _RENAME_TYPES or command.command_type in _MOVE_TYPES:
                self._apply_rename_or_move(command)
            elif command.command_type in _TRASH_TYPES:
                self._apply_trash(command)
            else:
                self._apply_upsert(command)

            self.db.save_applied_command(command.command_id, command.relative_path)
            self.api_client.ack_command(command.command_id, status="applied")
        except Exception as error:
            logger.error("Ошибка применения команды %s: %s", command.command_id, error, exc_info=True)
            self.api_client.ack_command(command.command_id, status="failed", error_message=str(error))

    def _apply_delete(self, command: SyncCommand):
        if command.target_type == "namespace":
            self._apply_delete_namespace(command)
            return

        target_path = self.local_folder / command.relative_path
        self._mark_file_paths_remote_apply(command.relative_path, is_remote=True)
        try:
            if target_path.exists():
                target_path.unlink()
            self.db.delete_file_state(command.relative_path)
            self.log(f"Удален файл по команде сервера: {command.relative_path}")
        finally:
            self._mark_file_paths_remote_apply(command.relative_path, is_remote=False)

    def _apply_delete_namespace(self, command: SyncCommand):
        target_path = self.local_folder / command.relative_path
        self._set_remote_apply_for_subtree(command.relative_path, is_remote=True)
        try:
            if target_path.exists():
                if target_path.is_dir():
                    shutil.rmtree(target_path)
                else:
                    target_path.unlink()
            self.db.delete_file_states_under_prefix(command.relative_path)
            self.db.delete_folder_states_under_prefix(command.relative_path)
            self.log(f"Удалена папка по команде сервера: {command.relative_path}")
        finally:
            self._set_remote_apply_for_subtree(command.relative_path, is_remote=False)

    def _apply_rename_or_move(self, command: SyncCommand):
        if command.target_type == "namespace":
            self._apply_rename_or_move_namespace(command)
            return
        self._apply_rename_or_move_file(command)

    def _apply_rename_or_move_file(self, command: SyncCommand):
        old_relative_path = self._resolve_existing_relative_path(command)
        new_relative_path = command.new_relative_path or command.relative_path
        if not new_relative_path:
            raise RuntimeError("Не указан новый путь для rename/move")

        old_path = self.local_folder / old_relative_path if old_relative_path else None
        new_path = self.local_folder / new_relative_path
        self._mark_file_paths_remote_apply(old_relative_path, new_relative_path, is_remote=True)

        try:
            new_path.parent.mkdir(parents=True, exist_ok=True)

            if old_path and old_path.exists() and old_path != new_path:
                if new_path.exists() and new_path != old_path:
                    new_path.unlink()
                old_path.rename(new_path)
            elif not new_path.exists():
                self._write_command_content(command, new_path)

            if command.content is not None or command.content_base64 is not None:
                self._write_command_content(command, new_path)

            if old_relative_path and old_relative_path != new_relative_path:
                self.db.rename_file_state(old_relative_path, new_relative_path)

            self._update_file_state_from_disk(new_relative_path, new_path, command)
            self.log(f"Применена команда {command.command_type}: {new_relative_path}")
        finally:
            self._mark_file_paths_remote_apply(old_relative_path, new_relative_path, is_remote=False)

    def _apply_rename_or_move_namespace(self, command: SyncCommand):
        old_relative_path = command.old_relative_path or command.relative_path
        new_relative_path = command.new_relative_path or command.relative_path
        if not old_relative_path or not new_relative_path:
            raise RuntimeError("Не указан путь namespace для rename/move")

        old_path = self.local_folder / old_relative_path
        new_path = self.local_folder / new_relative_path
        self._set_remote_apply_for_subtree(old_relative_path, is_remote=True)
        self._set_remote_apply_for_subtree(new_relative_path, is_remote=True)

        try:
            new_path.parent.mkdir(parents=True, exist_ok=True)
            if old_path.exists() and old_path != new_path:
                if new_path.exists() and new_path.is_dir():
                    shutil.rmtree(new_path)
                elif new_path.exists():
                    new_path.unlink()
                old_path.rename(new_path)
            elif not new_path.exists():
                new_path.mkdir(parents=True, exist_ok=True)

            self.db.rename_folder_prefix(old_relative_path, new_relative_path)
            self.db.rename_file_prefix(old_relative_path, new_relative_path)
            self._ensure_folder_chain(
                new_relative_path,
                namespace_id=command.namespace_id,
                last_command_id=command.command_id,
            )
            self.log(f"Применено перемещение папки: {old_relative_path} -> {new_relative_path}")
        finally:
            self._set_remote_apply_for_subtree(old_relative_path, is_remote=False)
            self._set_remote_apply_for_subtree(new_relative_path, is_remote=False)

    def _apply_trash(self, command: SyncCommand):
        old_relative_path = self._resolve_existing_relative_path(command)
        trash_relative_path = command.new_relative_path or command.relative_path
        if not trash_relative_path:
            filename = command.filename or (Path(old_relative_path).name if old_relative_path else None)
            if not filename:
                raise RuntimeError("Не удалось определить путь корзины")
            trash_relative_path = f"{TRASH_DIRNAME}/{filename}"

        if command.target_type == "namespace":
            self._apply_rename_or_move_namespace(
                SyncCommand(
                    command_id=command.command_id,
                    command_type=command.command_type,
                    relative_path=trash_relative_path,
                    old_relative_path=old_relative_path or command.relative_path,
                    new_relative_path=trash_relative_path,
                    namespace_id=command.namespace_id,
                    target_type="namespace",
                    extra=command.extra,
                )
            )
            self.log(f"Папка перемещена в корзину по команде сервера: {trash_relative_path}")
            return

        old_path = self.local_folder / old_relative_path if old_relative_path else None
        trash_path = self.local_folder / trash_relative_path
        self._mark_file_paths_remote_apply(old_relative_path, trash_relative_path, is_remote=True)

        try:
            trash_path.parent.mkdir(parents=True, exist_ok=True)
            if old_path and old_path.exists():
                if trash_path.exists() and trash_path != old_path:
                    trash_path.unlink()
                old_path.rename(trash_path)
            elif not trash_path.exists():
                self._write_command_content(command, trash_path)

            if old_relative_path:
                self.db.rename_file_state(old_relative_path, trash_relative_path)
            self._update_file_state_from_disk(trash_relative_path, trash_path, command)
            self.log(f"Файл перемещён в корзину по команде сервера: {trash_relative_path}")
        finally:
            self._mark_file_paths_remote_apply(old_relative_path, trash_relative_path, is_remote=False)

    def _apply_upsert(self, command: SyncCommand):
        if command.target_type == "namespace":
            target_path = self.local_folder / command.relative_path
            self._mark_folder_paths_remote_apply(command.relative_path, is_remote=True)
            try:
                target_path.mkdir(parents=True, exist_ok=True)
                self._ensure_folder_chain(
                    command.relative_path,
                    namespace_id=command.namespace_id,
                    last_command_id=command.command_id,
                )
                self.log(f"Создана папка по команде сервера: {command.relative_path}")
            finally:
                self._mark_folder_paths_remote_apply(command.relative_path, is_remote=False)
            return

        target_path = self.local_folder / command.relative_path
        target_path.parent.mkdir(parents=True, exist_ok=True)
        self._mark_file_paths_remote_apply(command.relative_path, is_remote=True)

        try:
            self._write_command_content(command, target_path)
            self._update_file_state_from_disk(command.relative_path, target_path, command)
            self.log(f"Применена команда сервера: {command.relative_path}")
        finally:
            self._mark_file_paths_remote_apply(command.relative_path, is_remote=False)
