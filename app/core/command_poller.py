"""Polling-компонент для применения серверных команд локально на ПК."""

import base64
import logging
import threading
import time
from pathlib import Path
from typing import Callable, Optional

from app.core.contracts import SyncCommand
from app.core.file_utils import compute_file_hash

logger = logging.getLogger(__name__)


_DELETE_TYPES = {"delete", "delete_file", "remove_file"}
_RENAME_TYPES = {"rename", "rename_file"}
_MOVE_TYPES = {"move", "move_file"}
_TRASH_TYPES = {"trash", "trash_file"}


class CommandPoller:
    """Опрашивает backend и тихо применяет изменения в локальную папку."""

    def __init__(
        self,
        api_client,
        db,
        local_folder: Path,
        poll_interval: int,
        batch_size: int = 100,
        reconcile_every: int = 6,
        file_sync=None,
        log_callback: Optional[Callable[[str], None]] = None,
    ):
        self.api_client = api_client
        self.db = db
        self.local_folder = Path(local_folder)
        self.poll_interval = poll_interval
        self.batch_size = batch_size
        self.reconcile_every = reconcile_every
        self.file_sync = file_sync
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
        poll_count = 0
        while not self._stop_event.is_set():
            commands = self.api_client.get_pending_commands(limit=self.batch_size)
            for command in commands:
                if self._stop_event.is_set():
                    break
                self._apply_command(command)

            poll_count += 1
            if self.file_sync and self.reconcile_every > 0 and poll_count % self.reconcile_every == 0:
                self._reconcile()

            self._stop_event.wait(self.poll_interval)

        self.log("Polling команд синхронизации остановлен")

    def _reconcile(self):
        """Периодическая полная сверка локального состояния с сервером."""
        try:
            stats = self.file_sync.sync(callback=self.log_callback)
            if stats["downloaded"] > 0 or stats.get("renamed", 0) > 0:
                self.log(
                    f"Reconcile: скачано {stats['downloaded']}, "
                    f"переименовано/перемещено {stats.get('renamed', 0)}"
                )
        except Exception as error:
            logger.error("Ошибка reconcile: %s", error, exc_info=True)

    def _resolve_existing_relative_path(self, command: SyncCommand) -> Optional[str]:
        if command.file_id:
            sync_state = self.db.get_sync_state_by_file_id(command.file_id)
            if sync_state:
                return sync_state["relative_path"]
        if command.old_relative_path:
            return command.old_relative_path
        return None

    def _mark_paths_remote_apply(self, *relative_paths: Optional[str], is_remote: bool):
        for relative_path in {path for path in relative_paths if path}:
            self.db.mark_remote_apply(relative_path, is_remote)

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

    def _update_sync_state_from_disk(self, relative_path: str, target_path: Path, command: SyncCommand):
        self.db.upsert_sync_state(
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
        target_path = self.local_folder / command.relative_path
        self._mark_paths_remote_apply(command.relative_path, is_remote=True)
        try:
            if target_path.exists():
                target_path.unlink()
            self.db.upsert_sync_state(
                command.relative_path,
                content_hash=None,
                last_downloaded_at=None,
                is_applying_remote=0,
                last_command_id=command.command_id,
            )
            self.log(f"Удален файл по команде сервера: {command.relative_path}")
        finally:
            self._mark_paths_remote_apply(command.relative_path, is_remote=False)

    def _apply_rename_or_move(self, command: SyncCommand):
        old_relative_path = self._resolve_existing_relative_path(command)
        new_relative_path = command.new_relative_path or command.relative_path
        if not new_relative_path:
            raise RuntimeError("Не указан новый путь для rename/move")

        old_path = self.local_folder / old_relative_path if old_relative_path else None
        new_path = self.local_folder / new_relative_path
        self._mark_paths_remote_apply(old_relative_path, new_relative_path, is_remote=True)

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
                self.db.rename_sync_state(old_relative_path, new_relative_path)

            self._update_sync_state_from_disk(new_relative_path, new_path, command)
            self.log(f"Применена команда {command.command_type}: {new_relative_path}")
        finally:
            self._mark_paths_remote_apply(old_relative_path, new_relative_path, is_remote=False)

    def _apply_trash(self, command: SyncCommand):
        old_relative_path = self._resolve_existing_relative_path(command)
        trash_relative_path = command.new_relative_path or command.relative_path
        if not trash_relative_path:
            filename = command.filename or (Path(old_relative_path).name if old_relative_path else None)
            if not filename:
                raise RuntimeError("Не удалось определить путь корзины")
            trash_relative_path = f"Trash/{filename}"

        old_path = self.local_folder / old_relative_path if old_relative_path else None
        trash_path = self.local_folder / trash_relative_path
        self._mark_paths_remote_apply(old_relative_path, trash_relative_path, is_remote=True)

        try:
            trash_path.parent.mkdir(parents=True, exist_ok=True)
            if old_path and old_path.exists():
                if trash_path.exists() and trash_path != old_path:
                    trash_path.unlink()
                old_path.rename(trash_path)
            elif not trash_path.exists():
                self._write_command_content(command, trash_path)

            if old_relative_path:
                self.db.rename_sync_state(old_relative_path, trash_relative_path)
            self.log(f"Файл перемещён в корзину по команде сервера: {trash_relative_path}")
        finally:
            self._mark_paths_remote_apply(old_relative_path, trash_relative_path, is_remote=False)

    def _apply_upsert(self, command: SyncCommand):
        target_path = self.local_folder / command.relative_path
        target_path.parent.mkdir(parents=True, exist_ok=True)
        self._mark_paths_remote_apply(command.relative_path, is_remote=True)

        try:
            self._write_command_content(command, target_path)
            self._update_sync_state_from_disk(command.relative_path, target_path, command)
            self.log(f"Применена команда сервера: {command.relative_path}")
        finally:
            self._mark_paths_remote_apply(command.relative_path, is_remote=False)
