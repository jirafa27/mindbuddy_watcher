"""Polling-компонент для применения серверных команд локально на ПК."""

import logging
import shutil
import threading
import time
from pathlib import Path
from typing import Callable, Optional

from app.core.api_client import SyncAPIClient
from app.core.contracts import SyncCommand, SyncCommandAckStatus, SyncCommandType
from app.core.db import SettingsDB
from app.core.file_utils import compute_file_hash
from app.core.namespace_constants import (
    INBOX_DIRNAME,
    INBOX_KIND,
    REGULAR_KIND,
    TRASH_DIRNAME,
    TRASH_KIND,
)
from app.core.sync.namespace_utils import build_path_by_namespace_id, flatten_namespace_list

logger = logging.getLogger(__name__)


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

    def _process_pending_command_batch(self) -> int:
        commands = self.api_client.get_pending_commands(limit=self.batch_size)
        for command in commands:
            if self._stop_event.is_set():
                break
            self._apply_command(command)
        return len(commands)

    def drain_pending_commands(self, log_empty: bool = False) -> int:
        """Применяет все накопившиеся pending-команды до опустошения очереди."""
        total_commands = 0
        while not self._stop_event.is_set():
            batch_count = self._process_pending_command_batch()
            total_commands += batch_count
            if batch_count == 0 or batch_count < self.batch_size:
                break

        if total_commands > 0:
            self.log(f"Применены накопившиеся серверные команды: {total_commands}")
        elif log_empty:
            self.log("Накопившихся серверных команд нет")

        return total_commands

    def run_forever(self):
        """Основной цикл опроса команд сервера."""
        self.log("Polling команд синхронизации запущен")
        while not self._stop_event.is_set():
            self.drain_pending_commands()
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
        return None

    def _mark_file_paths_remote_apply(self, *relative_paths: Optional[str], is_remote: bool):
        for relative_path in {path for path in relative_paths if path}:
            self.db.mark_file_remote_apply(relative_path, is_remote)

    def _mark_folder_paths_remote_apply(self, *relative_paths: Optional[str], is_remote: bool):
        for relative_path in {path for path in relative_paths if path}:
            self.db.mark_folder_remote_apply(relative_path, is_remote)

    def _resolve_namespace_local_path(self, namespace_id: int) -> Optional[str]:
        """Находит локальный путь пространства по его ID."""
        for folder_state in self.db.get_all_folder_states():
            if folder_state.get("namespace_id") == namespace_id:
                return folder_state["relative_path"]

        last_snapshot = self.db.get_last_sync_snapshot()
        path_by_namespace_id = build_path_by_namespace_id(last_snapshot)
        if namespace_id in path_by_namespace_id:
            return path_by_namespace_id[namespace_id]
        return None

    def _resolve_server_relative_path_by_file_id(self, file_id: int) -> Optional[str]:
        """Запрашивает у сервера актуальный relative_path файла по его user_file_id."""
        try:
            remote_structure = self.api_client.get_files_server_structure()
        except Exception as error:
            logger.warning(
                "Не удалось получить live server structure для file_id=%s: %s",
                file_id,
                error,
            )
            return None

        for relative_path, remote_meta in flatten_namespace_list(remote_structure).items():
            if remote_meta.file_id == file_id:
                return relative_path
        return None

    def _build_target_file_relative_path(
        self,
        filename: str,
        target_namespace_id: int,
    ) -> str:
        """Создает относительный путь файла в пространстве по его имени и ID пространства."""
        target_namespace_path = self._resolve_namespace_local_path(target_namespace_id)
        if target_namespace_path is None:
            raise RuntimeError(
                f"Не удалось найти локальное пространство для target_namespace_id={target_namespace_id}"
            )
        return f"{target_namespace_path}/{filename}" if target_namespace_path else filename

    def _build_target_namespace_relative_path(
        self,
        namespace_id: int,
        target_parent_id: int,
    ) -> tuple[str, str]:
        current_namespace_path = self._resolve_namespace_local_path(namespace_id)
        if current_namespace_path is None:
            raise RuntimeError(f"Не удалось найти локальную папку для namespace_id={namespace_id}")
        if not current_namespace_path:
            raise RuntimeError("Нельзя перемещать корневой namespace")

        target_parent_path = self._resolve_namespace_local_path(target_parent_id)
        if target_parent_path is None:
            raise RuntimeError(
                f"Не удалось найти локальный parent namespace для target_parent_id={target_parent_id}"
            )

        namespace_name = Path(current_namespace_path).name
        target_namespace_path = (
            f"{target_parent_path}/{namespace_name}"
            if target_parent_path
            else namespace_name
        )
        return current_namespace_path, target_namespace_path

    def _resolve_upsert_target_relative_path(
        self,
        command: SyncCommand,
        existing_relative_path: Optional[str],
    ) -> str:
        if command.namespace_id is not None and command.filename:
            return self._build_target_file_relative_path(command.filename, command.namespace_id)
        if command.file_id is not None:
            server_relative_path = self._resolve_server_relative_path_by_file_id(command.file_id)
            if server_relative_path:
                return server_relative_path
        if command.relative_path:
            return command.relative_path
        if existing_relative_path:
            return existing_relative_path
        raise RuntimeError(
            "Для upsert_file не указан путь файла: ожидается связка "
            "namespace_id + filename, live server path или уже известный local state"
        )

    def _set_remote_apply_for_subtree(self, subtree_root_path: str, is_remote: bool):
        self._mark_folder_paths_remote_apply(subtree_root_path, is_remote=is_remote)
        for folder_state in self.db.get_folder_states_under_prefix(subtree_root_path):
            self.db.mark_folder_remote_apply(folder_state["relative_path"], is_remote)
        for file_state in self.db.get_file_states_under_prefix(subtree_root_path):
            self.db.mark_file_remote_apply(file_state["relative_path"], is_remote)

    def _download_file_for_command(self, command: SyncCommand, target_path: Path) -> None:
        if command.file_id is None:
            raise RuntimeError("Для загрузки файла отсутствует user_file_id")
        if not self.api_client.download_file_by_id(command.file_id, target_path):
            raise RuntimeError(f"Не удалось скачать файл id={command.file_id}")

    def _update_file_state_from_disk(self, relative_path: str, target_path: Path, command: SyncCommand):
        """Обновляет состояние файла в базе данных на основе данных с диска."""
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

    @staticmethod
    def _ensure_file_relocation_applied(
        old_path: Optional[Path],
        new_path: Path,
    ) -> None:
        if not new_path.exists() or not new_path.is_file():
            raise RuntimeError(f"Локальный файл не появился по целевому пути: {new_path}")
        if old_path and old_path != new_path and old_path.exists():
            raise RuntimeError(f"Исходный файл остался на старом пути: {old_path}")

    @staticmethod
    def _ensure_namespace_relocation_applied(
        old_path: Path,
        new_path: Path,
    ) -> None:
        if not new_path.exists() or not new_path.is_dir():
            raise RuntimeError(f"Локальная папка не появилась по целевому пути: {new_path}")
        if old_path != new_path and old_path.exists():
            raise RuntimeError(f"Исходная папка осталась на старом пути: {old_path}")

    def _apply_command(self, command: SyncCommand):
        if not command.command_id:
            return

        if self.db.has_applied_command(command.command_id):
            self.api_client.ack_command(command.command_id, status=SyncCommandAckStatus.ACKED)
            return

        if command.command_type is None:
            message = f"Unsupported command type: {command.extra.get('unsupported_command_type') or 'unknown'}"
            logger.warning(message)
            self.api_client.ack_command(
                command.command_id,
                status=SyncCommandAckStatus.FAILED,
                error_message=message,
            )
            return

        try:
            if command.command_type == SyncCommandType.UPSERT_FILE:
                self._apply_upsert_file(command)
            elif command.command_type == SyncCommandType.MOVE_FILE:
                self._apply_move_file(command)
            elif command.command_type == SyncCommandType.RENAME_FILE:
                self._apply_rename_file(command)
            elif command.command_type == SyncCommandType.TRASH_FILE:
                self._apply_trash_file(command)
            elif command.command_type == SyncCommandType.DELETE_NAMESPACE:
                self._apply_delete_namespace(command)
            elif command.command_type == SyncCommandType.MOVE_NAMESPACE:
                self._apply_move_namespace(command)
            elif command.command_type == SyncCommandType.RENAME_NAMESPACE:
                self._apply_rename_namespace(command)
            else:
                message = f"Unsupported command type: {command.command_type.value}"
                logger.warning(message)
                self.api_client.ack_command(
                    command.command_id,
                    status=SyncCommandAckStatus.FAILED,
                    error_message=message,
                )
                return

            self.db.save_applied_command(command.command_id, command.relative_path)
            self.api_client.ack_command(command.command_id, status=SyncCommandAckStatus.ACKED)
        except Exception as error:
            logger.error("Ошибка применения команды %s: %s", command.command_id, error, exc_info=True)
            self.api_client.ack_command(
                command.command_id,
                status=SyncCommandAckStatus.FAILED,
                error_message=str(error),
            )

    def _apply_delete_namespace(self, command: SyncCommand):
        if not command.relative_path:
            raise RuntimeError("Для delete_namespace не указан relative_path")
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

    def _relocate_file(
        self,
        command: SyncCommand,
        old_relative_path: Optional[str],
        new_relative_path: str,
    ) -> None:
        old_path = self.local_folder / old_relative_path if old_relative_path else None
        new_path = self.local_folder / new_relative_path
        self._mark_file_paths_remote_apply(old_relative_path, new_relative_path, is_remote=True)

        try:
            new_path.parent.mkdir(parents=True, exist_ok=True)

            if not old_path or not old_path.exists():
                if new_path.exists() and new_path.is_file():
                    if old_relative_path and old_relative_path != new_relative_path:
                        self.db.rename_file_state(old_relative_path, new_relative_path)
                    self._update_file_state_from_disk(new_relative_path, new_path, command)
                    self.log(f"Команда {command.command_type} уже была применена локально: {new_relative_path}")
                    return
                raise RuntimeError("Локальный файл для перемещения не найден")
            if old_path != new_path:
                if new_path.exists() and new_path != old_path:
                    new_path.unlink()
                old_path.rename(new_path)

            if old_relative_path and old_relative_path != new_relative_path:
                self.db.rename_file_state(old_relative_path, new_relative_path)

            self._ensure_file_relocation_applied(old_path, new_path)
            self._update_file_state_from_disk(new_relative_path, new_path, command)
            self.log(f"Применена команда {command.command_type}: {new_relative_path}")
        finally:
            self._mark_file_paths_remote_apply(old_relative_path, new_relative_path, is_remote=False)

    def _relocate_namespace(
        self,
        command: SyncCommand,
        old_relative_path: str,
        new_relative_path: str,
    ) -> None:
        old_path = self.local_folder / old_relative_path
        new_path = self.local_folder / new_relative_path
        self._set_remote_apply_for_subtree(old_relative_path, is_remote=True)
        self._set_remote_apply_for_subtree(new_relative_path, is_remote=True)

        try:
            new_path.parent.mkdir(parents=True, exist_ok=True)
            if old_path.exists() and old_path != new_path:
                if new_path.exists():
                    raise RuntimeError(f"Конфликт имени в целевой папке: {new_relative_path}")
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
            self._ensure_namespace_relocation_applied(old_path, new_path)
            self.log(f"Применено перемещение папки: {old_relative_path} -> {new_relative_path}")
        finally:
            self._set_remote_apply_for_subtree(old_relative_path, is_remote=False)
            self._set_remote_apply_for_subtree(new_relative_path, is_remote=False)

    def _apply_move_namespace(self, command: SyncCommand):
        if command.namespace_id is None:
            raise RuntimeError("Для move_namespace не указан namespace_id")
        if command.target_parent_id is None:
            raise RuntimeError("Для move_namespace не указан target_parent_id")

        old_relative_path, new_relative_path = self._build_target_namespace_relative_path(
            command.namespace_id,
            command.target_parent_id,
        )
        old_parent_path = Path(old_relative_path).parent.as_posix()
        current_parent_path = "" if old_parent_path in {".", ""} else old_parent_path
        target_parent_path = self._resolve_namespace_local_path(command.target_parent_id)
        if target_parent_path is None:
            raise RuntimeError(
                f"Не удалось найти локальный parent namespace для target_parent_id={command.target_parent_id}"
            )
        if current_parent_path == target_parent_path:
            self.log(f"Команда {command.command_type} уже была применена локально: {old_relative_path}")
            return
        if new_relative_path.startswith(f"{old_relative_path}/"):
            raise RuntimeError(
                f"Нельзя переместить namespace {old_relative_path} внутрь собственного поддерева"
            )

        self._relocate_namespace(command, old_relative_path, new_relative_path)

    def _apply_rename_namespace(self, command: SyncCommand):
        if command.namespace_id is None:
            raise RuntimeError("Для rename_namespace не указан namespace_id")
        if not command.new_filename:
            raise RuntimeError("Для rename_namespace не указано новое имя")

        old_relative_path = self._resolve_namespace_local_path(command.namespace_id)
        if old_relative_path is None:
            raise RuntimeError(
                f"Не удалось найти локальную папку для rename_namespace по namespace_id={command.namespace_id}"
            )
        if not old_relative_path:
            raise RuntimeError("Нельзя переименовывать корневой namespace")

        parent_relative_path = Path(old_relative_path).parent.as_posix()
        new_relative_path = (
            f"{parent_relative_path}/{command.new_filename}"
            if parent_relative_path not in {".", ""}
            else command.new_filename
        )
        if old_relative_path == new_relative_path:
            self.log(f"Команда {command.command_type} уже была применена локально: {old_relative_path}")
            return

        self._relocate_namespace(command, old_relative_path, new_relative_path)

    def _apply_move_file(self, command: SyncCommand):
        old_relative_path = self._resolve_existing_relative_path(command)
        if not old_relative_path:
            raise RuntimeError(
                f"Не удалось найти локальный файл для move_file по user_file_id={command.file_id}"
            )
        if command.target_namespace_id is None:
            raise RuntimeError("Для move_file не указан target_namespace_id")

        filename = Path(old_relative_path).name
        new_relative_path = self._build_target_file_relative_path(
            filename,
            command.target_namespace_id,
        )
        self._relocate_file(command, old_relative_path, new_relative_path)

    def _apply_rename_file(self, command: SyncCommand):
        old_relative_path = self._resolve_existing_relative_path(command)
        if not old_relative_path:
            raise RuntimeError(
                f"Не удалось найти локальный файл для rename_file по user_file_id={command.file_id}"
            )
        if not command.new_filename:
            raise RuntimeError("Для rename_file не указан new_filename")

        parent_relative_path = Path(old_relative_path).parent.as_posix()
        new_relative_path = (
            f"{parent_relative_path}/{command.new_filename}"
            if parent_relative_path not in {".", ""}
            else command.new_filename
        )
        self._relocate_file(command, old_relative_path, new_relative_path)

    def _apply_trash_file(self, command: SyncCommand):
        old_relative_path = self._resolve_existing_relative_path(command)
        if not old_relative_path:
            raise RuntimeError(
                f"Не удалось найти локальный файл для trash_file по user_file_id={command.file_id}"
            )
        if command.target_namespace_id is None:
            raise RuntimeError("Для trash_file не указан target_namespace_id")

        filename = Path(old_relative_path).name
        trash_relative_path = self._build_target_file_relative_path(
            filename,
            command.target_namespace_id,
        )

        old_path = self.local_folder / old_relative_path if old_relative_path else None
        trash_path = self.local_folder / trash_relative_path
        self._mark_file_paths_remote_apply(old_relative_path, trash_relative_path, is_remote=True)

        try:
            trash_path.parent.mkdir(parents=True, exist_ok=True)
            if old_path and old_path.exists():
                if trash_path.exists() and trash_path != old_path:
                    trash_path.unlink()
                old_path.rename(trash_path)
            else:
                raise RuntimeError(f"Локальный файл для trash-команды не найден: {old_relative_path}")

            if old_relative_path:
                self.db.rename_file_state(old_relative_path, trash_relative_path)
            self._ensure_file_relocation_applied(old_path, trash_path)
            self._update_file_state_from_disk(trash_relative_path, trash_path, command)
            self.log(f"Файл перемещён в корзину по команде сервера: {trash_relative_path}")
        finally:
            self._mark_file_paths_remote_apply(old_relative_path, trash_relative_path, is_remote=False)

    def _apply_upsert_file(self, command: SyncCommand):
        if command.file_id is None:
            raise RuntimeError("Для upsert_file не указан user_file_id")
        if not command.content_hash:
            raise RuntimeError("Для upsert_file не указан content_hash")

        existing_relative_path = self._resolve_existing_relative_path(command)
        old_path = self.local_folder / existing_relative_path if existing_relative_path else None
        target_relative_path = self._resolve_upsert_target_relative_path(command, existing_relative_path)
        target_path = self.local_folder / target_relative_path
        self._mark_file_paths_remote_apply(existing_relative_path, target_relative_path, is_remote=True)

        try:
            target_path.parent.mkdir(parents=True, exist_ok=True)

            if old_path and old_path.exists() and old_path != target_path:
                if target_path.exists() and target_path != old_path:
                    target_path.unlink()
                old_path.rename(target_path)

            local_exists = target_path.exists() and target_path.is_file()
            local_hash = compute_file_hash(target_path) if local_exists else None
            should_download = not local_exists or local_hash != command.content_hash
            if should_download:
                self._download_file_for_command(command, target_path)

            if existing_relative_path and existing_relative_path != target_relative_path:
                self.db.rename_file_state(existing_relative_path, target_relative_path)

            self._ensure_file_relocation_applied(old_path if old_path and old_path != target_path else None, target_path)
            self._update_file_state_from_disk(target_relative_path, target_path, command)
            self.log(f"Применена команда сервера: {target_relative_path}")
        finally:
            self._mark_file_paths_remote_apply(existing_relative_path, target_relative_path, is_remote=False)
