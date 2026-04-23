"""Главный класс приложения."""

import logging
import threading
from pathlib import Path
from typing import List, Optional

from app import config
from app.core.api_client import SyncAPIClient
from app.core.command_poller import CommandPoller
from app.core.contracts import SyncMismatchReport, SyncMismatchResolution
from app.core.file_utils import is_temporary_file, normalize_relative_path
from app.core.local_event_reconciler import LocalEventReconciler
from app.core.logger import add_log_handler, configure_logging
from app.core.namespace_constants import (
    INBOX_DIRNAME,
    PROTECTED_NAMESPACE_KINDS,
    PROTECTED_NAMESPACE_NAMES,
    TRASH_DIRNAME,
    VAULT_ROOT_KIND,
)
from app.core.sync import FileSync, SyncStrategy
from app.core.sync.namespace_utils import (
    build_namespace_path_index,
    flatten_namespace_list,
)
from app.core.uploader import FileUploader
from app.core.watcher import FileWatcher
from app.gui import WatcherGUI

configure_logging()
logger = logging.getLogger(__name__)


class MindBuddyWatcher:
    """Оркестратор desktop watcher c polling-синхронизацией."""

    def __init__(self):
        self.gui = WatcherGUI(self)
        add_log_handler(self.gui)
        self.api_client = None
        self.file_watcher = None
        self.file_uploader = None
        self.file_sync = None
        self.command_poller = None
        self.command_poller_thread = None
        self.running = False
        self._ignored_folder_moves = set()
        self._root_folder_recovery_in_progress = False
        self.local_event_reconciler = LocalEventReconciler(
            grace_period_seconds=15.0,
            has_prefix_path=self._has_prefix_path,
            on_finalize_file_delete=self._finalize_local_file_delete,
            on_finalize_folder_delete=self._finalize_local_folder_delete,
        )

    def _build_services(self):
        self.api_client = SyncAPIClient(
            base_url=config.API_BASE_URL,
            token=self.gui.token,
            device_id=config.DEFAULT_DEVICE_ID,
            timeout=config.REQUEST_TIMEOUT_SECONDS,
        )
        self.file_uploader = FileUploader(
            api_client=self.api_client,
            local_folder=self.gui.selected_folder,
            db=self.gui.db,
        )
        self.file_sync = FileSync(
            api_client=self.api_client,
            local_folder=self.gui.selected_folder,
            uploader=self.file_uploader,
            db=self.gui.db,
        )
        self.command_poller = CommandPoller(
            api_client=self.api_client,
            db=self.gui.db,
            local_folder=self.gui.selected_folder,
            poll_interval=config.SYNC_POLL_INTERVAL_SECONDS,
            batch_size=config.SYNC_COMMANDS_BATCH_SIZE,
        )

    def _current_binding(self):
        """Возвращает текущую связку token + folder_path."""
        return {
            "token": self.gui.token,
            "folder_path": str(self.gui.selected_folder) if self.gui.selected_folder else None,
        }

    def _restore_controls_after_cancel(self):
        """Восстанавливает кнопки после отмены первичной инициализации."""
        self.running = False
        self.gui.root.after(0, lambda: (
            self.gui.start_button.config(state="normal"),
            self.gui.stop_button.config(state="disabled"),
        ))

    def _ask_initial_sync_strategy(self):
        """Показывает диалог выбора стратегии, когда обе стороны непустые."""

        strategy_result: Optional[SyncStrategy] = None
        done_event = threading.Event()

        def show_dialog():
            nonlocal strategy_result
            chosen = self.gui.ask_initial_sync_strategy()
            if chosen and not self.gui.confirm_initial_sync_replace(chosen):
                chosen = None
            strategy_result = chosen
            done_event.set()

        self.gui.root.after(0, show_dialog)
        done_event.wait()
        return strategy_result

    def _ask_sync_mismatch_resolutions(
        self,
        mismatch_report: SyncMismatchReport,
    ) -> Optional[List[SyncMismatchResolution]]:
        resolutions: Optional[List[SyncMismatchResolution]] = None
        done_event = threading.Event()

        def show_dialog():
            nonlocal resolutions
            resolutions = self.gui.ask_sync_mismatch_resolutions(mismatch_report)
            done_event.set()

        self.gui.root.after(0, show_dialog)
        done_event.wait()
        return resolutions

    def _resolve_sync_mismatches(self, mismatch_report: Optional[SyncMismatchReport]) -> bool:
        if not mismatch_report or mismatch_report.is_empty():
            return True

        self.gui.log(
            f"Обнаружены расхождения между ПК и сервером: {len(mismatch_report.items)}. "
            "Нужно выбрать, какую версию применить."
        )
        resolutions = self._ask_sync_mismatch_resolutions(mismatch_report)
        if resolutions is None:
            self.gui.log("Разрешение рассинхрона отменено пользователем")
            self.gui.update_status("Синхронизация отменена", "orange")
            self._restore_controls_after_cancel()
            return False

        self.file_sync.apply_sync_mismatch_resolutions(resolutions)
        remaining_report = self.file_sync.refresh_last_sync_snapshot_if_synced()
        if remaining_report and not remaining_report.is_empty():
            self.gui.log(
                f"После применения выбранных действий остались расхождения: {len(remaining_report.items)}"
            )
            self.gui.update_status("Есть расхождения", "orange")
        else:
            self.gui.log("Расхождения между ПК и сервером устранены")
        return True

    def _run_initial_sync(self):
        """Запускает первичную инициализацию новой связки token + folder."""
        
        self.gui.log("Обнаружена новая связка токена и папки, запускаю первичную инициализацию синхронизации...")
        local_structure = self.file_sync.get_local_structure()
        remote_structure = self.api_client.get_files_server_structure()
        local_files = self.file_sync.get_local_file_index()
        remote_files = flatten_namespace_list(remote_structure)

        is_empty_local = len(local_files) == 0 and all(
            namespace.kind == VAULT_ROOT_KIND or namespace.name in PROTECTED_NAMESPACE_NAMES
            for namespace in local_structure
        )
        is_empty_remote = len(remote_files) == 0 and all(
            namespace.kind == VAULT_ROOT_KIND or namespace.name in PROTECTED_NAMESPACE_NAMES
            for namespace in remote_structure
        )

        if is_empty_local and is_empty_remote:
            self.gui.log("ПК и сервер пусты, пропускаю первичную инициализацию синхронизации...")
            strategy = SyncStrategy.SKIP
        elif is_empty_local:
            strategy = SyncStrategy.SERVER_PRIORITY
            self.gui.log("ПК пуст, загружаю файлы с сервера...")
        elif is_empty_remote:
            strategy = SyncStrategy.PC_PRIORITY
            self.gui.log("Сервер пуст, загружаю файлы с ПК...")
        else:
            strategy = self._ask_initial_sync_strategy()
            if not strategy:
                self.gui.log("Первичная синхронизация отменена пользователем")
                self.gui.update_status("Синхронизация отменена", "orange")
                self._restore_controls_after_cancel()
                return False

        self.gui.db.clear_sync_data()
        self.gui.db.clear_last_sync_snapshot()

        mismatch_report = self.file_sync.initial_sync(strategy=strategy)
        if not self._resolve_sync_mismatches(mismatch_report):
            return False
        current = self._current_binding()
        self.gui.db.save_last_initialized_binding(current["token"], current["folder_path"])
        return True

    def _run_restart_sync(self) -> bool:
        """На повторном запуске сначала применяет команды сервера, потом сверяет состояние."""
        self.gui.log("Связка уже инициализирована, применяю накопившиеся серверные команды...")
        self.command_poller.drain_pending_commands(log_empty=True)
        self.gui.log("Проверяю расхождения между ПК и сервером после применения серверных команд...")
        mismatch_report = self.file_sync.refresh_last_sync_snapshot_if_synced()
        return self._resolve_sync_mismatches(mismatch_report)

    def _restore_protected_namespace(self, namespace_path: str) -> None:

        self.gui.log(
            f"Локальное изменение служебной папки {namespace_path} отменено"
            "Восстанавливаю структуру с сервера..."
        )
        self.file_sync.restore_namespace_from_server(namespace_path)

    def _handle_root_folder_deleted(self, deleted_folder: Path) -> None:
        if self._root_folder_recovery_in_progress:
            return
        self._root_folder_recovery_in_progress = True

        def handle_on_ui_thread():
            try:
                self.gui.log(f"Корневая папка watcher удалена: {deleted_folder}")
                action = self.gui.ask_root_folder_deleted_action(deleted_folder)
                self.stop()

                if action == "choose_new":
                    new_folder = self.gui.ask_new_root_folder()
                    if not new_folder:
                        self.gui.log("Новая корневая папка не выбрана, watcher остановлен")
                        return
                    self.gui.selected_folder = new_folder
                    self.gui.folder_label.config(text=str(new_folder))
                    self.gui.log(f"Выбрана новая корневая папка: {new_folder}")
                    self.gui.start_watcher()
                    return

                if action == "restore":
                    deleted_folder.mkdir(parents=True, exist_ok=True)
                    self.gui.selected_folder = deleted_folder
                    self.gui.folder_label.config(text=str(deleted_folder))
                    self.gui.log(f"Восстанавливаю удалённую корневую папку: {deleted_folder}")
                    self.gui.start_watcher()
                    return

                self.gui.log("Восстановление после удаления корневой папки отменено, watcher остановлен")
            finally:
                self._root_folder_recovery_in_progress = False

        self.gui.root.after(0, handle_on_ui_thread)

    def _normalize_relative_path(self, file_path: Path) -> str:
        if not self.gui.selected_folder:
            raise ValueError("Папка не выбрана")
        return normalize_relative_path(file_path, self.gui.selected_folder)

    def _has_remote_apply_under_prefix(self, relative_prefix: str) -> bool:
        for file_state in self.gui.db.get_file_states_under_prefix(relative_prefix):
            if file_state.get("is_applying_remote"):
                return True
        for folder_state in self.gui.db.get_folder_states_under_prefix(relative_prefix):
            if folder_state.get("is_applying_remote"):
                return True
        return False

    def _delete_remote_namespaces_under_prefix(self, relative_prefix: str):
        deleted = 0
        folder_states = sorted(
            self.gui.db.get_folder_states_under_prefix(relative_prefix),
            key=lambda item: item["relative_path"].count("/"),
            reverse=True,
        )
        attempted_paths = set()

        for folder_state in folder_states:
            namespace_path = folder_state["relative_path"]
            namespace_id = folder_state.get("namespace_id")
            if namespace_id is None:
                continue
            attempted_paths.add(namespace_path)
            if self.api_client.delete_namespace(namespace_id):
                deleted += 1
                self.gui.log(f"Удалён пустой namespace на сервере: {namespace_path}")
            else:
                self.gui.log(f"Ошибка удаления namespace на сервере: {namespace_path}")

        snapshot = self.gui.db.get_last_sync_snapshot()
        namespace_by_path = build_namespace_path_index(snapshot)
        prefix = f"{relative_prefix}/"
        for namespace_path in sorted(namespace_by_path.keys(), key=lambda path: path.count("/"), reverse=True):
            if namespace_path in attempted_paths:
                continue
            if namespace_path != relative_prefix and not namespace_path.startswith(prefix):
                continue
            namespace = namespace_by_path[namespace_path]
            if namespace.id is None or namespace.kind in PROTECTED_NAMESPACE_KINDS:
                continue
            if self.api_client.delete_namespace(namespace.id):
                deleted += 1
                self.gui.log(f"Удалён пустой namespace на сервере: {namespace_path}")
            else:
                self.gui.log(f"Ошибка удаления namespace на сервере: {namespace_path}")
        return deleted

    def _is_known_folder_prefix(self, relative_prefix: str) -> bool:
        if self.gui.db.has_folder_path(relative_prefix):
            return True
        snapshot = self.gui.db.get_last_sync_snapshot()
        return relative_prefix in build_namespace_path_index(snapshot)

    def _resolve_vault_root_namespace_id(self) -> Optional[int]:
        snapshot = self.gui.db.get_last_sync_snapshot()
        for namespace in snapshot:
            if namespace.kind == VAULT_ROOT_KIND and namespace.id is not None:
                return namespace.id
        return None

    def _resolve_namespace_id_by_path(self, relative_path: str) -> Optional[int]:
        if not relative_path:
            return self._resolve_vault_root_namespace_id()

        folder_state = self.gui.db.get_folder_state(relative_path)
        if folder_state and folder_state.get("namespace_id") is not None:
            return int(folder_state["namespace_id"])

        snapshot = self.gui.db.get_last_sync_snapshot()
        namespace = build_namespace_path_index(snapshot).get(relative_path)
        if namespace and namespace.id is not None:
            return namespace.id
        return None

    def _resolve_target_parent_namespace_id(self, relative_path: str) -> Optional[int]:
        parent_path = Path(relative_path).parent.as_posix()
        if parent_path in {".", ""}:
            return self._resolve_vault_root_namespace_id()
        return self._resolve_namespace_id_by_path(parent_path)

    def _sync_local_folder_move_state(self, old_prefix: str, new_prefix: str) -> None:
        old_folder_state = self.gui.db.get_folder_state(old_prefix)
        new_folder_state = self.gui.db.get_folder_state(new_prefix)

        if new_folder_state:
            self.gui.db.delete_file_states_under_prefix(old_prefix)
            self.gui.db.delete_folder_states_under_prefix(old_prefix)
            if old_folder_state:
                new_parent_path = Path(new_prefix).parent.as_posix()
                self.gui.db.upsert_folder_state(
                    new_prefix,
                    namespace_id=old_folder_state.get("namespace_id"),
                    parent_relative_path=None if new_parent_path in {".", ""} else new_parent_path,
                    kind=old_folder_state.get("kind"),
                    is_applying_remote=0,
                    last_command_id=old_folder_state.get("last_command_id"),
                )
        else:
            self.gui.db.rename_folder_prefix(old_prefix, new_prefix)
            self.gui.db.rename_file_prefix(old_prefix, new_prefix)
        self.file_sync.replace_folder_states_from_local()

    def _sync_local_namespace_path_change(
        self,
        namespace_id: int,
        old_prefix: str,
        new_prefix: str,
    ) -> bool:
        old_parent = Path(old_prefix).parent.as_posix()
        new_parent = Path(new_prefix).parent.as_posix()
        old_name = Path(old_prefix).name
        new_name = Path(new_prefix).name

        if old_prefix == new_prefix:
            return True

        move_ok = True
        rename_ok = True

        if old_parent != new_parent:
            target_parent_id = self._resolve_target_parent_namespace_id(new_prefix)
            if target_parent_id is None:
                return False
            move_ok = self.api_client.move_namespace(namespace_id, target_parent_id)

        if move_ok and old_name != new_name:
            rename_ok = self.api_client.rename_namespace(namespace_id, new_name)

        return move_ok and rename_ok

    @staticmethod
    def _has_prefix_path(path: str, prefix: str) -> bool:
        return path == prefix or path.startswith(prefix + "/")

    @staticmethod
    def _relative_suffix(path: str, prefix: str) -> str:
        if path == prefix:
            return ""
        prefix_parts = Path(prefix).parts
        path_parts = Path(path).parts
        if tuple(path_parts[: len(prefix_parts)]) != tuple(prefix_parts):
            raise ValueError(f"{path} is not under prefix {prefix}")
        suffix_parts = path_parts[len(prefix_parts):]
        return Path(*suffix_parts).as_posix() if suffix_parts else ""

    def _detect_folder_path_change_for_file(
        self,
        old_relative_path: str,
        new_relative_path: str,
    ) -> Optional[tuple[str, str]]:
        if not self.gui.selected_folder:
            return None
        if Path(old_relative_path).name != Path(new_relative_path).name:
            return None

        old_parts = Path(old_relative_path).parts
        new_parts = Path(new_relative_path).parts
        if len(old_parts) < 2 or len(new_parts) < 2:
            return None

        for cut_index in range(1, len(old_parts) - 1):
            old_folder_parts = old_parts[:cut_index]
            suffix_parts = old_parts[cut_index:]
            if len(new_parts) <= len(suffix_parts):
                continue
            if tuple(new_parts[-len(suffix_parts):]) != tuple(suffix_parts):
                continue

            old_folder = Path(*old_folder_parts).as_posix()
            new_folder_parts = new_parts[:-len(suffix_parts)]
            new_folder = Path(*new_folder_parts).as_posix() if new_folder_parts else ""
            if not old_folder or not new_folder or old_folder == new_folder:
                continue

            folder_state = self.gui.db.get_folder_state(old_folder)
            if not folder_state or folder_state.get("namespace_id") is None:
                continue

            new_folder_path = self.gui.selected_folder / new_folder
            if not new_folder_path.exists() or not new_folder_path.is_dir():
                continue

            return old_folder, new_folder

        return None

    def _detect_moved_folder_destination(self, old_prefix: str) -> Optional[str]:
        if not self.file_sync:
            return None

        file_states = [
            state
            for state in self.gui.db.get_file_states_under_prefix(old_prefix)
            if state["relative_path"] != old_prefix
        ]
        if not file_states:
            return None

        local_files = self.file_sync.get_local_file_index()
        candidate_prefixes: Optional[set[str]] = None
        for file_state in file_states:
            old_relative_path = file_state["relative_path"]
            old_hash = file_state.get("content_hash")
            suffix = self._relative_suffix(old_relative_path, old_prefix)
            suffix_parts = Path(suffix).parts if suffix else ()
            matching_prefixes: set[str] = set()

            for local_path, local_meta in local_files.items():
                if self._has_prefix_path(local_path, old_prefix):
                    continue
                local_parts = Path(local_path).parts
                if suffix_parts and tuple(local_parts[-len(suffix_parts):]) != tuple(suffix_parts):
                    continue
                if old_hash and local_meta.content_hash != old_hash:
                    continue
                prefix_parts = local_parts[:-len(suffix_parts)] if suffix_parts else local_parts
                candidate_prefix = Path(*prefix_parts).as_posix() if prefix_parts else ""
                if candidate_prefix:
                    matching_prefixes.add(candidate_prefix)

            if not matching_prefixes:
                return None
            candidate_prefixes = (
                matching_prefixes
                if candidate_prefixes is None
                else candidate_prefixes & matching_prefixes
            )
            if not candidate_prefixes:
                return None

        if not candidate_prefixes:
            return None
        return sorted(candidate_prefixes, key=lambda item: (item.count("/"), item))[0]

    def _detect_moved_file_destination(self, old_relative_path: str) -> Optional[str]:
        if not self.file_sync:
            return None

        file_state = self.gui.db.get_file_state(old_relative_path)
        if not file_state:
            return None

        old_hash = file_state.get("content_hash")
        if not old_hash:
            return None

        local_files = self.file_sync.get_local_file_index()
        candidates = [
            local_path
            for local_path, local_meta in local_files.items()
            if local_path != old_relative_path and local_meta.content_hash == old_hash
        ]
        if len(candidates) != 1:
            return None
        return candidates[0]

    def _try_handle_moved_file_after_delete(self, old_relative_path: str) -> bool:
        if not self.file_uploader or not self.gui.selected_folder:
            return False

        new_relative_path = self._detect_moved_file_destination(old_relative_path)
        if not new_relative_path or new_relative_path == old_relative_path:
            return False

        target_path = self.gui.selected_folder / new_relative_path
        if not target_path.exists() or not target_path.is_file():
            return False

        file_state = self.gui.db.get_file_state(old_relative_path)
        server_user_file_id = file_state.get("server_user_file_id") if file_state else None
        result = self.file_uploader.upload_file(
            target_path,
            server_user_file_id=server_user_file_id,
        )
        if result.get("status") not in {"uploaded", "skipped"}:
            self.gui.log(
                f"Ошибка восстановления перемещения файла {old_relative_path} -> {new_relative_path}: "
                f"{result.get('message')}"
            )
            return False

        self.gui.db.delete_file_state(old_relative_path)
        self.gui.log(
            f"Локальное перемещение файла обнаружено по delete-событию: {old_relative_path} -> {new_relative_path}"
        )
        return True

    def _try_handle_moved_folder_after_delete(self, old_prefix: str) -> bool:
        if not self.file_sync or not self.file_uploader or not self.gui.selected_folder:
            return False

        new_prefix = self._detect_moved_folder_destination(old_prefix)
        if not new_prefix or new_prefix == old_prefix:
            return False

        namespace_id = self._resolve_namespace_id_by_path(old_prefix)
        if namespace_id is not None:
            self._sync_local_folder_move_state(old_prefix, new_prefix)
            if self._sync_local_namespace_path_change(namespace_id, old_prefix, new_prefix):
                self.gui.log(
                    f"Локальное перемещение папки обнаружено по delete-событию: {old_prefix} -> {new_prefix}"
                )
                return True
            self.gui.log(
                f"Ошибка синхронизации папки на сервере: {old_prefix} -> {new_prefix}"
            )
            return False

        file_states = sorted(
            self.gui.db.get_file_states_under_prefix(old_prefix),
            key=lambda item: item["relative_path"].count("/"),
        )
        for file_state in file_states:
            old_relative_path = file_state["relative_path"]
            if old_relative_path == old_prefix:
                continue
            suffix = self._relative_suffix(old_relative_path, old_prefix)
            new_relative_path = f"{new_prefix}/{suffix}" if suffix else new_prefix
            target_path = self.gui.selected_folder / new_relative_path
            if not target_path.exists() or not target_path.is_file():
                return False

            result = self.file_uploader.upload_file(
                target_path,
                server_user_file_id=file_state.get("server_user_file_id"),
            )
            if result.get("status") not in {"uploaded", "skipped"}:
                self.gui.log(
                    f"Ошибка восстановления перемещения папки {old_prefix} -> {new_prefix}: "
                    f"{result.get('message')}"
                )
                return False

        self.file_sync.replace_folder_states_from_local()
        self.gui.log(
            f"Локальное перемещение папки обнаружено по delete-событию: {old_prefix} -> {new_prefix}"
        )
        return True

    def _finalize_local_folder_delete(self, relative_prefix: str) -> None:
        if relative_prefix in PROTECTED_NAMESPACE_NAMES:
            self._restore_protected_namespace(namespace_path=relative_prefix)
            return

        if relative_prefix == TRASH_DIRNAME:
            self.gui.db.delete_file_states_under_prefix(relative_prefix)
            self.gui.db.delete_folder_states_under_prefix(relative_prefix)
            return

        if self._has_remote_apply_under_prefix(relative_prefix):
            return

        if self._try_handle_moved_folder_after_delete(relative_prefix):
            return

        file_states = sorted(
            self.gui.db.get_file_states_under_prefix(relative_prefix),
            key=lambda item: item["relative_path"].count("/"),
            reverse=True,
        )
        for file_state in file_states:
            server_user_file_id = file_state.get("server_user_file_id")
            relative_path = file_state["relative_path"]
            if server_user_file_id:
                if self.api_client.delete_server_file(server_user_file_id):
                    self.gui.log(f"Удалён на сервере по папке: {relative_path}")
                else:
                    self.gui.log(f"Ошибка удаления на сервере по папке: {relative_path}")
                    continue
            self.gui.db.delete_file_state(relative_path)

        self._delete_remote_namespaces_under_prefix(relative_prefix)
        self.gui.db.delete_folder_states_under_prefix(relative_prefix)
        self.file_sync.replace_folder_states_from_local()
        self.gui.log(f"Удалена локальная папка: {relative_prefix}")

    def _finalize_local_file_delete(self, relative_path: str) -> None:
        if self._try_handle_moved_file_after_delete(relative_path):
            return

        file_state = self.gui.db.get_file_state(relative_path)
        server_user_file_id = file_state.get("server_user_file_id") if file_state else None

        if server_user_file_id:
            if self.api_client.delete_server_file(server_user_file_id):
                if relative_path.startswith(TRASH_DIRNAME + "/"):
                    self.gui.log(f"Удалён на сервере из корзины: {relative_path}")
                else:
                    self.gui.log(f"Удалён на сервере: {relative_path}")
            else:
                if relative_path.startswith(TRASH_DIRNAME + "/"):
                    self.gui.log(f"Ошибка удаления на сервере из корзины: {relative_path}")
                else:
                    self.gui.log(f"Ошибка удаления на сервере: {relative_path}")
        else:
            self.gui.log(f"Удалён локально (не был загружен): {relative_path}")

        self.gui.db.delete_file_state(relative_path)

    def should_ignore_file_event(self, file_path):
        """Возращает True, если событие должно быть игнорировано."""
        if not self.gui.selected_folder:
            return False

        try:
            relative_path = self._normalize_relative_path(file_path)
        except Exception:
            return False

        if relative_path in PROTECTED_NAMESPACE_NAMES:
            return True

        file_state = self.gui.db.get_file_state(relative_path)
        folder_state = self.gui.db.get_folder_state(relative_path)
        if not file_state and not folder_state:
            return False

        if (file_state and file_state.get("is_applying_remote")) or (folder_state and folder_state.get("is_applying_remote")):
            return True

        if Path(file_path).exists() and file_state and file_state.get("last_seen_mtime") is not None:
            current_mtime = Path(file_path).stat().st_mtime
            return abs(current_mtime - file_state["last_seen_mtime"]) < 0.001

        return False

    def on_local_file_changed(self, file_path, event_type):
        """Обрабатывает локальные create/modify события и грузит файл на backend."""
        if is_temporary_file(Path(file_path)):
            return
        try:
            relative_path = self._normalize_relative_path(Path(file_path))
        except Exception:
            relative_path = None
        if relative_path:
            should_continue = self.local_event_reconciler.register_file_activity(relative_path)
            if not should_continue:
                self.gui.log(f"Откладываю sync файла до подтверждения перемещения папки: {relative_path}")
                return
        result = self.file_uploader.upload_file(file_path)
        status = result.get("status")
        relative_path = result.get("relative_path") or Path(file_path).name

        if status == "uploaded":
            self.gui.log(f"Отправлен {event_type}: {relative_path}")
        elif status == "failed":
            self.gui.log(f"Ошибка синхронизации {relative_path}: {result.get('message')}")

    def on_local_folder_created(self, folder_path):
        """Обрабатывает локальное создание папки как создание namespace на сервере."""
        folder_path = Path(folder_path)
        try:
            relative_prefix = self._normalize_relative_path(folder_path)
        except Exception:
            self.gui.log(f"Ошибка нормализации пути папки: {folder_path}")
            return

        if not relative_prefix or relative_prefix in PROTECTED_NAMESPACE_NAMES:
            return

        self.local_event_reconciler.register_folder_activity(relative_prefix)
        if self._has_remote_apply_under_prefix(relative_prefix):
            return

        existing_folder_state = self.gui.db.get_folder_state(relative_prefix)
        if existing_folder_state and existing_folder_state.get("namespace_id") is not None:
            return

        parent_namespace_id = self._resolve_target_parent_namespace_id(relative_prefix)
        if parent_namespace_id is None:
            self.gui.log(f"Не удалось определить parent namespace для папки: {relative_prefix}")
            return

        created_namespace = self.api_client.create_namespace(
            name=folder_path.name,
            parent_namespace_id=parent_namespace_id,
        )
        if created_namespace is None:
            self.gui.log(f"Ошибка создания папки на сервере: {relative_prefix}")
            return

        parent_relative_path = Path(relative_prefix).parent.as_posix()
        self.gui.db.upsert_folder_state(
            relative_prefix,
            namespace_id=created_namespace.id,
            parent_relative_path=(
                None if parent_relative_path in {".", ""} else parent_relative_path
            ),
            kind=created_namespace.kind,
            is_applying_remote=0,
        )
        self.gui.log(f"Создана локальная папка: {relative_prefix}")

    def on_local_file_moved(self, src_path, dest_path):
        """Обрабатывает локальное переименование/перемещение файла."""
        if is_temporary_file(Path(src_path)) or is_temporary_file(Path(dest_path)):
            return
        try:
            old_relative_path = self._normalize_relative_path(src_path)
        except Exception:
            self.gui.log(f"Ошибка нормализации исходного пути: {src_path}")
            return

        try:
            new_relative_path = self._normalize_relative_path(dest_path)
        except Exception:
            self.on_local_file_deleted(src_path)
            return

        self.local_event_reconciler.cancel_file_delete(old_relative_path)

        folder_path_change = self._detect_folder_path_change_for_file(
            old_relative_path,
            new_relative_path,
        )
        if folder_path_change is not None:
            old_folder, new_folder = folder_path_change
            self.gui.log(
                f"Перемещение файла относится к перемещению папки, жду folder event: "
                f"{old_folder} -> {new_folder}"
            )
            return

        file_state = self.gui.db.get_file_state(old_relative_path)
        server_user_file_id = file_state.get("server_user_file_id") if file_state else None
        old_parent = Path(old_relative_path).parent.as_posix()
        new_parent = Path(new_relative_path).parent.as_posix()

        if old_parent == new_parent and old_relative_path != new_relative_path and server_user_file_id:
            new_name = Path(new_relative_path).name
            if self.api_client.rename_server_file(server_user_file_id, new_name):
                self.gui.db.rename_file_state(old_relative_path, new_relative_path)
                self.gui.db.upsert_file_state(
                    new_relative_path,
                    last_seen_mtime=Path(dest_path).stat().st_mtime,
                    server_user_file_id=server_user_file_id,
                    is_applying_remote=0,
                )
                self.gui.log(f"Переименован локально: {old_relative_path} -> {new_relative_path}")
            else:
                self.gui.log(f"Ошибка переименования {old_relative_path} -> {new_relative_path}")
            return

        result = self.file_uploader.upload_file(
            dest_path,
            server_user_file_id=server_user_file_id,
        )
        if result.get("status") == "uploaded":
            self.gui.db.delete_file_state(old_relative_path)
            self.gui.log(f"Перемещён локально: {old_relative_path} -> {new_relative_path}")
        elif result.get("status") == "failed":
            self.gui.log(
                f"Ошибка перемещения {old_relative_path} -> {new_relative_path}: "
                f"{result.get('message')}"
            )

    def on_local_folder_moved(self, src_path, dest_path):
        """Обрабатывает локальное переименование/перемещение папки."""
        src_path = Path(src_path)
        dest_path = Path(dest_path)

        # Игнорируем перемещение папки, если оно было уже обработано
        ignored_move_pair = (str(src_path), str(dest_path))
        if ignored_move_pair in self._ignored_folder_moves:
            self._ignored_folder_moves.discard(ignored_move_pair)
            return

        try:
            old_prefix = self._normalize_relative_path(src_path)
            new_prefix = self._normalize_relative_path(dest_path)
        except Exception:
            try:
                old_prefix = self._normalize_relative_path(src_path)
            except Exception:
                self.on_local_folder_deleted(src_path)
                return
            if old_prefix in PROTECTED_NAMESPACE_NAMES:
                self._restore_protected_namespace(namespace_path=old_prefix)
                return
            self.on_local_folder_deleted(src_path)
            return

        if old_prefix in PROTECTED_NAMESPACE_NAMES:
            if dest_path.exists():
                src_path.parent.mkdir(parents=True, exist_ok=True)
                self._ignored_folder_moves.add((str(dest_path), str(src_path)))
                dest_path.rename(src_path)
            self._restore_protected_namespace(namespace_path=old_prefix)
            return

        if self._has_remote_apply_under_prefix(old_prefix) or self._has_remote_apply_under_prefix(new_prefix):
            return

        self.local_event_reconciler.cancel_folder_delete(old_prefix)
        self.local_event_reconciler.cancel_deletes_under_prefix(old_prefix)

        namespace_id = self._resolve_namespace_id_by_path(old_prefix)
        if namespace_id is not None and old_prefix != new_prefix:
            self._sync_local_folder_move_state(old_prefix, new_prefix)
            if self._sync_local_namespace_path_change(namespace_id, old_prefix, new_prefix):
                old_parent = Path(old_prefix).parent.as_posix()
                new_parent = Path(new_prefix).parent.as_posix()
                if old_parent == new_parent:
                    self.gui.log(f"Переименована локальная папка: {old_prefix} -> {new_prefix}")
                else:
                    self.gui.log(f"Перемещена локальная папка: {old_prefix} -> {new_prefix}")
            else:
                self.gui.log(
                    f"Ошибка синхронизации папки на сервере: {old_prefix} -> {new_prefix}"
                )
            return

        for file_path in dest_path.rglob("*"):
            if not file_path.is_file() or is_temporary_file(file_path):
                continue

            suffix = file_path.relative_to(dest_path).as_posix()
            old_relative_path = f"{old_prefix}/{suffix}" if suffix else old_prefix
            new_relative_path = f"{new_prefix}/{suffix}" if suffix else new_prefix
            file_state = self.gui.db.get_file_state(old_relative_path)
            server_user_file_id = file_state.get("server_user_file_id") if file_state else None
            result = self.file_uploader.upload_file(
                file_path,
                server_user_file_id=server_user_file_id,
            )
            if result.get("status") == "uploaded":
                self.gui.db.delete_file_state(old_relative_path)
            elif result.get("status") == "failed":
                self.gui.log(
                    f"Ошибка перемещения файла {old_relative_path} -> {new_relative_path}: "
                    f"{result.get('message')}"
                )

        self.gui.db.rename_folder_prefix(old_prefix, new_prefix)
        self.file_sync.replace_folder_states_from_local()
        self.gui.log(f"Перемещена локальная папка: {old_prefix} -> {new_prefix}")

    def on_local_folder_deleted(self, folder_path):
        """Откладывает удаление папки, чтобы отличать delete от move."""
        if not self.gui.selected_folder:
            return

        if folder_path.resolve() == self.gui.selected_folder.resolve():
            self._handle_root_folder_deleted(Path(folder_path))
            return

        try:
            relative_prefix = self._normalize_relative_path(folder_path)
        except Exception:
            self.gui.log(f"Ошибка нормализации пути папки: {folder_path}")
            return

        if relative_prefix in PROTECTED_NAMESPACE_NAMES:
            self._restore_protected_namespace(namespace_path=relative_prefix)
            return

        if relative_prefix == TRASH_DIRNAME:
            self.gui.db.delete_file_states_under_prefix(relative_prefix)
            self.gui.db.delete_folder_states_under_prefix(relative_prefix)
            return

        if self._has_remote_apply_under_prefix(relative_prefix):
            return

        self.local_event_reconciler.register_folder_delete(relative_prefix)

    def on_local_file_deleted(self, file_path):
        """Откладывает удаление файла, чтобы отличать delete от move."""
        if not self.gui.selected_folder:
            self.gui.log("Ошибка: папка не выбрана")
            self.gui.update_status("Ошибка конфигурации", "red")
            return
        if is_temporary_file(Path(file_path)):
            return
        try:
            relative_path = self._normalize_relative_path(file_path)
        except Exception:
            self.gui.log(f"Ошибка нормализации пути: {file_path}")
            self.gui.update_status("Ошибка конфигурации", "red")
            return

        if self._is_known_folder_prefix(relative_path):
            self.on_local_folder_deleted(file_path)
            return

        self.local_event_reconciler.register_file_delete(relative_path)

    def start(self):
        """Запуск приложения."""
        if self.running:
            return

        if not self.gui.selected_folder:
            self.gui.log("Ошибка: папка не выбрана")
            self.gui.update_status("Ошибка конфигурации", "red")
            return

        if not self.gui.token:
            self.gui.log("Ошибка: не указан токен аутентификации")
            self.gui.update_status("Ошибка конфигурации", "red")
            return

        self.running = True
        self._build_services()

        # Если нет папок Trash и Inbox, то создаём их
        if not self.gui.selected_folder.joinpath(TRASH_DIRNAME).exists():
            self.gui.selected_folder.joinpath(TRASH_DIRNAME).mkdir(parents=True, exist_ok=True)
        if not self.gui.selected_folder.joinpath(INBOX_DIRNAME).exists():
            self.gui.selected_folder.joinpath(INBOX_DIRNAME).mkdir(parents=True, exist_ok=True)

        last_initialized_binding = self.gui.db.get_last_initialized_binding()
        current_binding = self._current_binding()
        try:
            if last_initialized_binding != current_binding:
                if self._run_initial_sync() is False:
                    return
            else:
                if not self._run_restart_sync():
                    return
            
            self.file_sync.replace_folder_states_from_local()
            self.gui.db.save_last_initialized_binding(current_binding["token"], current_binding["folder_path"])

            self.file_watcher = FileWatcher(
                self.gui.selected_folder,
                on_file_changed_callback=self.on_local_file_changed,
                on_folder_created_callback=self.on_local_folder_created,
                on_file_deleted_callback=self.on_local_file_deleted,
                on_file_moved_callback=self.on_local_file_moved,
                on_folder_moved_callback=self.on_local_folder_moved,
                on_folder_deleted_callback=self.on_local_folder_deleted,
                should_ignore_callback=self.should_ignore_file_event,
            )
            self.file_watcher.start()
            self.gui.log("File Watcher запущен")

            self.command_poller_thread = threading.Thread(
                target=self.command_poller.run_forever,
                daemon=True,
            )
            self.command_poller_thread.start()

            self.gui.update_status("Подключено", "green")
            self.gui.log("HTTP polling синхронизация запущена")
        except Exception as error:
            logger.error("Ошибка запуска приложения: %s", error, exc_info=True)
            self.gui.log(f"Ошибка запуска: {error}")
            self.gui.update_status("Ошибка запуска", "red")
            self.running = False

    def stop(self):
        """Остановка приложения."""
        if not self.running:
            return

        self.running = False

        self.local_event_reconciler.clear()

        if self.command_poller:
            self.command_poller.stop()
        if self.command_poller_thread and self.command_poller_thread.is_alive():
            self.command_poller_thread.join(timeout=2)
        self.command_poller_thread = None
        self.command_poller = None

        if self.file_watcher:
            self.file_watcher.stop()
            self.file_watcher = None

        self.file_uploader = None
        self.file_sync = None
        self.api_client = None
        self.gui.update_status("Остановлено", "red")

    def run(self):
        """Запуск GUI."""
        self.gui.root.protocol("WM_DELETE_WINDOW", self.gui.on_closing)
        self.gui.run()
