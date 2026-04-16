"""Главный класс приложения."""

import logging
import threading
from pathlib import Path
from typing import Optional

from app import config
from app.core.api_client import SyncAPIClient
from app.core.command_poller import CommandPoller
from app.core.file_utils import is_temporary_file, normalize_relative_path
from app.core.logger import add_log_handler, configure_logging
from app.core.namespace_constants import (
    INBOX_DIRNAME,
    PROTECTED_NAMESPACE_KINDS,
    PROTECTED_NAMESPACE_NAMES,
    TRASH_DIRNAME,
    VAULT_ROOT_NAME,
    VAULT_ROOT_KIND,
)
from app.core.sync import FileSync, SyncStrategy
from app.core.sync.namespace_utils import build_namespace_path_index, flatten_namespace_list
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

    def _build_services(self):
        vault_name = VAULT_ROOT_NAME if self.gui.selected_folder else ""
        self.api_client = SyncAPIClient(
            base_url=config.API_BASE_URL,
            token=self.gui.token,
            device_id=config.DEFAULT_DEVICE_ID,
            vault_name=vault_name,
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
            log_callback=self.gui.log,
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

        self.file_sync.initial_sync(strategy=strategy)
        current = self._current_binding()
        self.gui.db.save_last_initialized_binding(current["token"], current["folder_path"])
        return True

    def _restore_protected_namespace(self, namespace_path: str) -> None:

        self.gui.log(
            f"Локальное изменение служебной папки {namespace_path} отменено"
            "Восстанавливаю структуру с сервера..."
        )
        self.file_sync.restore_namespace_from_server(namespace_path)

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

    def should_ignore_file_event(self, file_path):
        """Возращает True, если событие должно быть игнорировано."""
        if not self.gui.selected_folder:
            return False

        try:
            relative_path = self._normalize_relative_path(file_path)
        except Exception:
            return False

        if relative_path in PROTECTED_NAMESPACE_NAMES or any(
            relative_path.startswith(f"{name}/")
            for name in PROTECTED_NAMESPACE_NAMES
        ):
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
        result = self.file_uploader.upload_file(file_path)
        status = result.get("status")
        relative_path = result.get("relative_path") or Path(file_path).name

        if status == "uploaded":
            self.gui.log(f"Отправлен {event_type}: {relative_path}")
        elif status == "failed":
            self.gui.log(f"Ошибка синхронизации {relative_path}: {result.get('message')}")

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

        file_state = self.gui.db.get_file_state(old_relative_path)
        server_user_file_id = file_state.get("server_user_file_id") if file_state else None
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

        self.file_sync.replace_folder_states_from_local()
        self.gui.log(f"Перемещена локальная папка: {old_prefix} -> {new_prefix}")

    def on_local_folder_deleted(self, folder_path):
        """Обрабатывает локальное удаление папки как hard delete всех её файлов на сервере."""
        if not self.gui.selected_folder:
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

    def on_local_file_deleted(self, file_path):
        """Обрабатывает локальное удаление файла и удаляет его на сервере."""
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

        if relative_path == TRASH_DIRNAME or relative_path.startswith(TRASH_DIRNAME + "/"):
            self.gui.db.delete_file_state(relative_path)
            return

        file_state = self.gui.db.get_file_state(relative_path)
        server_user_file_id = file_state.get("server_user_file_id") if file_state else None

        if server_user_file_id:
            if self.api_client.delete_server_file(server_user_file_id):
                self.gui.log(f"Удалён на сервере: {relative_path}")
            else:
                self.gui.log(f"Ошибка удаления на сервере: {relative_path}")
        else:
            self.gui.log(f"Удалён локально (не был загружен): {relative_path}")

        self.gui.db.delete_file_state(relative_path)

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
                self.gui.log("Связка уже инициализирована, запускаю синхронизацию после перезапуска...")
                self.file_sync.sync_after_restart()
            
            self.file_sync.replace_folder_states_from_local()
            self.gui.db.save_last_initialized_binding(current_binding["token"], current_binding["folder_path"])

            self.file_watcher = FileWatcher(
                self.gui.selected_folder,
                on_file_changed_callback=self.on_local_file_changed,
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
