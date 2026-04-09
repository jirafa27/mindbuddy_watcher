"""Главный класс приложения."""

import logging
import threading
from pathlib import Path

from app import config
from app.core.api_client import SyncAPIClient
from app.core.command_poller import CommandPoller
from app.core.file_utils import normalize_relative_path
from app.core.sync import FileSync, SyncStrategy
from app.core.uploader import FileUploader
from app.core.watcher import FileWatcher
from app.gui import WatcherGUI

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


class MindBuddyWatcher:
    """Оркестратор desktop watcher c polling-синхронизацией."""

    def __init__(self):
        self.gui = WatcherGUI(self)
        self.api_client = None
        self.file_watcher = None
        self.file_uploader = None
        self.file_sync = None
        self.command_poller = None
        self.command_poller_thread = None
        self.running = False

    def _build_services(self):
        vault_name = self.gui.selected_folder.name if self.gui.selected_folder else ""
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
            reconcile_every=config.RECONCILE_EVERY_N_POLLS,
            file_sync=self.file_sync,
            log_callback=self.gui.log,
        )

    def _resolve_sync_strategy(self):
        """Определяет стратегию синхронизации.

        Если расхождений нет — возвращает PC_PRIORITY (по умолчанию).
        Иначе показывает диалог и ждёт выбора пользователя.
        Возвращает SyncStrategy или None (отмена).
        """
        self.gui.log("Проверка расхождений с сервером...")
        try:
            discrepancies = self.file_sync.get_differences()
        except Exception as error:
            logger.error("Ошибка при get_differences: %s", error, exc_info=True)
            self.gui.log(f"Не удалось проверить расхождения: {error}")
            return SyncStrategy.PC_PRIORITY

        if not discrepancies["has_discrepancies"]:
            self.gui.log("Расхождений не обнаружено, синхронизация по умолчанию (приоритет ПК)")
            return SyncStrategy.PC_PRIORITY

        self.gui.log(
            f"Обнаружены расхождения: только на ПК={len(discrepancies['only_local'])}, "
            f"только на сервере={len(discrepancies['only_server'])}, "
            f"конфликты={len(discrepancies['content_conflict'])}, "
            f"перемещено={len(discrepancies['path_moved'])}"
        )

        result_holder = {"strategy": None}
        done_event = threading.Event()

        def show_dialog():
            result_holder["strategy"] = self.gui.ask_sync_strategy(discrepancies)
            done_event.set()

        self.gui.root.after(0, show_dialog)
        done_event.wait()
        return result_holder["strategy"]

    @staticmethod
    def _is_trash_relative_path(relative_path: str) -> bool:
        return relative_path == "Trash" or relative_path.startswith("Trash/")

    def _normalize_relative_path(self, file_path: Path) -> str:
        if not self.gui.selected_folder:
            raise ValueError("Папка не выбрана")
        return normalize_relative_path(Path(file_path), self.gui.selected_folder)

    def _has_remote_apply_under_prefix(self, relative_prefix: str) -> bool:
        if not self.gui.db:
            return False
        prefix = f"{relative_prefix}/"
        for sync_state in self.gui.db.get_all_sync_states():
            path = sync_state["relative_path"]
            if (path == relative_prefix or path.startswith(prefix)) and sync_state.get("is_applying_remote"):
                return True
        return False

    def should_ignore_file_event(self, file_path):
        """Игнорирует события, спровоцированные применением серверной команды."""
        if not self.gui.selected_folder:
            return False

        try:
            relative_path = self._normalize_relative_path(Path(file_path))
        except Exception:
            return False

        if self._is_trash_relative_path(relative_path):
            return True

        sync_state = self.gui.db.get_sync_state(relative_path)
        if not sync_state:
            return False

        if sync_state.get("is_applying_remote"):
            return True

        if Path(file_path).exists() and sync_state.get("last_seen_mtime") is not None:
            current_mtime = Path(file_path).stat().st_mtime
            return abs(current_mtime - sync_state["last_seen_mtime"]) < 0.001

        return False

    def on_local_file_changed(self, file_path, event_type):
        """Обрабатывает локальные create/modify события и грузит файл на backend."""
        result = self.file_uploader.upload_file(file_path)
        status = result.get("status")
        relative_path = result.get("relative_path") or Path(file_path).name

        if status == "uploaded":
            self.gui.log(f"Отправлен {event_type}: {relative_path}")
        elif status == "failed":
            self.gui.log(f"Ошибка синхронизации {relative_path}: {result.get('message')}")

    def on_local_file_moved(self, src_path, dest_path):
        """Обрабатывает локальное переименование/перемещение файла."""
        try:
            old_relative_path = self._normalize_relative_path(Path(src_path))
        except Exception:
            self.gui.log(f"Ошибка нормализации исходного пути: {src_path}")
            return

        try:
            new_relative_path = self._normalize_relative_path(Path(dest_path))
        except Exception:
            self.on_local_file_deleted(src_path)
            return

        sync_state = self.gui.db.get_sync_state(old_relative_path)
        server_user_file_id = sync_state.get("server_user_file_id") if sync_state else None
        result = self.file_uploader.upload_file(
            dest_path,
            server_user_file_id=server_user_file_id,
        )
        if result.get("status") == "uploaded":
            self.gui.db.delete_sync_state(old_relative_path)
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
        if not dest_path.exists():
            return

        try:
            old_prefix = self._normalize_relative_path(src_path)
            new_prefix = self._normalize_relative_path(dest_path)
        except Exception as error:
            self.gui.log(f"Ошибка обработки перемещения папки: {error}")
            return

        if self._has_remote_apply_under_prefix(old_prefix) or self._has_remote_apply_under_prefix(new_prefix):
            return

        for file_path in dest_path.rglob("*"):
            if not file_path.is_file():
                continue

            suffix = file_path.relative_to(dest_path).as_posix()
            old_relative_path = f"{old_prefix}/{suffix}" if suffix else old_prefix
            new_relative_path = f"{new_prefix}/{suffix}" if suffix else new_prefix
            sync_state = self.gui.db.get_sync_state(old_relative_path)
            server_user_file_id = sync_state.get("server_user_file_id") if sync_state else None
            result = self.file_uploader.upload_file(
                file_path,
                server_user_file_id=server_user_file_id,
            )
            if result.get("status") == "uploaded":
                self.gui.db.delete_sync_state(old_relative_path)
            elif result.get("status") == "failed":
                self.gui.log(
                    f"Ошибка перемещения файла {old_relative_path} -> {new_relative_path}: "
                    f"{result.get('message')}"
                )
        self.gui.log(f"Перемещена локальная папка: {old_prefix} -> {new_prefix}")

    def on_local_file_deleted(self, file_path):
        """Обрабатывает локальное удаление файла и удаляет его на сервере."""
        if not self.gui.selected_folder:
            self.gui.log("Ошибка: папка не выбрана")
            self.gui.update_status("Ошибка конфигурации", "red")
            return
        try:
            relative_path = self._normalize_relative_path(Path(file_path))
        except Exception:
            self.gui.log(f"Ошибка нормализации пути: {file_path}")
            self.gui.update_status("Ошибка конфигурации", "red")
            return

        if self._is_trash_relative_path(relative_path):
            self.gui.db.delete_sync_state(relative_path)
            return

        sync_state = self.gui.db.get_sync_state(relative_path)
        server_user_file_id = sync_state.get("server_user_file_id") if sync_state else None

        if server_user_file_id:
            if self.api_client.delete_desktop_file(server_user_file_id):
                self.gui.log(f"Удалён на сервере: {relative_path}")
            else:
                self.gui.log(f"Ошибка удаления на сервере: {relative_path}")
        else:
            self.gui.log(f"Удалён локально (не был загружен): {relative_path}")

        self.gui.db.delete_sync_state(relative_path)

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

        try:
            strategy = self._resolve_sync_strategy()
            if strategy is None:
                self.gui.log("Синхронизация отменена пользователем")
                self.gui.update_status("Синхронизация отменена", "orange")
                self.running = False
                self.gui.root.after(0, lambda: (
                    self.gui.start_button.config(state="normal"),
                    self.gui.stop_button.config(state="disabled"),
                ))
                return

            self.file_sync.sync(callback=self.gui.log, strategy=strategy)

            self.file_watcher = FileWatcher(
                self.gui.selected_folder,
                self.on_local_file_changed,
                on_file_deleted_callback=self.on_local_file_deleted,
                on_file_moved_callback=self.on_local_file_moved,
                on_folder_moved_callback=self.on_local_folder_moved,
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
