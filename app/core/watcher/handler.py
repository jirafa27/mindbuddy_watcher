"""Обработчик событий файловой системы."""

import logging
import threading
from pathlib import Path

from watchdog.events import FileSystemEventHandler

logger = logging.getLogger(__name__)


class FileWatcherHandler(FileSystemEventHandler):
    """Отслеживает create/modify/delete события с debounce по пути файла."""

    def __init__(
        self,
        on_file_changed_callback,
        on_file_deleted_callback=None,
        on_file_moved_callback=None,
        on_folder_moved_callback=None,
        should_ignore_callback=None,
        debounce_seconds=1.0,
    ):
        super().__init__()
        self.on_file_changed = on_file_changed_callback
        self.on_file_deleted = on_file_deleted_callback
        self.on_file_moved = on_file_moved_callback
        self.on_folder_moved = on_folder_moved_callback
        self.should_ignore_callback = should_ignore_callback
        self.debounce_seconds = debounce_seconds
        self._timers = {}
        self._lock = threading.Lock()

    @staticmethod
    def is_temporary_file(file_path: Path) -> bool:
        """Отфильтровывает временные и lock-файлы редакторов."""
        name = file_path.name
        return (
            name.startswith("~$")
            or name.endswith(".tmp")
            or name.endswith(".temp")
            or name.startswith(".~")
        )

    def _schedule_event(self, file_path: Path, event_type: str):
        if self.is_temporary_file(file_path):
            return

        if self.should_ignore_callback and self.should_ignore_callback(file_path):
            return

        file_key = str(file_path)
        with self._lock:
            existing_timer = self._timers.get(file_key)
            if existing_timer:
                existing_timer.cancel()

            timer = threading.Timer(
                self.debounce_seconds,
                self._dispatch_event,
                args=(file_path, event_type),
            )
            timer.daemon = True
            self._timers[file_key] = timer
            timer.start()

    def _dispatch_event(self, file_path: Path, event_type: str):
        file_key = str(file_path)
        with self._lock:
            self._timers.pop(file_key, None)

        if self.is_temporary_file(file_path):
            return

        if self.should_ignore_callback and self.should_ignore_callback(file_path):
            return

        if file_path.exists() and file_path.is_file() and self.on_file_changed:
            logger.info("Обнаружено событие %s для файла: %s", event_type, file_path)
            self.on_file_changed(file_path, event_type)

    def on_created(self, event):
        """Обработка создания файла."""
        if not event.is_directory:
            self._schedule_event(Path(event.src_path), "created")

    def on_modified(self, event):
        """Обработка изменения файла."""
        if not event.is_directory:
            self._schedule_event(Path(event.src_path), "modified")

    def on_deleted(self, event):
        """Обработка удаления файла."""
        if event.is_directory:
            return
        file_path = Path(event.src_path)
        if self.is_temporary_file(file_path):
            return
        if self.on_file_deleted:
            logger.info("Обнаружено удаление файла: %s", file_path)
            self.on_file_deleted(file_path)

    def on_moved(self, event):
        """Обработка перемещения или переименования файла/папки."""
        src_path = Path(event.src_path)
        dest_path = Path(event.dest_path)

        if event.is_directory:
            if self.on_folder_moved:
                logger.info("Обнаружено перемещение папки: %s -> %s", src_path, dest_path)
                self.on_folder_moved(src_path, dest_path)
            return

        if self.is_temporary_file(src_path) or self.is_temporary_file(dest_path):
            return
        if self.should_ignore_callback and (
            self.should_ignore_callback(src_path) or self.should_ignore_callback(dest_path)
        ):
            return
        if self.on_file_moved:
            logger.info("Обнаружено перемещение файла: %s -> %s", src_path, dest_path)
            self.on_file_moved(src_path, dest_path)

    def shutdown(self):
        """Останавливает все таймеры debounce."""
        with self._lock:
            timers = list(self._timers.values())
            self._timers.clear()

        for timer in timers:
            timer.cancel()
