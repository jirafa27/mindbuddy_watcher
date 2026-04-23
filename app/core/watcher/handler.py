"""Обработчик событий файловой системы."""

import logging
import threading
from pathlib import Path

from watchdog.events import FileSystemEventHandler, FileCreatedEvent, DirCreatedEvent, DirDeletedEvent, FileDeletedEvent, DirMovedEvent, FileModifiedEvent, DirModifiedEvent
from app.core.file_utils import is_temporary_file

logger = logging.getLogger(__name__)


class FileWatcherHandler(FileSystemEventHandler):
    """Отслеживает create/modify/delete события по пути файла."""

    def __init__(
        self,
        on_file_changed_callback=None,
        on_folder_created_callback=None,
        on_file_deleted_callback=None,
        on_file_moved_callback=None,
        on_folder_moved_callback=None,
        on_folder_deleted_callback=None,
        should_ignore_callback=None,
        debounce_seconds=1.0,
    ):
        super().__init__()
        self.on_file_changed = on_file_changed_callback
        self.on_folder_created = on_folder_created_callback
        self.on_file_deleted = on_file_deleted_callback
        self.on_file_moved = on_file_moved_callback
        self.on_folder_moved = on_folder_moved_callback
        self.on_folder_deleted = on_folder_deleted_callback
        self.should_ignore_callback = should_ignore_callback
        self.debounce_seconds = debounce_seconds
        self._timers = {}
        self._lock = threading.Lock()

    def _schedule_event(self, file_path: Path, event_type: str):
        """
        Планирует событие на выполнение.
        Нужно для того, чтобы не обрабатывать события слишком часто.
        редакторы часто сохраняют файл несколькими системными событиями
        без debounce watcher отправит несколько одинаковых запросов
        можно поймать файл в момент, когда он ещё не до конца записан
        """

        if is_temporary_file(file_path):
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

        if is_temporary_file(file_path):
            return

        if self.should_ignore_callback and self.should_ignore_callback(file_path):
            return

        if event_type == "create_folder" and file_path.exists() and file_path.is_dir() and self.on_folder_created:
            logger.info("Обнаружено событие %s для папки: %s", event_type, file_path)
            self.on_folder_created(file_path)
            return

        if file_path.exists() and file_path.is_file() and self.on_file_changed:
            logger.info("Обнаружено событие %s для файла: %s", event_type, file_path)
            self.on_file_changed(file_path, event_type)

    def on_created(self, event):
        """Обработка создания файла или папки."""
        if isinstance(event, FileCreatedEvent):
            self._schedule_event(Path(event.src_path), "create_file")
        elif isinstance(event, DirCreatedEvent):
            self._schedule_event(Path(event.src_path), "create_folder")


    def on_modified(self, event):
        """Обработка изменения файла."""
        if isinstance(event, FileModifiedEvent):
            self._schedule_event(Path(event.src_path), "modify_file")
        elif isinstance(event, DirModifiedEvent):
            self._schedule_event(Path(event.src_path), "modify_folder")

    def on_deleted(self, event):
        """Обработка удаления файла."""
        if isinstance(event, DirDeletedEvent):
            folder_path = Path(event.src_path)
            logger.info("Обнаружено удаление папки: %s", folder_path)
            self.on_folder_deleted(folder_path)
            return
        elif isinstance(event, FileDeletedEvent):
            file_path = Path(event.src_path)
            if is_temporary_file(file_path):
                return
            logger.info("Обнаружено удаление файла: %s", file_path)
            self.on_file_deleted(file_path)

    def on_moved(self, event):
        """Обработка перемещения или переименования файла/папки."""
        src_path = Path(event.src_path)
        dest_path = Path(event.dest_path)

        if isinstance(event, DirMovedEvent):
            if is_temporary_file(src_path) or is_temporary_file(dest_path):
                return
            if self.on_folder_moved:
                logger.info("Обнаружено перемещение папки: %s -> %s", src_path, dest_path)
                self.on_folder_moved(src_path, dest_path)
            return

        if is_temporary_file(src_path) or is_temporary_file(dest_path):
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
