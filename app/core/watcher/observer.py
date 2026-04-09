"""File Observer для отслеживания изменений в папке."""

import logging
from watchdog.observers import Observer
from pathlib import Path
from .handler import FileWatcherHandler

logger = logging.getLogger(__name__)


class FileWatcher:
    """Наблюдатель за файлами в папке."""
    
    def __init__(
        self,
        folder_path,
        on_file_changed_callback,
        on_file_deleted_callback=None,
        on_file_moved_callback=None,
        on_folder_moved_callback=None,
        should_ignore_callback=None,
        debounce_seconds=1.0,
    ):
        self.folder_path = Path(folder_path)
        self.on_file_changed = on_file_changed_callback
        self.on_file_deleted = on_file_deleted_callback
        self.on_file_moved = on_file_moved_callback
        self.on_folder_moved = on_folder_moved_callback
        self.should_ignore_callback = should_ignore_callback
        self.debounce_seconds = debounce_seconds
        self.observer = None
        self.event_handler = None
        self.running = False
        
    def start(self):
        """Запуск наблюдения"""
        if not self.folder_path.exists():
            self.folder_path.mkdir(parents=True, exist_ok=True)
            logger.info(f"Создана папка: {self.folder_path}")
            
        self.event_handler = FileWatcherHandler(
            self.on_file_changed,
            on_file_deleted_callback=self.on_file_deleted,
            on_file_moved_callback=self.on_file_moved,
            on_folder_moved_callback=self.on_folder_moved,
            should_ignore_callback=self.should_ignore_callback,
            debounce_seconds=self.debounce_seconds,
        )
        self.observer = Observer()
        self.observer.schedule(self.event_handler, str(self.folder_path), recursive=True)
        self.observer.start()
        self.running = True
        logger.info(f"File Watcher запущен для папки: {self.folder_path}")
        
    def stop(self):
        """Остановка наблюдения"""
        if self.event_handler:
            self.event_handler.shutdown()
            self.event_handler = None
        if self.observer:
            self.observer.stop()
            self.observer.join()
            self.observer = None
        self.running = False
        logger.info("File Watcher остановлен")
