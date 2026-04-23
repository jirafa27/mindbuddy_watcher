"""Локальный reconciler неоднозначных delete/create/move событий файловой системы."""

from __future__ import annotations

import threading
from typing import Callable, Dict


class LocalEventReconciler:
    """Откладывает destructive delete, чтобы отличать его от move."""

    def __init__(
        self,
        grace_period_seconds: float,
        has_prefix_path: Callable[[str, str], bool],
        on_finalize_file_delete: Callable[[str], None],
        on_finalize_folder_delete: Callable[[str], None],
    ) -> None:
        self.grace_period_seconds = grace_period_seconds
        self._has_prefix_path = has_prefix_path
        self._on_finalize_file_delete = on_finalize_file_delete
        self._on_finalize_folder_delete = on_finalize_folder_delete
        self._lock = threading.Lock()
        self._pending_file_deletes: Dict[str, threading.Timer] = {}
        self._pending_folder_deletes: Dict[str, threading.Timer] = {}

    def register_file_delete(self, relative_path: str) -> None:
        self._schedule_delete(
            self._pending_file_deletes,
            relative_path,
            self._process_pending_file_delete,
        )

    def register_folder_delete(self, relative_prefix: str) -> None:
        self._schedule_delete(
            self._pending_folder_deletes,
            relative_prefix,
            self._process_pending_folder_delete,
        )

    def register_file_activity(self, relative_path: str) -> bool:
        self.cancel_file_delete(relative_path)
        return not self.has_pending_folder_delete_for_path(relative_path)

    def register_folder_activity(self, relative_prefix: str) -> None:
        self.cancel_folder_delete(relative_prefix)

    def cancel_file_delete(self, relative_path: str) -> None:
        self._cancel_delete(self._pending_file_deletes, relative_path)

    def cancel_folder_delete(self, relative_prefix: str) -> None:
        self._cancel_delete(self._pending_folder_deletes, relative_prefix)

    def cancel_deletes_under_prefix(self, relative_prefix: str) -> None:
        with self._lock:
            file_paths = [
                path
                for path in self._pending_file_deletes
                if self._has_prefix_path(path, relative_prefix)
            ]
            folder_paths = [
                path
                for path in self._pending_folder_deletes
                if self._has_prefix_path(path, relative_prefix)
            ]
            file_timers = [
                self._pending_file_deletes.pop(path)
                for path in file_paths
            ]
            folder_timers = [
                self._pending_folder_deletes.pop(path)
                for path in folder_paths
            ]
        for timer in file_timers + folder_timers:
            timer.cancel()

    def has_pending_folder_delete_for_path(self, relative_path: str) -> bool:
        with self._lock:
            return any(
                self._has_prefix_path(relative_path, folder_prefix)
                for folder_prefix in self._pending_folder_deletes
            )

    def clear(self) -> None:
        with self._lock:
            timers = list(self._pending_file_deletes.values()) + list(
                self._pending_folder_deletes.values()
            )
            self._pending_file_deletes.clear()
            self._pending_folder_deletes.clear()
        for timer in timers:
            timer.cancel()

    def _schedule_delete(
        self,
        storage: Dict[str, threading.Timer],
        key: str,
        callback: Callable[[str], None],
    ) -> None:
        with self._lock:
            existing_timer = storage.pop(key, None)
            if existing_timer:
                existing_timer.cancel()
            timer = threading.Timer(
                self.grace_period_seconds,
                callback,
                args=(key,),
            )
            timer.daemon = True
            storage[key] = timer
            timer.start()

    def _cancel_delete(self, storage: Dict[str, threading.Timer], key: str) -> None:
        with self._lock:
            timer = storage.pop(key, None)
        if timer:
            timer.cancel()

    def _process_pending_file_delete(self, relative_path: str) -> None:
        with self._lock:
            self._pending_file_deletes.pop(relative_path, None)
        self._on_finalize_file_delete(relative_path)

    def _process_pending_folder_delete(self, relative_prefix: str) -> None:
        with self._lock:
            self._pending_folder_deletes.pop(relative_prefix, None)
        self._on_finalize_folder_delete(relative_prefix)
