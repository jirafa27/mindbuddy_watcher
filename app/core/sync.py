"""Стартовая синхронизация локальной папки с backend."""

import enum
import logging
import time
from pathlib import Path
from typing import Callable, Dict, List, Optional

from app.core.file_utils import build_local_file_meta, compute_file_hash

logger = logging.getLogger(__name__)


class SyncStrategy(enum.Enum):
    PC_PRIORITY = "pc"
    SERVER_PRIORITY = "server"


class FileSync:
    """Синхронизация локальной папки с backend."""

    def __init__(self, api_client, local_folder: Path, uploader, db=None):
        self.api_client = api_client
        self.local_folder = Path(local_folder)
        self.uploader = uploader
        self.db = db

    def _callback(self, callback: Optional[Callable[[str], None]], message: str):
        logger.info(message)
        if callback:
            callback(message)

    @staticmethod
    def _is_temporary_file(file_path: Path) -> bool:
        name = file_path.name
        return (
            name.startswith("~$")
            or name.endswith(".tmp")
            or name.endswith(".temp")
            or name.startswith(".~")
        )

    @staticmethod
    def _is_trash_relative_path(relative_path: str) -> bool:
        return relative_path == "Trash" or relative_path.startswith("Trash/")

    def get_local_structure(self) -> Dict[str, Dict]:
        """Собирает локальный индекс файлов с хешами и mtime."""
        result: Dict[str, Dict] = {}

        for file_path in self.local_folder.rglob("*"):
            if not file_path.is_file() or self._is_temporary_file(file_path):
                continue
            meta = build_local_file_meta(file_path, self.local_folder)
            result[meta["relative_path"]] = meta
        return result

    def get_differences(self) -> Dict:
        """
        Сравнивает локальные файлы с серверными.
        Возвращает словарь с ключами:
          has_discrepancies, only_local, only_server, content_conflict, path_moved
        """
        remote_files = self.api_client.get_files_structure()
        local_files = self.get_local_structure()
        sync_states = self.db.get_all_sync_states() if self.db else []
        sync_state_by_file_id = {
            state["server_user_file_id"]: state
            for state in sync_states
            if state.get("server_user_file_id") is not None
        }

        remote_paths = set(remote_files.keys())
        local_paths = set(local_files.keys())

        only_local = sorted(local_paths - remote_paths)
        only_server = sorted(remote_paths - local_paths)
        content_conflict = sorted(
            p for p in (local_paths & remote_paths)
            if remote_files[p].content_hash and remote_files[p].content_hash != local_files[p]["content_hash"]
        )
        path_moved = [
            (sync_state["relative_path"], relative_path)
            for relative_path, remote_meta in remote_files.items()
            if remote_meta.file_id is not None
            for sync_state in [sync_state_by_file_id.get(remote_meta.file_id)]
            if sync_state and sync_state["relative_path"] != relative_path
        ]

        return {
            "has_discrepancies": bool(only_local or only_server or content_conflict or path_moved),
            "only_local": only_local,
            "only_server": only_server,
            "content_conflict": content_conflict,
            "path_moved": path_moved,
        }

    def _mark_remote_paths(self, *relative_paths: Optional[str], is_remote: bool):
        if not self.db:
            return
        for relative_path in {path for path in relative_paths if path}:
            self.db.mark_remote_apply(relative_path, is_remote)

    def _download_remote_file(self, relative_path: str, remote_meta) -> bool:
        target_path = self.local_folder / relative_path
        self._mark_remote_paths(relative_path, is_remote=True)

        try:
            downloaded = False
            if remote_meta.file_id:
                downloaded = self.api_client.download_file_by_id(remote_meta.file_id, target_path)
            if not downloaded:
                downloaded = self.api_client.download_file(
                    relative_path=relative_path,
                    destination=target_path,
                    download_url=remote_meta.download_url,
                )
            if not downloaded:
                return False

            if self.db:
                self.db.upsert_sync_state(
                    relative_path,
                    content_hash=compute_file_hash(target_path),
                    last_downloaded_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                    last_seen_mtime=target_path.stat().st_mtime,
                    server_user_file_id=remote_meta.file_id,
                    namespace_id=remote_meta.namespace_id,
                    is_applying_remote=0,
                )
            return True
        finally:
            self._mark_remote_paths(relative_path, is_remote=False)

    def _move_local_file(self, old_relative_path: str, new_relative_path: str) -> bool:
        if old_relative_path == new_relative_path:
            return True

        old_path = self.local_folder / old_relative_path
        new_path = self.local_folder / new_relative_path
        if not old_path.exists():
            return False

        self._mark_remote_paths(old_relative_path, new_relative_path, is_remote=True)
        try:
            new_path.parent.mkdir(parents=True, exist_ok=True)
            if new_path.exists() and new_path != old_path:
                new_path.unlink()
            old_path.rename(new_path)
            if self.db:
                self.db.rename_sync_state(old_relative_path, new_relative_path)
            return True
        finally:
            self._mark_remote_paths(old_relative_path, new_relative_path, is_remote=False)

    def _detect_folder_renames(self, remote_files: Dict[str, object], sync_state_by_file_id: Dict[int, Dict]):
        candidates = {}
        for relative_path, remote_meta in remote_files.items():
            if not remote_meta.file_id or self._is_trash_relative_path(relative_path):
                continue
            sync_state = sync_state_by_file_id.get(remote_meta.file_id)
            if not sync_state:
                continue

            old_relative_path = sync_state["relative_path"]
            if old_relative_path == relative_path:
                continue

            old_parent = Path(old_relative_path).parent.as_posix()
            new_parent = Path(relative_path).parent.as_posix()
            if old_parent in {".", ""} or new_parent in {".", ""} or old_parent == new_parent:
                continue

            key = (old_parent, new_parent)
            candidates[key] = candidates.get(key, 0) + 1

        return sorted(
            [(old_prefix, new_prefix, count) for (old_prefix, new_prefix), count in candidates.items() if count > 1],
            key=lambda item: len(item[0]),
            reverse=True,
        )

    def _apply_folder_rename(self, old_prefix: str, new_prefix: str) -> bool:
        old_path = self.local_folder / old_prefix
        new_path = self.local_folder / new_prefix
        if not old_path.exists() or not old_path.is_dir() or new_path.exists():
            return False

        self._mark_remote_paths(old_prefix, new_prefix, is_remote=True)
        try:
            new_path.parent.mkdir(parents=True, exist_ok=True)
            old_path.rename(new_path)
            if self.db:
                for sync_state in self.db.get_all_sync_states():
                    relative_path = sync_state["relative_path"]
                    if relative_path == old_prefix or relative_path.startswith(f"{old_prefix}/"):
                        suffix = relative_path[len(old_prefix):].lstrip("/")
                        renamed_path = f"{new_prefix}/{suffix}" if suffix else new_prefix
                        self.db.rename_sync_state(relative_path, renamed_path)
            return True
        finally:
            self._mark_remote_paths(old_prefix, new_prefix, is_remote=False)

    def sync(
        self,
        callback: Optional[Callable[[str], None]] = None,
        strategy: SyncStrategy = SyncStrategy.PC_PRIORITY,
    ) -> Dict[str, int]:
        """Сверяет локальные файлы и серверный snapshot."""
        stats = {
            "server_files": 0,
            "local_files": 0,
            "downloaded": 0,
            "uploaded": 0,
            "unchanged": 0,
            "renamed": 0,
        }

        self._callback(callback, "Стартовая синхронизация с backend...")
        remote_files = self.api_client.get_files_structure()
        local_files = self.get_local_structure()
        sync_states = self.db.get_all_sync_states() if self.db else []
        sync_state_by_file_id = {
            state["server_user_file_id"]: state
            for state in sync_states
            if state.get("server_user_file_id") is not None
        }

        stats["server_files"] = len(remote_files)
        stats["local_files"] = len(local_files)

        remote_paths = set(remote_files.keys())
        local_paths = set(local_files.keys())
        only_local = local_paths - remote_paths
        only_server = remote_paths - local_paths
        both = local_paths & remote_paths
        hash_mismatch = [
            p for p in both
            if remote_files[p].content_hash and remote_files[p].content_hash != local_files[p]["content_hash"]
        ]
        self._callback(callback, (
            f"Серверных файлов: {len(remote_files)}, локальных: {len(local_files)} | "
            f"только на ПК: {len(only_local)}, только на сервере: {len(only_server)}, "
            f"совпадают: {len(both) - len(hash_mismatch)}, различается контент: {len(hash_mismatch)}"
        ))
        if only_local:
            self._callback(callback, f"  Только на ПК: {', '.join(sorted(only_local))}")
        if only_server:
            self._callback(callback, f"  Только на сервере: {', '.join(sorted(only_server))}")
        if hash_mismatch:
            self._callback(callback, f"  Различается контент: {', '.join(sorted(hash_mismatch))}")

        for old_prefix, new_prefix, count in self._detect_folder_renames(remote_files, sync_state_by_file_id):
            if strategy == SyncStrategy.SERVER_PRIORITY and self._apply_folder_rename(old_prefix, new_prefix):
                stats["renamed"] += count
                self._callback(callback, f"Переименована папка: {old_prefix} -> {new_prefix}")

        local_files = self.get_local_structure()
        sync_states = self.db.get_all_sync_states() if self.db else []
        sync_state_by_file_id = {
            state["server_user_file_id"]: state
            for state in sync_states
            if state.get("server_user_file_id") is not None
        }
        remote_file_ids = {
            meta.file_id
            for meta in remote_files.values()
            if meta.file_id is not None
        }
        remote_path_by_file_id = {
            meta.file_id: relative_path
            for relative_path, meta in remote_files.items()
            if meta.file_id is not None
        }
        matched_local_paths = set()

        for relative_path, remote_meta in remote_files.items():
            sync_state = sync_state_by_file_id.get(remote_meta.file_id) if remote_meta.file_id is not None else None
            known_relative_path = sync_state["relative_path"] if sync_state else None

            path_changed_by_file_id = bool(
                sync_state and known_relative_path and known_relative_path != relative_path
            )
            if path_changed_by_file_id:
                if strategy == SyncStrategy.SERVER_PRIORITY:
                    # Сервер — источник истины для путей: перемещаем локальный файл
                    if self._move_local_file(known_relative_path, relative_path):
                        stats["renamed"] += 1
                        self._callback(callback, f"Переименован/перемещён файл: {known_relative_path} -> {relative_path}")
                        local_files.pop(known_relative_path, None)
                        if (self.local_folder / relative_path).exists():
                            local_files[relative_path] = build_local_file_meta(
                                self.local_folder / relative_path,
                                self.local_folder,
                            )
                else:
                    # ПК — источник истины для путей: загружаем файл с ПК-пути, сервер обновит путь
                    pc_local_meta = local_files.get(known_relative_path)
                    if pc_local_meta:
                        result = self.uploader.upload_file(
                            self.local_folder / known_relative_path,
                            server_user_file_id=remote_meta.file_id,
                        )
                        if result["status"] == "uploaded":
                            stats["uploaded"] += 1
                            matched_local_paths.add(known_relative_path)
                            self._callback(callback, f"Приоритет ПК (путь): {known_relative_path} (сервер: {relative_path})")
                        elif result["status"] == "failed":
                            self._callback(callback, f"Ошибка загрузки {known_relative_path}: {result.get('message')}")
                    continue

            local_meta = local_files.get(relative_path)
            if not local_meta:
                if self._download_remote_file(relative_path, remote_meta):
                    stats["downloaded"] += 1
                    matched_local_paths.add(relative_path)
                    self._callback(callback, f"Скачан отсутствующий файл: {relative_path}")
                continue

            matched_local_paths.add(relative_path)

            if remote_meta.content_hash and remote_meta.content_hash == local_meta["content_hash"]:
                stats["unchanged"] += 1
                if self.db:
                    self.db.upsert_sync_state(
                        relative_path,
                        content_hash=local_meta["content_hash"],
                        last_seen_mtime=local_meta["last_seen_mtime"],
                        server_user_file_id=remote_meta.file_id,
                        namespace_id=remote_meta.namespace_id,
                        is_applying_remote=0,
                    )
                continue

            if path_changed_by_file_id and strategy == SyncStrategy.SERVER_PRIORITY:
                if self._download_remote_file(relative_path, remote_meta):
                    stats["downloaded"] += 1
                    self._callback(callback, f"Обновлён после remote move: {relative_path}")
                continue

            if strategy == SyncStrategy.SERVER_PRIORITY:
                if self._download_remote_file(relative_path, remote_meta):
                    stats["downloaded"] += 1
                    self._callback(callback, f"Приоритет сервера: {relative_path}")
            else:
                result = self.uploader.upload_file(
                    self.local_folder / relative_path,
                    server_user_file_id=remote_meta.file_id,
                )
                if result["status"] == "uploaded":
                    stats["uploaded"] += 1
                    self._callback(callback, f"Приоритет ПК: {relative_path}")
                elif result["status"] == "failed":
                    self._callback(callback, f"Ошибка загрузки {relative_path}: {result.get('message')}")

        local_files = self.get_local_structure()
        for relative_path in local_files:
            if relative_path in matched_local_paths:
                continue

            sync_state = self.db.get_sync_state(relative_path) if self.db else None
            file_id = sync_state.get("server_user_file_id") if sync_state else None
            remote_relative_path = remote_path_by_file_id.get(file_id) if file_id is not None else None
            if (
                file_id is not None
                and file_id in remote_file_ids
                and remote_relative_path == relative_path
            ):
                continue

            result = self.uploader.upload_file(
                self.local_folder / relative_path,
                server_user_file_id=file_id,
            )
            if result["status"] == "uploaded":
                stats["uploaded"] += 1
                self._callback(callback, f"Baseline upload: {relative_path}")
            elif result["status"] == "skipped":
                self._callback(callback, f"Пропущен upload {relative_path}: {result.get('message')}")
            elif result["status"] == "failed":
                self._callback(callback, f"Ошибка загрузки {relative_path}: {result.get('message')}")

        self._callback(
            callback,
            (
                "Синхронизация завершена: "
                f"локальных={stats['local_files']}, серверных={stats['server_files']}, "
                f"uploaded={stats['uploaded']}, downloaded={stats['downloaded']}, "
                f"unchanged={stats['unchanged']}, renamed={stats['renamed']}"
            ),
        )
        return stats
