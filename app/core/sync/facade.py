"""Фасад синхронизации локальной папки с backend."""

from __future__ import annotations

import logging
import shutil
import time
from pathlib import Path
from typing import Dict, List, Optional, Set

from app.core.api_client import SyncAPIClient
from app.core.contracts import FileInfo, IndexedFileMeta, LocalFileMeta, NamespaceStructureItem
from app.core.db import SettingsDB
from app.core.file_utils import (
    build_local_file_info,
    build_local_file_meta,
    compute_file_hash,
    is_temporary_file,
    normalize_relative_path,
)
from app.core.namespace_constants import (
    INBOX_DIRNAME,
    INBOX_KIND,
    REGULAR_KIND,
    VAULT_ROOT_NAME,
    VAULT_ROOT_KIND,
    TRASH_DIRNAME,
    TRASH_KIND,
)
from app.core.uploader import FileUploader

from .initial import run_initial_sync
from .namespace_utils import (
    build_comparison_dict,
    build_namespace_path_index,
    build_path_by_namespace_id,
    flatten_namespace_list,
    get_namespace_paths,
)
from .restart import RestartSyncRunner
from .types import SyncStrategy


logger = logging.getLogger("app.core.sync")


class FileSync:
    """Синхронизация локальной папки с backend."""

    def __init__(self, api_client: SyncAPIClient, local_folder: Path, uploader: FileUploader, db: SettingsDB):
        self.api_client: SyncAPIClient = api_client
        self.local_folder: Path = Path(local_folder)
        self.uploader: FileUploader = uploader
        self.db: SettingsDB = db

    def _folder_kind_from_path(self, relative_path: str) -> str:
        if relative_path in {"", "."}:
            return VAULT_ROOT_KIND
        if relative_path == INBOX_DIRNAME:
            return INBOX_KIND
        if relative_path == TRASH_DIRNAME:
            return TRASH_KIND
        return REGULAR_KIND

    # Хелперы состояния папок
    def replace_folder_states_from_local(self) -> None:
        local_paths = get_namespace_paths(self.get_local_structure())
        folder_states: List[Dict[str, object]] = []
        for relative_path in sorted(local_paths, key=lambda path: (path.count("/"), path)):
            parent_path = Path(relative_path).parent.as_posix()
            folder_states.append({
                "relative_path": relative_path,
                "namespace_id": None,
                "parent_relative_path": None if parent_path in {".", ""} else parent_path,
                "kind": self._folder_kind_from_path(relative_path),
                "is_applying_remote": False,
                "last_command_id": None,
            })
        self.db.replace_folder_states(folder_states)

    def _replace_folder_states_from_remote(self, namespaces: List[NamespaceStructureItem]) -> None:
        path_by_namespace_id = build_path_by_namespace_id(namespaces)
        folder_states: List[Dict[str, object]] = []
        for namespace in namespaces:
            if namespace.id is None:
                continue
            relative_path = path_by_namespace_id.get(namespace.id, "")
            if not relative_path:
                continue
            parent_path = Path(relative_path).parent.as_posix()
            folder_states.append({
                "relative_path": relative_path,
                "namespace_id": namespace.id,
                "parent_relative_path": None if parent_path in {".", ""} else parent_path,
                "kind": namespace.kind,
                "is_applying_remote": False,
                "last_command_id": None,
            })
        self.db.replace_folder_states(folder_states)

    def _remember_parent_folders(self, relative_path: str, namespace_id: Optional[int] = None) -> None:
        parts = Path(relative_path).parts[:-1]
        for index in range(len(parts)):
            folder_path = Path(*parts[: index + 1]).as_posix()
            parent_path = Path(*parts[:index]).as_posix() if index > 0 else None
            self.db.upsert_folder_state(
                folder_path,
                namespace_id=namespace_id if index == len(parts) - 1 else None,
                parent_relative_path=parent_path,
                kind=self._folder_kind_from_path(folder_path),
            )

    # Публичный API структуры
    def get_local_structure(self) -> List[NamespaceStructureItem]:
        """Собирает локальную структуру в формате backend namespace list."""
        namespace_paths = {""}
        files_by_namespace: Dict[str, List[FileInfo]] = {}
        sorted_paths = sorted(
            self.local_folder.rglob("*"),
            key=lambda path: path.as_posix(),
        )

        for dir_path in sorted_paths:
            if not dir_path.is_dir():
                continue
            try:
                relative_path = normalize_relative_path(dir_path, self.local_folder)
            except ValueError:
                continue
            namespace_paths.add(relative_path)


        for file_path in sorted_paths:
            if not file_path.is_file() or is_temporary_file(file_path):
                continue

            try:
                relative_path = normalize_relative_path(file_path, self.local_folder)
            except ValueError:
                continue

            parent_dir = Path(relative_path).parent.as_posix()
            namespace_path = "" if parent_dir in {".", ""} else parent_dir
            namespace_paths.add(namespace_path)
            files_by_namespace.setdefault(namespace_path, []).append(build_local_file_info(file_path))

        ordered_paths = sorted(namespace_paths, key=lambda path: (path.count("/"), path))
        namespace_items: List[NamespaceStructureItem] = []
        namespace_id_by_path: Dict[str, int] = {}

        for next_namespace_id, namespace_path in enumerate(ordered_paths, start=1):
            if namespace_path:
                parts = namespace_path.split("/")
                parent_path = "/".join(parts[:-1]) if len(parts) > 1 else ""
                namespace_name = parts[-1]
            else:
                parent_path = ""
                namespace_name = VAULT_ROOT_NAME
            namespace = NamespaceStructureItem(
                id=next_namespace_id,
                name=namespace_name,
                parent_id=None if not namespace_path else namespace_id_by_path.get(parent_path),
                kind=VAULT_ROOT_KIND if not namespace_path else (
                    INBOX_KIND if namespace_name == INBOX_DIRNAME else (
                        TRASH_KIND if namespace_name == TRASH_DIRNAME else REGULAR_KIND
                    )
                ),
                files=files_by_namespace.get(namespace_path, []),
            )
            namespace_items.append(namespace)
            namespace_id_by_path[namespace_path] = next_namespace_id

        return sorted(namespace_items, key=lambda item: item.id if item.id is not None else 0)

    # Хелперы локальной файловой системы
    def get_local_file_index(self) -> Dict[str, LocalFileMeta]:
        """Создает словарь `relative_path -> LocalFileMeta`."""
        result: Dict[str, LocalFileMeta] = {}
        for file_path in self.local_folder.rglob("*"):
            if not file_path.is_file() or is_temporary_file(file_path):
                continue
            try:
                file_meta = build_local_file_meta(file_path, self.local_folder)
            except ValueError:
                continue
            result[file_meta.relative_path] = file_meta
        return result

    @staticmethod
    def _prune_empty_parents(start_dir: Path, stop_dir: Path):
        current = start_dir
        while current != stop_dir and current.exists():
            try:
                current.rmdir()
            except OSError:
                break
            current = current.parent

    def create_local_folder_if_not_exists(self, relative_dir: str) -> bool:
        if not relative_dir:
            return False
        target_dir = self.local_folder / relative_dir
        created = False
        if not target_dir.exists():
            target_dir.mkdir(parents=True, exist_ok=True)
            created = True
        parent_path = Path(relative_dir).parent.as_posix()
        self.db.upsert_folder_state(
            relative_dir,
            parent_relative_path=None if parent_path in {".", ""} else parent_path,
            kind=self._folder_kind_from_path(relative_dir),
        )
        return created

    def delete_local_namespace_if_empty(self, relative_dir: str) -> bool:
        if not relative_dir:
            return False
        target_dir = self.local_folder / relative_dir
        if not target_dir.exists() or not target_dir.is_dir():
            return False
        try:
            target_dir.rmdir()
            self._prune_empty_parents(target_dir.parent, self.local_folder)
            self.replace_folder_states_from_local()
            return True
        except OSError:
            return False

    # Хелперы удалённых namespace
    def create_remote_namespace_tree(
        self,
        namespace_paths: Set[str],
        remote_structure: List[NamespaceStructureItem],
    ) -> int:
        """Создаёт на сервере отсутствующие namespace по путям."""
        created = 0
        namespace_by_path = build_namespace_path_index(remote_structure)

        for namespace_path in sorted(namespace_paths, key=lambda path: (path.count("/"), path)):
            if not namespace_path or namespace_path in {INBOX_DIRNAME, TRASH_DIRNAME} or namespace_path in namespace_by_path:
                continue

            created_namespace = self.api_client.create_namespace(
                relative_path=namespace_path,
            )
            if created_namespace is None:
                logger.warning(f"Не удалось создать namespace на сервере: {namespace_path}")
                continue

            remote_structure.append(created_namespace)
            namespace_by_path[namespace_path] = created_namespace
            parent_path = Path(namespace_path).parent.as_posix()
            self.db.upsert_folder_state(
                namespace_path,
                namespace_id=created_namespace.id,
                parent_relative_path=None if parent_path in {".", ""} else parent_path,
                kind=created_namespace.kind,
            )
            created += 1
            logger.info(f"Создан namespace на сервере: {namespace_path}")

        return created

    def delete_remote_namespaces(
        self,
        namespace_paths: Set[str],
        remote_structure: List[NamespaceStructureItem],
    ) -> int:
        """Удаляет namespace на сервере по путям, начиная с самых глубоких."""
        deleted = 0
        namespace_by_path = build_namespace_path_index(remote_structure)

        for namespace_path in sorted(namespace_paths, key=lambda path: (path.count("/"), path), reverse=True):
            namespace = namespace_by_path.get(namespace_path)
            if not namespace or namespace.id is None or namespace.kind in {TRASH_KIND, INBOX_KIND}:
                continue

            if not self.api_client.delete_namespace(namespace.id):
                logger.warning(f"Не удалось удалить namespace на сервере: {namespace_path}")
                continue

            deleted += 1
            logger.info(f"Удалён namespace на сервере: {namespace_path}")
            remote_structure[:] = [
                item
                for item in remote_structure
                if item.id != namespace.id
            ]
            namespace_by_path.pop(namespace_path, None)
            self.db.delete_folder_state(namespace_path)

        return deleted

    def delete_remote_files_under_prefixes(
        self,
        namespace_paths: Set[str],
        remote_files: Dict[str, IndexedFileMeta],
    ) -> int:
        """Полностью удаляет на сервере файлы под указанными namespace-префиксами."""
        deleted = 0
        for namespace_path in sorted(namespace_paths, key=lambda path: (path.count("/"), path), reverse=True):
            prefix = f"{namespace_path}/"
            for relative_path, remote_meta in list(remote_files.items()):
                if not (relative_path == namespace_path or relative_path.startswith(prefix)):
                    continue
                file_id = remote_meta.file_id
                if not file_id:
                    continue
                if self.api_client.delete_server_file(file_id):
                    deleted += 1
                    remote_files.pop(relative_path, None)
                    logger.info(f"Удалён файл на сервере по локальному удалению папки: {relative_path}")
        return deleted

    # Публичный API синхронизации
    def restore_namespace_from_server(self, namespace_path: str) -> Dict[str, int]:
        """Восстанавливает локальный namespace subtree по серверной структуре."""
        remote_structure = self.api_client.get_files_server_structure()
        remote_namespace_paths = get_namespace_paths(remote_structure)
        remote_files = flatten_namespace_list(remote_structure)
        restored = {
            "created_dirs": 0,
            "downloaded_files": 0,
        }

        if namespace_path not in remote_namespace_paths:
            logger.warning(f"Namespace {namespace_path} не найден на сервере")
            return restored

        namespace_prefix = f"{namespace_path}/"
        target_namespaces = {
            path
            for path in remote_namespace_paths
            if path == namespace_path or path.startswith(namespace_prefix)
        }
        for target_namespace_path in sorted(target_namespaces, key=lambda path: (path.count("/"), path)):
            if self.create_local_folder_if_not_exists(target_namespace_path):
                restored["created_dirs"] += 1
                logger.info(f"Создана локальная папка по серверной структуре: {target_namespace_path}")

        for relative_path, remote_meta in remote_files.items():
            if relative_path == namespace_path or not (
                relative_path.startswith(namespace_prefix)
                or relative_path == namespace_path
            ):
                continue
            action = self.apply_remote_file_state(remote_meta)
            if action in {"downloaded", "moved"}:
                restored["downloaded_files"] += 1
                logger.info(f"Восстановлен файл из серверной папки {namespace_path}: {relative_path}")

        self.replace_folder_states_from_local()

        return restored

    # Хелперы передачи файлов и snapshot
    def _mark_remote_paths(self, *relative_paths: Optional[str], is_remote: bool):
        for relative_path in {path for path in relative_paths if path}:
            self.db.mark_file_remote_apply(relative_path, is_remote)

    def download_remote_file(self, relative_path: str, remote_meta: IndexedFileMeta) -> bool:
        target_path = self.local_folder / relative_path
        self._mark_remote_paths(relative_path, is_remote=True)

        try:
            downloaded = False
            file_id = remote_meta.file_id
            if file_id:
                downloaded = self.api_client.download_file_by_id(file_id, target_path)
            if not downloaded:
                downloaded = self.api_client.download_file(
                    relative_path=relative_path,
                    destination=target_path,
                    download_url=None,
                )
            if not downloaded:
                return False

            self._remember_parent_folders(relative_path, namespace_id=remote_meta.namespace_id)
            self.db.upsert_file_state(
                relative_path,
                content_hash=compute_file_hash(target_path),
                last_downloaded_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                last_seen_mtime=target_path.stat().st_mtime,
                server_user_file_id=file_id,
                namespace_id=remote_meta.namespace_id,
                is_applying_remote=0,
            )
            return True
        finally:
            self._mark_remote_paths(relative_path, is_remote=False)

    def _move_local_file(self, old_relative_path: str, new_relative_path: str):
        if old_relative_path == new_relative_path:
            return

        old_path = self.local_folder / old_relative_path
        new_path = self.local_folder / new_relative_path
        if not old_path.exists():
            return

        new_path.parent.mkdir(parents=True, exist_ok=True)
        if new_path.exists() and new_path != old_path:
            new_path.unlink()
        old_path.rename(new_path)
        self._prune_empty_parents(old_path.parent, self.local_folder)
        self.replace_folder_states_from_local()

    def delete_local_file(self, relative_path: str) -> bool:
        target_path = self.local_folder / relative_path
        if not target_path.exists():
            return True

        self._mark_remote_paths(relative_path, is_remote=True)
        try:
            if target_path.is_file():
                target_path.unlink()
            self._prune_empty_parents(target_path.parent, self.local_folder)
            self.db.delete_file_state(relative_path)
            self.replace_folder_states_from_local()
            return True
        finally:
            self._mark_remote_paths(relative_path, is_remote=False)

    def apply_remote_file_state(
        self,
        remote_meta: IndexedFileMeta,
        previous_relative_path: Optional[str] = None,
    ) -> str:
        """Применяет серверное состояние на ПК, по возможности без лишней загрузки."""
        target_path = self.local_folder / remote_meta.relative_path
        moved = False

        if previous_relative_path and previous_relative_path != remote_meta.relative_path:
            old_path = self.local_folder / previous_relative_path
            if old_path.exists():
                self._move_local_file(previous_relative_path, remote_meta.relative_path)
                moved = True

        if target_path.exists() and remote_meta.content_hash:
            local_hash = compute_file_hash(target_path)
            if local_hash == remote_meta.content_hash:
                self._remember_parent_folders(remote_meta.relative_path, namespace_id=remote_meta.namespace_id)
                self.db.upsert_file_state(
                    remote_meta.relative_path,
                    content_hash=remote_meta.content_hash,
                    last_downloaded_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                    last_seen_mtime=target_path.stat().st_mtime,
                    server_user_file_id=remote_meta.file_id,
                    namespace_id=remote_meta.namespace_id,
                    is_applying_remote=0,
                )
                if previous_relative_path and previous_relative_path != remote_meta.relative_path:
                    self.db.delete_file_state(previous_relative_path)
                return "moved" if moved else "unchanged"

        downloaded = self.download_remote_file(remote_meta.relative_path, remote_meta)
        if downloaded and previous_relative_path and previous_relative_path != remote_meta.relative_path:
            self.db.delete_file_state(previous_relative_path)
        if downloaded:
            return "downloaded"
        return "failed"

    def upload_local_file(
        self,
        relative_path: str,
        server_user_file_id: Optional[int] = None,
    ) -> Dict[str, str]:
        return self.uploader.upload_file(
            self.local_folder / relative_path,
            server_user_file_id=server_user_file_id,
        )

    def empty_local_folder(self):
        for child in self.local_folder.iterdir():
            if child.is_dir():
                shutil.rmtree(child)
            else:
                child.unlink()

    def refresh_last_sync_snapshot_if_synced(self):
        """Проверяет, что сервер и ПК синхронизированы и обновляет last_sync_snapshot."""
        remote_structure = self.api_client.get_files_server_structure()

        remote_files_dict = build_comparison_dict(flatten_namespace_list(remote_structure))
        local_files_dict = build_comparison_dict(self.get_local_file_index())

        dir_local_paths = get_namespace_paths(self.get_local_structure())
        dir_remote_paths = get_namespace_paths(remote_structure)

        if local_files_dict != remote_files_dict or dir_local_paths != dir_remote_paths:
            logger.warning("ПК и сервер не синхронизированы, пропускаю обновление last_sync_snapshot")
            local_only_files = sorted(set(local_files_dict) - set(remote_files_dict))
            remote_only_files = sorted(set(remote_files_dict) - set(local_files_dict))
            changed_files = sorted(
                path
                for path in set(local_files_dict) & set(remote_files_dict)
                if local_files_dict[path] != remote_files_dict[path]
            )
            local_only_dirs = sorted(dir_local_paths - dir_remote_paths)
            remote_only_dirs = sorted(dir_remote_paths - dir_local_paths)

            if local_only_files:
                logger.warning("Только локальные файлы: %s", local_only_files)
            if remote_only_files:
                logger.warning("Только серверные файлы: %s", remote_only_files)
            if changed_files:
                logger.warning("Файлы с разным content_hash: %s", changed_files)
            if local_only_dirs:
                logger.warning("Только локальные папки: %s", local_only_dirs)
            if remote_only_dirs:
                logger.warning("Только серверные папки: %s", remote_only_dirs)
            return

        logger.info("ПК и сервер синхронизированы, обновляю last_sync_snapshot...")
        self.db.save_last_sync_snapshot(remote_structure)
        self._replace_folder_states_from_remote(remote_structure)

    # Высокоуровневые сценарии синхронизации
    def initial_sync(
        self,
        strategy: Optional[SyncStrategy] = None,
    ) -> None:
        """Первичная инициализация связки token + folder."""
        run_initial_sync(self, strategy)

    def sync_after_restart(self) -> None:
        """
        Сверяет ПК и сервер относительно last_sync_snapshot после простоя watcher.
        """
        RestartSyncRunner(self).run()
