"""Первичная синхронизация после рестарта watcher."""

import enum
import logging
import shutil
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Set

from app.core.api_client import SyncAPIClient
from app.core.contracts import FileInfo, IndexedFileMeta, LocalFileMeta, NamespaceStructureItem
from app.core.database import SettingsDB
from app.core.file_utils import build_local_file_info, build_local_file_meta, compute_file_hash, normalize_relative_path
from app.core.namespace_constants import (
    INBOX_DIRNAME,
    INBOX_KIND,
    REGULAR_KIND,
    TRASH_DIRNAME,
    TRASH_KIND,
)
from app.core.uploader import FileUploader

logger = logging.getLogger(__name__)


class SyncStrategy(enum.Enum):
    PC_PRIORITY = "pc"
    SERVER_PRIORITY = "server"
    SKIP = "skip"


@dataclass
class RestartSyncContext:
    """Внутренний контейнер состояния для sync_after_restart()."""
    local_structure: List[NamespaceStructureItem]
    remote_structure: List[NamespaceStructureItem]
    last_structure: List[NamespaceStructureItem]
    local_files: Dict[str, LocalFileMeta]
    remote_files: Dict[str, IndexedFileMeta]
    last_sync: Dict[str, Dict[str, object]]
    last_paths: Set[str]
    local_namespace_paths: Set[str]
    remote_namespace_paths: Set[str]
    last_namespace_paths: Set[str]
    remote_by_file_id: Dict[int, IndexedFileMeta]
    processed_local_paths: Set[str]
    claimed_local_paths: Set[str]
    processed_remote_paths: Set[str]
    stats: Dict[str, int]
    new_remote_dirs: Set[str]
    new_local_dirs: Set[str]
    removed_remote_dirs: Set[str]
    removed_local_dirs: Set[str]


class FileSync:
    """Синхронизация локальной папки с backend."""

    def __init__(self, api_client: SyncAPIClient, local_folder: Path, uploader: FileUploader, db: SettingsDB):
        self.api_client: SyncAPIClient = api_client
        self.local_folder: Path = Path(local_folder)
        self.uploader: FileUploader = uploader
        self.db: SettingsDB = db

    # Хелперы структуры и индексов
    @staticmethod
    def _is_temporary_file(file_path: Path) -> bool:
        name = file_path.name
        return (
            name.startswith("~$")
            or name.endswith(".tmp")
            or name.endswith(".temp")
            or name.startswith(".~")
        )

    def _build_path_by_namespace_id(self, namespaces: List[NamespaceStructureItem]) -> Dict[int, str]:
        namespace_by_id = {
            namespace.id: namespace
            for namespace in namespaces
            if namespace.id is not None
        }
        path_by_namespace_id: Dict[int, str] = {}

        def resolve_path(namespace_id: int) -> str:
            if namespace_id in path_by_namespace_id:
                return path_by_namespace_id[namespace_id]

            namespace = namespace_by_id[namespace_id]
            if namespace.kind == INBOX_KIND:
                path = INBOX_DIRNAME
            elif namespace.kind == TRASH_KIND:
                path = TRASH_DIRNAME
            elif namespace.parent_id is None or namespace.parent_id not in namespace_by_id:
                path = namespace.name
            else:
                parent_path = resolve_path(namespace.parent_id)
                path = f"{parent_path}/{namespace.name}" if parent_path else namespace.name

            path_by_namespace_id[namespace_id] = path
            return path

        for namespace_id in namespace_by_id:
            resolve_path(namespace_id)
        return path_by_namespace_id

    def flatten_namespace_list(self, namespaces: List[NamespaceStructureItem]) -> Dict[str, IndexedFileMeta]:
        """Строит словарь `relative_path -> IndexedFileMeta`."""
        path_by_namespace_id = self._build_path_by_namespace_id(namespaces)
        result: Dict[str, IndexedFileMeta] = {}

        for namespace in namespaces:
            namespace_path = path_by_namespace_id.get(namespace.id, "") if namespace.id is not None else ""
            for file_info in namespace.files:
                relative_path = f"{namespace_path}/{file_info.filename}" if namespace_path else file_info.filename
                result[relative_path] = IndexedFileMeta(
                    relative_path=relative_path,
                    filename=file_info.filename,
                    file_size=file_info.file_size,
                    content_hash=file_info.content_hash,
                    updated_at=file_info.updated_at,
                    file_id=file_info.id,
                    namespace_id=namespace.id,
                )

        return result


    def _build_comparison_dict(self, file_index: Dict[str, LocalFileMeta | IndexedFileMeta]) -> Dict[str, str]:
        return {
            relative_path: file_meta.content_hash
            for relative_path, file_meta in file_index.items()
        }

    def _get_namespace_paths(self, namespaces: List[NamespaceStructureItem]) -> Set[str]:
        """
        Возвращает набор namespace-путей.
        Args:
            namespaces: Список NamespaceStructureItem.
        Returns:
            Множество namespace-путей.
        """
        path_by_namespace_id = self._build_path_by_namespace_id(namespaces)
        return {
            path_by_namespace_id[namespace.id]
            for namespace in namespaces
            if namespace.id is not None and path_by_namespace_id.get(namespace.id, "")
        }

    def _build_namespace_path_index(self, namespaces: List[NamespaceStructureItem]) -> Dict[str, NamespaceStructureItem]:
        """Строит индекс namespace_path -> NamespaceStructureItem."""
        path_by_namespace_id = self._build_path_by_namespace_id(namespaces)
        result: Dict[str, NamespaceStructureItem] = {}
        for namespace in namespaces:
            if namespace.id is None:
                continue
            namespace_path = path_by_namespace_id.get(namespace.id, "")
            if namespace_path:
                result[namespace_path] = namespace
        return result

    def _folder_kind_from_path(self, relative_path: str) -> str:
        if relative_path == INBOX_DIRNAME:
            return INBOX_KIND
        if relative_path == TRASH_DIRNAME:
            return TRASH_KIND
        return REGULAR_KIND

    # Хелперы состояния папок
    def _replace_folder_states_from_local(self) -> None:
        local_paths = self._get_namespace_paths(self.get_local_structure())
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
        path_by_namespace_id = self._build_path_by_namespace_id(namespaces)
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
        namespace_paths = set()
        files_by_namespace: Dict[str, List[FileInfo]] = {}

        sorted_paths = sorted(self.local_folder.rglob("*"), key=lambda x: x.name)

        for dir_path in sorted_paths:
            if not dir_path.is_dir():
                continue
            try:
                relative_path = normalize_relative_path(dir_path, self.local_folder)
            except ValueError:
                continue
            namespace_paths.add(relative_path)

        for file_path in sorted_paths:
            if not file_path.is_file() or self._is_temporary_file(file_path):
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
            parts = namespace_path.split("/")
            parent_path = "/".join(parts[:-1]) if len(parts) > 1 else ""
            namespace = NamespaceStructureItem(
                id=next_namespace_id,
                name=INBOX_DIRNAME if namespace_path == INBOX_DIRNAME else parts[-1],
                parent_id=namespace_id_by_path.get(parent_path),
                kind=INBOX_KIND if namespace_path == INBOX_DIRNAME else (
                    TRASH_KIND if namespace_path == TRASH_DIRNAME else REGULAR_KIND
                ),
                files=files_by_namespace.get(namespace_path, []),
            )
            namespace_items.append(namespace)
            namespace_id_by_path[namespace_path] = next_namespace_id

        return sorted(namespace_items, key=lambda item: item.id if item.id is not None else 0)

    # Хелперы локальной файловой системы
    def _get_local_file_index(self) -> Dict[str, LocalFileMeta]:
        """Создает словарь `relative_path -> LocalFileMeta`."""
        result: Dict[str, LocalFileMeta] = {}
        for file_path in self.local_folder.rglob("*"):
            if not file_path.is_file() or self._is_temporary_file(file_path):
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

    def _create_local_folder_if_not_exists(self, relative_dir: str) -> bool:
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

    def _delete_local_namespace_if_empty(self, relative_dir: str) -> bool:
        if not relative_dir:
            return False
        target_dir = self.local_folder / relative_dir
        if not target_dir.exists() or not target_dir.is_dir():
            return False
        try:
            target_dir.rmdir()
            self._prune_empty_parents(target_dir.parent, self.local_folder)
            self._replace_folder_states_from_local()
            return True
        except OSError:
            return False

    # Хелперы удалённых namespace
    def _create_remote_namespace_tree(
        self,
        namespace_paths: Set[str],
        remote_structure: List[NamespaceStructureItem],
    ) -> int:
        """Создаёт на сервере отсутствующие namespace по путям."""
        created = 0
        namespace_by_path = self._build_namespace_path_index(remote_structure)

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

    def _delete_remote_namespaces(
        self,
        namespace_paths: Set[str],
        remote_structure: List[NamespaceStructureItem],
    ) -> int:
        """Удаляет namespace на сервере по путям, начиная с самых глубоких."""
        deleted = 0
        namespace_by_path = self._build_namespace_path_index(remote_structure)

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

    def _delete_remote_files_under_prefixes(
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
        remote_namespace_paths = self._get_namespace_paths(remote_structure)
        remote_files = self.flatten_namespace_list(remote_structure)
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
            if self._create_local_folder_if_not_exists(target_namespace_path):
                restored["created_dirs"] += 1
                logger.info(f"Создана локальная папка по серверной структуре: {target_namespace_path}")

        for relative_path, remote_meta in remote_files.items():
            if relative_path == namespace_path or not (
                relative_path.startswith(namespace_prefix)
                or relative_path == namespace_path
            ):
                continue
            action = self._apply_remote_file_state(remote_meta)
            if action in {"downloaded", "moved"}:
                restored["downloaded_files"] += 1
                logger.info(f"Восстановлен файл из серверной папки {namespace_path}: {relative_path}")

        self._replace_folder_states_from_local()

        return restored

    # Хелперы передачи файлов и snapshot
    def _mark_remote_paths(self, *relative_paths: Optional[str], is_remote: bool):
        for relative_path in {path for path in relative_paths if path}:
            self.db.mark_file_remote_apply(relative_path, is_remote)

    def _download_remote_file(self, relative_path: str, remote_meta: IndexedFileMeta) -> bool:
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
        self._replace_folder_states_from_local()

    def _delete_local_file(self, relative_path: str) -> bool:
        target_path = self.local_folder / relative_path
        if not target_path.exists():
            return True

        self._mark_remote_paths(relative_path, is_remote=True)
        try:
            if target_path.is_file():
                target_path.unlink()
            self._prune_empty_parents(target_path.parent, self.local_folder)
            self.db.delete_file_state(relative_path)
            self._replace_folder_states_from_local()
            return True
        finally:
            self._mark_remote_paths(relative_path, is_remote=False)

    def _apply_remote_file_state(
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

        downloaded = self._download_remote_file(remote_meta.relative_path, remote_meta)
        if downloaded and previous_relative_path and previous_relative_path != remote_meta.relative_path:
            self.db.delete_file_state(previous_relative_path)
        if downloaded:
            return "downloaded"
        return "failed"

    def _upload_local_file(
        self,
        relative_path: str,
        server_user_file_id: Optional[int] = None,
    ) -> Dict[str, str]:
        return self.uploader.upload_file(
            self.local_folder / relative_path,
            server_user_file_id=server_user_file_id,
        )

    def _find_local_move_candidate(
        self,
        last_state: Dict[str, object],
        local_files: Dict[str, LocalFileMeta],
        last_paths: Set[str],
        claimed_local_paths: Set[str],
    ) -> Optional[LocalFileMeta]:
        last_hash = last_state.get("content_hash")
        if not last_hash:
            return None

        candidates = [
            local_meta
            for relative_path, local_meta in local_files.items()
            if relative_path not in last_paths
            and relative_path not in claimed_local_paths
            and local_meta.content_hash == last_hash
        ]
        if len(candidates) == 1:
            return candidates[0]
        return None

    def _empty_local_folder(self):
        for child in self.local_folder.iterdir():
            if child.is_dir():
                shutil.rmtree(child)
            else:
                child.unlink()

    def _refresh_last_sync_snapshot_if_synced(self):
        """Проверяет, что сервер и ПК синхронизированы и обновляет last_sync_snapshot."""
        remote_structure = self.api_client.get_files_server_structure()

        remote_files_dict = self._build_comparison_dict(self.flatten_namespace_list(remote_structure))
        local_files_dict = self._build_comparison_dict(self._get_local_file_index())

        dir_local_paths = self._get_namespace_paths(self.get_local_structure())
        dir_remote_paths = self._get_namespace_paths(remote_structure)


        if local_files_dict != remote_files_dict or dir_local_paths != dir_remote_paths:
            logger.warning("ПК и сервер не синхронизированы, пропускаю обновление last_sync_snapshot")
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
        
        local_structure = self.get_local_structure()
        remote_structure = self.api_client.get_files_server_structure()
        local_files = self._get_local_file_index()
        remote_files = self.flatten_namespace_list(remote_structure)
        local_namespace_paths = self._get_namespace_paths(local_structure)
        remote_namespace_paths = self._get_namespace_paths(remote_structure)

        logger.info("Запущена первичная инициализация синхронизации...")


        if strategy == SyncStrategy.SKIP:
            logger.info("Пропускаю первичную инициализацию синхронизации...")

        elif strategy == SyncStrategy.SERVER_PRIORITY:
            logger.info("Заменяю локальную папку серверной структурой...")
            self._empty_local_folder()
            for namespace_path in sorted(remote_namespace_paths, key=lambda path: (path.count("/"), path)):
                if self._create_local_folder_if_not_exists(namespace_path):
                    logger.info(f"Создана локальная папка по серверной структуре: {namespace_path}")
            for relative_path, remote_meta in remote_files.items():
                if self._download_remote_file(relative_path, remote_meta):
                    logger.info(f"Скачан файл с сервера: {relative_path}")
        elif strategy == SyncStrategy.PC_PRIORITY:
            logger.info("Заменяю серверное пространство локальной структурой...")
            for relative_path, remote_meta in remote_files.items():
                file_id = remote_meta.file_id
                if file_id and self.api_client.delete_server_file(file_id):
                    logger.info(f"Удалён файл на сервере: {relative_path}")
            self._delete_remote_namespaces(
                remote_namespace_paths - local_namespace_paths,
                remote_structure,
            )
            self._create_remote_namespace_tree(
                local_namespace_paths,
                remote_structure,
            )
            for relative_path in sorted(local_files.keys()):
                result = self.uploader.upload_file(self.local_folder / relative_path)
                if result["status"] == "uploaded":
                    logger.info(f"Загружен файл на сервер: {relative_path}")
                elif result["status"] == "failed":
                    logger.warning(f"Ошибка загрузки {relative_path}: {result.get('message')}")
    


        
        self._refresh_last_sync_snapshot_if_synced()

    def sync_after_restart(self) -> None:
        """
        Сверяет ПК и сервер относительно last_sync_snapshot после простоя watcher. 
        """
        logger.info("Запущена синхронизация после перезапуска watcher...")
        context = self._build_restart_sync_context()
        self._sync_directory_changes(context)
        for last_path, last_state in context.last_sync.items():
            self._sync_known_file_state(context, last_path, last_state)
        self._upload_new_local_files(context)
        self._apply_new_remote_files(context)
        self._apply_removed_directory_changes(context)
        self._refresh_last_sync_snapshot_if_synced()

    # Хелперы синхронизации после перезапуска
    def _build_restart_sync_context(self) -> RestartSyncContext:
        local_structure = self.get_local_structure()
        remote_structure = self.api_client.get_files_server_structure()
        last_structure = self.db.get_last_sync_snapshot()

        local_files = self._get_local_file_index()
        remote_files = self.flatten_namespace_list(remote_structure)
        last_sync = {
            state.relative_path: {
                "relative_path": state.relative_path,
                "content_hash": state.content_hash,
                "server_user_file_id": state.file_id,
                "namespace_id": state.namespace_id,
            }
            for state in self.flatten_namespace_list(last_structure).values()
        }
        last_paths = set(last_sync.keys())
        local_namespace_paths = self._get_namespace_paths(local_structure)
        remote_namespace_paths = self._get_namespace_paths(remote_structure)
        last_namespace_paths = self._get_namespace_paths(last_structure)

        return RestartSyncContext(
            local_structure=local_structure,
            remote_structure=remote_structure,
            last_structure=last_structure,
            local_files=local_files,
            remote_files=remote_files,
            last_sync=last_sync,
            last_paths=last_paths,
            local_namespace_paths=local_namespace_paths,
            remote_namespace_paths=remote_namespace_paths,
            last_namespace_paths=last_namespace_paths,
            remote_by_file_id={
                remote_meta.file_id: remote_meta
                for remote_meta in remote_files.values()
                if remote_meta.file_id is not None
            },
            processed_local_paths=set(),
            claimed_local_paths=set(),
            processed_remote_paths=set(),
            stats={
                "server_files": len(remote_files),
                "local_files": len(local_files),
                "uploaded": 0,
                "downloaded": 0,
                "deleted_local": 0,
                "deleted_remote": 0,
                "conflicts": 0,
                "created_local_dirs": 0,
                "deleted_local_dirs": 0,
                "created_remote_dirs": 0,
                "deleted_remote_dirs": 0,
            },
            new_remote_dirs=(remote_namespace_paths - last_namespace_paths) - local_namespace_paths,
            new_local_dirs=(local_namespace_paths - last_namespace_paths) - remote_namespace_paths,
            removed_remote_dirs=(last_namespace_paths & local_namespace_paths) - remote_namespace_paths,
            removed_local_dirs=(last_namespace_paths & remote_namespace_paths) - local_namespace_paths,
        )

    def _sync_directory_changes(self, context: RestartSyncContext) -> None:
        for namespace_path in sorted(context.new_remote_dirs, key=lambda path: (path.count("/"), path)):
            if self._create_local_folder_if_not_exists(namespace_path):
                context.stats["created_local_dirs"] += 1
                logger.info(f"Создана локальная папка по серверной структуре: {namespace_path}")

        context.stats["created_remote_dirs"] = self._create_remote_namespace_tree(
            context.new_local_dirs,
            context.remote_structure,
        )
        context.remote_namespace_paths = self._get_namespace_paths(context.remote_structure)

    def _sync_known_file_state(
        self,
        context: RestartSyncContext,
        last_path: str,
        last_state: Dict[str, object],
    ) -> None:
        last_hash = last_state.get("content_hash")
        last_file_id = last_state.get("server_user_file_id")

        local_meta = context.local_files.get(last_path)
        local_path = last_path if local_meta else None
        if not local_meta:
            moved_candidate = self._find_local_move_candidate(
                last_state,
                context.local_files,
                context.last_paths,
                context.claimed_local_paths,
            )
            if moved_candidate:
                local_meta = moved_candidate
                local_path = moved_candidate.relative_path
                context.claimed_local_paths.add(local_path)

        remote_meta = (
            context.remote_by_file_id.get(last_file_id)
            if last_file_id is not None
            else context.remote_files.get(last_path)
        )
        remote_path = remote_meta.relative_path if remote_meta else None
        if remote_path:
            context.processed_remote_paths.add(remote_path)

        local_exists = local_meta is not None
        remote_exists = remote_meta is not None
        if local_exists and local_path:
            context.processed_local_paths.add(local_path)

        local_changed = (
            not local_exists
            or local_path != last_path
            or local_meta.content_hash != last_hash
        )
        remote_changed = (
            not remote_exists
            or remote_path != last_path
            or remote_meta.content_hash != last_hash
        )

        if not local_changed and not remote_changed:
            return

        if local_changed and not remote_changed:
            self._apply_local_change_after_restart(
                context,
                last_path,
                last_file_id,
                local_meta,
                local_path,
            )
            return

        if remote_changed and not local_changed:
            self._apply_remote_change_after_restart(
                context,
                last_path,
                remote_meta,
                remote_path,
            )
            return

        if (
            local_exists
            and remote_exists
            and local_path == remote_path
            and local_meta.content_hash == remote_meta.content_hash
        ):
            return

        self._resolve_restart_conflict(
            context,
            last_path,
            last_file_id,
            local_meta,
        )

    def _apply_local_change_after_restart(
        self,
        context: RestartSyncContext,
        last_path: str,
        last_file_id: Optional[int],
        local_meta: Optional[LocalFileMeta],
        local_path: Optional[str],
    ) -> None:
        if not local_meta:
            if last_file_id and self.api_client.delete_desktop_file(last_file_id):
                context.stats["deleted_remote"] += 1
                logger.info(f"Удалён на сервере после локального удаления: {last_path}")
            return

        result = self._upload_local_file(
            local_meta.relative_path,
            server_user_file_id=last_file_id,
        )
        if result["status"] == "uploaded":
            context.stats["uploaded"] += 1
            if local_path != last_path:
                logger.info(f"Локальное перемещение отправлено на сервер: {last_path} -> {local_path}")
            else:
                logger.info(f"Локальное изменение отправлено на сервер: {local_path}")
        elif result["status"] == "failed":
            logger.warning(f"Ошибка отправки локального изменения {local_meta.relative_path}: {result.get('message')}")

    def _apply_remote_change_after_restart(
        self,
        context: RestartSyncContext,
        last_path: str,
        remote_meta: Optional[IndexedFileMeta],
        remote_path: Optional[str],
    ) -> None:
        if not remote_meta:
            if self._delete_local_file(last_path):
                context.stats["deleted_local"] += 1
                logger.info(f"Удалён локальный файл по серверному удалению: {last_path}")
            return

        action = self._apply_remote_file_state(remote_meta, previous_relative_path=last_path)
        if action in {"downloaded", "moved"}:
            context.stats["downloaded"] += 1
        if action != "failed":
            if remote_path != last_path:
                logger.info(f"Серверное перемещение применено локально: {last_path} -> {remote_path}")
            else:
                logger.info(f"Серверное изменение применено локально: {remote_path}")
        else:
            logger.warning(f"Ошибка применения серверного изменения: {remote_path or last_path}")

    def _resolve_restart_conflict(
        self,
        context: RestartSyncContext,
        last_path: str,
        last_file_id: Optional[int],
        local_meta: Optional[LocalFileMeta],
    ) -> None:
        context.stats["conflicts"] += 1
        if local_meta:
            result = self._upload_local_file(
                local_meta.relative_path,
                server_user_file_id=last_file_id,
            )
            if result["status"] == "uploaded":
                context.stats["uploaded"] += 1
                logger.info(f"Конфликт разрешён в пользу ПК: {local_meta.relative_path}")
            elif result["status"] == "failed":
                logger.warning(
                    f"Ошибка разрешения конфликта для {local_meta.relative_path}: {result.get('message')}",
                )
            return

        if last_file_id and self.api_client.delete_desktop_file(last_file_id):
            context.stats["deleted_remote"] += 1
            logger.info(f"Конфликт удаления разрешён в пользу ПК: {last_path}")

    def _upload_new_local_files(self, context: RestartSyncContext) -> None:
        for relative_path, local_meta in context.local_files.items():
            if relative_path in context.last_paths or relative_path in context.processed_local_paths:
                continue
            result = self._upload_local_file(relative_path)
            if result["status"] == "uploaded":
                context.stats["uploaded"] += 1
                logger.info(f"Новый локальный файл загружен на сервер: {relative_path}")
            elif result["status"] == "failed":
                logger.warning(f"Ошибка загрузки нового локального файла {relative_path}: {result.get('message')}")

    def _apply_new_remote_files(self, context: RestartSyncContext) -> None:
        for relative_path, remote_meta in context.remote_files.items():
            if relative_path in context.last_paths or relative_path in context.processed_remote_paths:
                continue
            action = self._apply_remote_file_state(remote_meta)
            if action in {"downloaded", "moved"}:
                context.stats["downloaded"] += 1
            if action != "failed":
                logger.info(f"Новый серверный файл применён локально: {relative_path}")
            else:
                logger.warning(f"Ошибка загрузки нового серверного файла: {relative_path}")

    def _apply_removed_directory_changes(self, context: RestartSyncContext) -> None:
        for namespace_path in sorted(context.removed_remote_dirs, key=lambda path: path.count("/"), reverse=True):
            if self._delete_local_namespace_if_empty(namespace_path):
                context.stats["deleted_local_dirs"] += 1
                logger.info(f"Удалена локальная пустая папка по серверной структуре: {namespace_path}")

        context.stats["deleted_remote"] += self._delete_remote_files_under_prefixes(
            context.removed_local_dirs,
            context.remote_files,
        )
        context.stats["deleted_remote_dirs"] = self._delete_remote_namespaces(
            context.removed_local_dirs,
            context.remote_structure,
        )
