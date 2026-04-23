"""Типы и enum для синхронизации watcher."""

import enum
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Protocol, Set

from app.core.api_client import SyncAPIClient
from app.core.contracts import IndexedFileMeta, LocalFileMeta, NamespaceStructureItem, SyncMismatchReport
from app.core.db import SettingsDB
from app.core.uploader import FileUploader


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


class InitialSyncFacade(Protocol):
    api_client: SyncAPIClient
    uploader: FileUploader
    local_folder: Path

    def get_local_structure(self) -> List[NamespaceStructureItem]: ...
    def get_local_file_index(self) -> Dict[str, LocalFileMeta]: ...
    def empty_local_folder(self) -> None: ...
    def create_local_folder_if_not_exists(self, relative_dir: str) -> bool: ...
    def download_remote_file(self, relative_path: str, remote_meta: IndexedFileMeta) -> bool: ...
    def delete_remote_namespaces(
        self,
        namespace_paths: Set[str],
        remote_structure: List[NamespaceStructureItem],
    ) -> int: ...
    def create_remote_namespace_tree(
        self,
        namespace_paths: Set[str],
        remote_structure: List[NamespaceStructureItem],
    ) -> int: ...
    def refresh_last_sync_snapshot_if_synced(self) -> Optional[SyncMismatchReport]: ...


class RestartSyncFacade(Protocol):
    api_client: SyncAPIClient
    db: SettingsDB

    def get_local_structure(self) -> List[NamespaceStructureItem]: ...
    def get_local_file_index(self) -> Dict[str, LocalFileMeta]: ...
    def create_local_folder_if_not_exists(self, relative_dir: str) -> bool: ...
    def create_remote_namespace_tree(
        self,
        namespace_paths: Set[str],
        remote_structure: List[NamespaceStructureItem],
    ) -> int: ...
    def upload_local_file(
        self,
        relative_path: str,
        server_user_file_id: Optional[int] = None,
    ) -> Dict[str, str]: ...
    def delete_local_file(self, relative_path: str) -> bool: ...
    def apply_remote_file_state(
        self,
        remote_meta: IndexedFileMeta,
        previous_relative_path: Optional[str] = None,
    ) -> str: ...
    def delete_local_namespace_if_empty(self, relative_dir: str) -> bool: ...
    def delete_remote_files_under_prefixes(
        self,
        namespace_paths: Set[str],
        remote_files: Dict[str, IndexedFileMeta],
    ) -> int: ...
    def delete_remote_namespaces(
        self,
        namespace_paths: Set[str],
        remote_structure: List[NamespaceStructureItem],
    ) -> int: ...
    def refresh_last_sync_snapshot_if_synced(self) -> Optional[SyncMismatchReport]: ...
