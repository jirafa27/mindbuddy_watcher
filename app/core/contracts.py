"""Контракты синхронизации между watcher, backend и RN."""

import enum
from dataclasses import dataclass, field
from typing import Any, Dict, Optional, List


class SyncCommandAckStatus(enum.StrEnum):
    ACKED = "acked"
    FAILED = "failed"
    PENDING = "pending"


class SyncCommandType(enum.StrEnum):
    UPSERT_FILE = "upsert_file"
    MOVE_FILE = "move_file"
    RENAME_FILE = "rename_file"
    TRASH_FILE = "trash_file"
    DELETE_NAMESPACE = "delete_namespace"
    DELETE_FILE = "delete_file"
    MOVE_NAMESPACE = "move_namespace"
    RENAME_NAMESPACE = "rename_namespace"
    TRASH_NAMESPACE = "trash_namespace"


class SyncMismatchItemKind(enum.StrEnum):
    FILE = "file"
    FOLDER = "folder"


class SyncMismatchType(enum.StrEnum):
    LOCAL_ONLY = "local_only"
    REMOTE_ONLY = "remote_only"
    CONTENT_DIFF = "content_diff"
    PATH_CHANGED = "path_changed"


class SyncResolutionAction(enum.StrEnum):
    USE_LOCAL = "use_local"
    USE_REMOTE = "use_remote"
    SKIP = "skip"


@dataclass(frozen=True)
class SyncMismatchItem:
    path: str
    item_kind: SyncMismatchItemKind
    mismatch_type: SyncMismatchType
    local_hash: Optional[str] = None
    remote_hash: Optional[str] = None
    local_path: Optional[str] = None
    remote_path: Optional[str] = None


@dataclass(frozen=True)
class SyncMismatchResolution:
    path: str
    item_kind: SyncMismatchItemKind
    mismatch_type: SyncMismatchType
    action: SyncResolutionAction
    local_path: Optional[str] = None
    remote_path: Optional[str] = None


@dataclass
class SyncMismatchReport:
    items: List[SyncMismatchItem] = field(default_factory=list)

    def is_empty(self) -> bool:
        return not self.items


@dataclass
class FileInfo:
    """Метаданные файла."""

    id: Optional[int]
    filename: str
    file_size: int
    updated_at: str
    content_hash: str

    @classmethod
    def from_dict(cls, payload: Dict[str, Any]) -> "FileInfo":
        file_id = payload.get("id")
        content_hash = payload.get("content_hash")
        if content_hash is None:
            raise ValueError("content_hash is required for FileInfo")
        return cls(
            id=int(file_id) if file_id is not None else None,
            filename=payload.get("filename"),
            file_size=payload.get("file_size"),
            updated_at=payload.get("updated_at"),
            content_hash=content_hash,
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "filename": self.filename,
            "file_size": self.file_size,
            "updated_at": self.updated_at,
            "content_hash": self.content_hash,
        }


@dataclass
class LocalFileMeta:
    """Локальные метаданные файла для upload и file_state."""

    relative_path: str
    filename: str
    file_size: int
    content_hash: str
    desktop_updated_at: str
    last_seen_mtime: float


@dataclass
class IndexedFileMeta:
    """Файл с сервера с уже вычисленным `relative_path` для sync-алгоритма."""

    relative_path: str
    filename: str
    file_size: int
    content_hash: str
    updated_at: str
    file_id: Optional[int]
    namespace_id: Optional[int]


@dataclass
class NamespaceStructureItem:
    """Namespace для синхронизации."""

    id: Optional[int]
    name: str
    parent_id: Optional[int]
    kind: str
    files: List[FileInfo] = field(default_factory=list)

    @classmethod
    def from_dict(cls, payload: Dict[str, Any]) -> "NamespaceStructureItem":
        namespace_id = payload.get("id")
        name = payload.get("name")
        parent_id = payload.get("parent_id")
        kind = payload.get("kind")
        files = payload.get("files", [])
        return cls(
            id=int(namespace_id) if namespace_id is not None else None,
            name=name,
            parent_id=int(parent_id) if parent_id is not None else None,
            kind=kind,
            files=[FileInfo.from_dict(file) for file in files],
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "parent_id": self.parent_id,
            "kind": self.kind,
            "files": [file_info.to_dict() for file_info in self.files],
        }


@dataclass
class SyncCommand:
    """Команда для применения серверных изменений локально на ПК."""

    command_id: str
    command_type: Optional[SyncCommandType]
    relative_path: str
    filename: Optional[str] = None
    new_filename: Optional[str] = None
    file_id: Optional[int] = None
    namespace_id: Optional[int] = None
    target_namespace_id: Optional[int] = None
    target_parent_id: Optional[int] = None
    content_hash: Optional[str] = None
    extra: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, payload: Dict[str, Any]) -> "SyncCommand":
        command_id = payload.get("id") or payload.get("command_id")
        raw_command_type = payload.get("command_type") or ""
        try:
            command_type = SyncCommandType(raw_command_type) if raw_command_type else None
        except ValueError:
            command_type = None
        nested: Dict[str, Any] = payload.get("payload") or {}
        relative_path = (
            nested.get("relative_path")
            or payload.get("relative_path")
            or payload.get("path")
            or ""
        )

        raw_file_id = nested.get("user_file_id") or nested.get("file_id") or payload.get("file_id")
        raw_namespace_id = nested.get("namespace_id") or payload.get("namespace_id")
        raw_target_namespace_id = nested.get("target_namespace_id") or payload.get("target_namespace_id")
        raw_target_parent_id = nested.get("target_parent_id") or payload.get("target_parent_id")

        extra = {
            key: value
            for key, value in payload.items()
            if key
            not in {
                "id",
                "command_id",
                "type",
                "command_type",
                "relative_path",
                "path",
                "payload",
                "content_hash",
                "namespace_id",
                "target_namespace_id",
                "target_parent_id",
                "new_filename",
                "new_name",
            }
        }
        if command_type is None and raw_command_type:
            extra["unsupported_command_type"] = raw_command_type

        return cls(
            command_id=str(command_id),
            command_type=command_type,
            relative_path=relative_path,
            filename=nested.get("filename") or payload.get("filename"),
            new_filename=(
                nested.get("new_filename")
                or payload.get("new_filename")
                or nested.get("new_name")
                or payload.get("new_name")
            ),
            file_id=int(raw_file_id) if raw_file_id is not None else None,
            namespace_id=int(raw_namespace_id) if raw_namespace_id is not None else None,
            target_namespace_id=(
                int(raw_target_namespace_id)
                if raw_target_namespace_id is not None
                else None
            ),
            target_parent_id=(
                int(raw_target_parent_id)
                if raw_target_parent_id is not None
                else None
            ),
            content_hash=nested.get("content_hash") or payload.get("content_hash"),
            extra=extra,
        )


@dataclass
class UploadResult:
    """Результат загрузки desktop-версии файла."""

    ok: bool
    status: str
    content_hash: Optional[str] = None
    updated_at: Optional[str] = None
    file_id: Optional[int] = None
    message: Optional[str] = None
    raw: Dict[str, Any] = field(default_factory=dict)
