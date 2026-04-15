"""Контракты синхронизации между watcher, backend и RN."""

from dataclasses import dataclass, field
from typing import Any, Dict, Optional, List


def _strip_vault_prefix(path: str) -> str:
    if not path:
        return ""
    normalized = path.replace("\\", "/")
    if normalized.startswith("Trash/"):
        return normalized
    parts = normalized.split("/", 1)
    return parts[1] if len(parts) > 1 else normalized


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
    command_type: str
    relative_path: str
    old_relative_path: Optional[str] = None
    new_relative_path: Optional[str] = None
    filename: Optional[str] = None
    file_id: Optional[int] = None
    namespace_id: Optional[int] = None
    target_type: Optional[str] = None
    content_hash: Optional[str] = None
    content: Optional[str] = None
    content_base64: Optional[str] = None
    download_url: Optional[str] = None
    extra: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, payload: Dict[str, Any]) -> "SyncCommand":
        command_id = payload.get("id") or payload.get("command_id")

        # Backend вкладывает данные файла в поле "payload"
        nested: Dict[str, Any] = payload.get("payload") or {}

        # relative_path: из vault_relative_path без первого компонента (имени vault)
        vault_relative = nested.get("vault_relative_path") or ""
        if vault_relative:
            relative_path = _strip_vault_prefix(vault_relative)
        else:
            relative_path = (
                nested.get("relative_path")
                or payload.get("relative_path")
                or payload.get("path")
                or ""
            )

        old_relative_path = _strip_vault_prefix(nested.get("old_vault_relative_path") or "")
        new_relative_path = _strip_vault_prefix(nested.get("new_vault_relative_path") or "")
        raw_file_id = nested.get("user_file_id") or nested.get("file_id") or payload.get("file_id")
        raw_namespace_id = nested.get("namespace_id") or payload.get("namespace_id")

        return cls(
            command_id=str(command_id),
            command_type=payload.get("command_type") or payload.get("type") or "upsert",
            relative_path=relative_path,
            old_relative_path=old_relative_path or None,
            new_relative_path=new_relative_path or None,
            filename=nested.get("filename") or payload.get("filename"),
            file_id=int(raw_file_id) if raw_file_id is not None else None,
            namespace_id=int(raw_namespace_id) if raw_namespace_id is not None else None,
            target_type=nested.get("target_type") or payload.get("target_type"),
            content_hash=nested.get("content_hash") or payload.get("content_hash"),
            content=nested.get("content") or payload.get("content"),
            content_base64=(
                nested.get("content_base64") or nested.get("data")
                or payload.get("content_base64") or payload.get("data")
            ),
            download_url=(
                nested.get("download_url") or nested.get("url")
                or payload.get("download_url") or payload.get("url")
            ),
            extra={
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
                    "content",
                    "content_base64",
                    "data",
                    "download_url",
                    "url",
                    "namespace_id",
                }
            },
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
