"""Контракты синхронизации между watcher, backend и RN."""

from dataclasses import dataclass, field
from typing import Any, Dict, Optional


def _strip_vault_prefix(path: str) -> str:
    if not path:
        return ""
    normalized = path.replace("\\", "/")
    if normalized.startswith("Trash/"):
        return normalized
    parts = normalized.split("/", 1)
    return parts[1] if len(parts) > 1 else normalized


@dataclass
class SyncFileMeta:
    """Метаданные файла, которые backend отдает клиентам."""

    relative_path: str
    content_hash: Optional[str] = None
    updated_at: Optional[str] = None
    desktop_updated_at: Optional[str] = None
    app_updated_at: Optional[str] = None
    download_url: Optional[str] = None
    file_id: Optional[int] = None
    namespace_id: Optional[int] = None
    extra: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, payload: Dict[str, Any]) -> "SyncFileMeta":
        # relative_path: прямое поле, "path", или vault_relative_path с обрезкой имени vault
        relative_path = (
            payload.get("relative_path")
            or payload.get("path")
            or _strip_vault_prefix(payload.get("vault_relative_path") or "")
            or ""
        )
        # file_id: бэкенд может отдавать user_file_id, file_id или id
        raw_file_id = (
            payload.get("user_file_id")
            or payload.get("file_id")
            or payload.get("id")
        )
        raw_namespace_id = payload.get("namespace_id")
        return cls(
            relative_path=relative_path,
            content_hash=payload.get("content_hash"),
            updated_at=payload.get("updated_at"),
            desktop_updated_at=payload.get("desktop_updated_at"),
            app_updated_at=payload.get("app_updated_at"),
            download_url=payload.get("download_url"),
            file_id=int(raw_file_id) if raw_file_id is not None else None,
            namespace_id=int(raw_namespace_id) if raw_namespace_id is not None else None,
            extra={
                key: value
                for key, value in payload.items()
                if key
                not in {
                    "relative_path",
                    "path",
                    "vault_relative_path",
                    "content_hash",
                    "updated_at",
                    "desktop_updated_at",
                    "app_updated_at",
                    "download_url",
                    "file_id",
                    "user_file_id",
                    "id",
                    "namespace_id",
                }
            },
        )


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
            relative_path = payload.get("relative_path") or payload.get("path") or ""

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
