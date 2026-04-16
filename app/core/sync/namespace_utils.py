"""Чистые helper-функции для namespace-структур синхронизации."""

from typing import Dict, List, Set

from app.core.contracts import IndexedFileMeta, LocalFileMeta, NamespaceStructureItem
from app.core.namespace_constants import (
    INBOX_DIRNAME,
    INBOX_KIND,
    TRASH_DIRNAME,
    TRASH_KIND,
    VAULT_ROOT_KIND,
)


def build_path_by_namespace_id(namespaces: List[NamespaceStructureItem]) -> Dict[int, str]:
    """Строит индекс `namespace_id -> relative_path`."""
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
        if namespace.kind == VAULT_ROOT_KIND:
            path = ""
        elif namespace.kind == INBOX_KIND:
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


def flatten_namespace_list(namespaces: List[NamespaceStructureItem]) -> Dict[str, IndexedFileMeta]:
    """Строит словарь `relative_path -> IndexedFileMeta`."""
    path_by_namespace_id = build_path_by_namespace_id(namespaces)
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


def build_comparison_dict(file_index: Dict[str, LocalFileMeta | IndexedFileMeta]) -> Dict[str, str]:
    """Нормализует индекс файлов до `relative_path -> content_hash`."""
    return {
        relative_path: file_meta.content_hash
        for relative_path, file_meta in file_index.items()
    }


def get_namespace_paths(namespaces: List[NamespaceStructureItem]) -> Set[str]:
    """Возвращает набор namespace-путей."""
    path_by_namespace_id = build_path_by_namespace_id(namespaces)
    return {
        path_by_namespace_id[namespace.id]
        for namespace in namespaces
        if namespace.id is not None and path_by_namespace_id.get(namespace.id, "")
    }


def build_namespace_path_index(namespaces: List[NamespaceStructureItem]) -> Dict[str, NamespaceStructureItem]:
    """Строит индекс `namespace_path -> NamespaceStructureItem`."""
    path_by_namespace_id = build_path_by_namespace_id(namespaces)
    result: Dict[str, NamespaceStructureItem] = {}
    for namespace in namespaces:
        if namespace.id is None:
            continue
        namespace_path = path_by_namespace_id.get(namespace.id, "")
        if namespace_path:
            result[namespace_path] = namespace
    return result
