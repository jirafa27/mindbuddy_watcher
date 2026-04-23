"""Фасад синхронизации локальной папки с backend."""

from __future__ import annotations

import logging
import shutil
import time
from pathlib import Path
from typing import Dict, List, Optional, Set

from app.core.api_client import SyncAPIClient
from app.core.contracts import (
    FileInfo,
    IndexedFileMeta,
    LocalFileMeta,
    NamespaceStructureItem,
    SyncMismatchItem,
    SyncMismatchItemKind,
    SyncMismatchReport,
    SyncMismatchResolution,
    SyncMismatchType,
    SyncResolutionAction,
)
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
    PROTECTED_NAMESPACE_NAMES,
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

    @staticmethod
    def _has_prefix_path(path: str, prefix: str) -> bool:
        return path == prefix or path.startswith(f"{prefix}/")

    def _is_protected_namespace_path(self, relative_path: str) -> bool:
        return relative_path in PROTECTED_NAMESPACE_NAMES

    def _keep_top_level_paths(self, paths: List[str]) -> List[str]:
        top_level_paths: List[str] = []
        for path in sorted(paths, key=lambda item: (item.count("/"), item)):
            if any(self._has_prefix_path(path, existing_path) for existing_path in top_level_paths):
                continue
            top_level_paths.append(path)
        return top_level_paths

    def _match_path_changed_file_mismatches(
        self,
        local_only_files: List[str],
        remote_only_files: List[str],
        local_files: Dict[str, LocalFileMeta],
        remote_files: Dict[str, IndexedFileMeta],
    ) -> tuple[List[SyncMismatchItem], List[str], List[str]]:
        path_changed_items: List[SyncMismatchItem] = []
        matched_local_paths: Set[str] = set()
        remaining_remote_paths: Set[str] = set(remote_only_files)
        local_file_states = {
            state["relative_path"]: state
            for state in self.db.get_all_file_states()
        }
        remote_only_by_file_id = {
            remote_files[path].file_id: path
            for path in remote_only_files
            if remote_files[path].file_id is not None
        }

        def register_match(local_path: str, remote_path: str) -> None:
            local_meta = local_files[local_path]
            remote_meta = remote_files[remote_path]
            path_changed_items.append(
                SyncMismatchItem(
                    path=local_path,
                    item_kind=SyncMismatchItemKind.FILE,
                    mismatch_type=SyncMismatchType.PATH_CHANGED,
                    local_hash=local_meta.content_hash,
                    remote_hash=remote_meta.content_hash,
                    local_path=local_path,
                    remote_path=remote_path,
                )
            )
            matched_local_paths.add(local_path)
            remaining_remote_paths.discard(remote_path)

        for local_path in local_only_files:
            local_state = local_file_states.get(local_path)
            local_file_id = local_state.get("server_user_file_id") if local_state else None
            if local_file_id is None:
                continue
            remote_path = remote_only_by_file_id.get(local_file_id)
            if remote_path and remote_path in remaining_remote_paths:
                register_match(local_path, remote_path)

        for local_path in local_only_files:
            if local_path in matched_local_paths:
                continue
            local_meta = local_files.get(local_path)
            if local_meta is None:
                continue
            local_parent = Path(local_path).parent.as_posix()
            candidates = [
                remote_path
                for remote_path in remaining_remote_paths
                if remote_files[remote_path].content_hash == local_meta.content_hash
                and Path(remote_path).parent.as_posix() == local_parent
            ]
            if len(candidates) == 1:
                register_match(local_path, candidates[0])

        remaining_local_only = [
            path for path in local_only_files
            if path not in matched_local_paths
        ]
        remaining_remote_only = [
            path for path in remote_only_files
            if path in remaining_remote_paths
        ]
        return path_changed_items, remaining_local_only, remaining_remote_only

    def _match_path_changed_folder_mismatches(
        self,
        local_only_dirs: List[str],
        remote_only_dirs: List[str],
        remote_structure: List[NamespaceStructureItem],
    ) -> tuple[List[SyncMismatchItem], List[str], List[str]]:
        path_changed_items: List[SyncMismatchItem] = []
        matched_local_paths: Set[str] = set()
        top_local_only_dirs = self._keep_top_level_paths(local_only_dirs)
        top_remote_only_dirs = self._keep_top_level_paths(remote_only_dirs)
        remaining_remote_paths: Set[str] = set(top_remote_only_dirs)
        local_folder_states = {
            state["relative_path"]: state
            for state in self.db.get_all_folder_states()
        }
        remote_namespace_by_path = build_namespace_path_index(remote_structure)
        remote_top_by_namespace_id = {
            namespace.id: path
            for path, namespace in remote_namespace_by_path.items()
            if path in remaining_remote_paths and namespace.id is not None
        }

        for local_path in top_local_only_dirs:
            local_state = local_folder_states.get(local_path)
            local_namespace_id = local_state.get("namespace_id") if local_state else None
            if local_namespace_id is None:
                continue
            remote_path = remote_top_by_namespace_id.get(local_namespace_id)
            if remote_path and remote_path in remaining_remote_paths:
                path_changed_items.append(
                    SyncMismatchItem(
                        path=local_path,
                        item_kind=SyncMismatchItemKind.FOLDER,
                        mismatch_type=SyncMismatchType.PATH_CHANGED,
                        local_path=local_path,
                        remote_path=remote_path,
                    )
                )
                matched_local_paths.add(local_path)
                remaining_remote_paths.discard(remote_path)

        remaining_local_only = [
            path for path in top_local_only_dirs
            if path not in matched_local_paths
        ]
        remaining_remote_only = [
            path for path in top_remote_only_dirs
            if path in remaining_remote_paths
        ]
        return path_changed_items, remaining_local_only, remaining_remote_only

    def _filter_file_path_changes_covered_by_folder_changes(
        self,
        path_changed_files: List[SyncMismatchItem],
        path_changed_dirs: List[SyncMismatchItem],
    ) -> List[SyncMismatchItem]:
        def relative_suffix(path: str, prefix: str) -> str:
            if path == prefix:
                return ""
            return path[len(prefix) + 1:]

        filtered_items: List[SyncMismatchItem] = []
        for file_item in path_changed_files:
            local_path = file_item.local_path or file_item.path
            remote_path = file_item.remote_path or file_item.path
            covered_by_folder_change = False

            for folder_item in path_changed_dirs:
                local_dir_path = folder_item.local_path or folder_item.path
                remote_dir_path = folder_item.remote_path or folder_item.path
                if not local_dir_path or not remote_dir_path:
                    continue
                if not self._has_prefix_path(local_path, local_dir_path):
                    continue
                if not self._has_prefix_path(remote_path, remote_dir_path):
                    continue
                if relative_suffix(local_path, local_dir_path) == relative_suffix(remote_path, remote_dir_path):
                    covered_by_folder_change = True
                    break

            if not covered_by_folder_change:
                filtered_items.append(file_item)

        return filtered_items

    def build_sync_mismatch_report(
        self,
        remote_structure: Optional[List[NamespaceStructureItem]] = None,
    ) -> SyncMismatchReport:
        if remote_structure is None:
            remote_structure = self.api_client.get_files_server_structure()

        remote_files_dict = build_comparison_dict(flatten_namespace_list(remote_structure))
        local_files_dict = build_comparison_dict(self.get_local_file_index())
        dir_local_paths = get_namespace_paths(self.get_local_structure())
        dir_remote_paths = get_namespace_paths(remote_structure)

        local_only_files = sorted(
            path
            for path in set(local_files_dict) - set(remote_files_dict)
            if not self._is_protected_namespace_path(path)
        )
        remote_only_files = sorted(
            path
            for path in set(remote_files_dict) - set(local_files_dict)
            if not self._is_protected_namespace_path(path)
        )
        path_changed_files, local_only_files, remote_only_files = self._match_path_changed_file_mismatches(
            local_only_files,
            remote_only_files,
            self.get_local_file_index(),
            flatten_namespace_list(remote_structure),
        )
        changed_files = sorted(
            path
            for path in set(local_files_dict) & set(remote_files_dict)
            if local_files_dict[path] != remote_files_dict[path]
            and not self._is_protected_namespace_path(path)
        )
        local_only_dirs = sorted(
            path
            for path in dir_local_paths - dir_remote_paths
            if path and not self._is_protected_namespace_path(path)
        )
        remote_only_dirs = sorted(
            path
            for path in dir_remote_paths - dir_local_paths
            if path and not self._is_protected_namespace_path(path)
        )
        path_changed_dirs, local_only_dirs, remote_only_dirs = self._match_path_changed_folder_mismatches(
            local_only_dirs,
            remote_only_dirs,
            remote_structure,
        )
        path_changed_files = self._filter_file_path_changes_covered_by_folder_changes(
            path_changed_files,
            path_changed_dirs,
        )

        mismatched_file_paths = set(local_only_files) | set(remote_only_files) | set(changed_files)
        for item in path_changed_files:
            if item.local_path:
                mismatched_file_paths.add(item.local_path)
            if item.remote_path:
                mismatched_file_paths.add(item.remote_path)

        items: List[SyncMismatchItem] = []
        for item in path_changed_files:
            items.append(item)
        for path in local_only_files:
            items.append(SyncMismatchItem(
                path=path,
                item_kind=SyncMismatchItemKind.FILE,
                mismatch_type=SyncMismatchType.LOCAL_ONLY,
                local_hash=local_files_dict.get(path),
                local_path=path,
            ))
        for path in remote_only_files:
            items.append(SyncMismatchItem(
                path=path,
                item_kind=SyncMismatchItemKind.FILE,
                mismatch_type=SyncMismatchType.REMOTE_ONLY,
                remote_hash=remote_files_dict.get(path),
                remote_path=path,
            ))
        for path in changed_files:
            items.append(SyncMismatchItem(
                path=path,
                item_kind=SyncMismatchItemKind.FILE,
                mismatch_type=SyncMismatchType.CONTENT_DIFF,
                local_hash=local_files_dict.get(path),
                remote_hash=remote_files_dict.get(path),
                local_path=path,
                remote_path=path,
            ))

        for item in path_changed_dirs:
            items.append(item)

        folder_candidates = [
            (SyncMismatchType.LOCAL_ONLY, path)
            for path in local_only_dirs
        ] + [
            (SyncMismatchType.REMOTE_ONLY, path)
            for path in remote_only_dirs
        ]
        for mismatch_type, path in sorted(folder_candidates, key=lambda item: item[1]):
            if any(self._has_prefix_path(file_path, path) for file_path in mismatched_file_paths):
                continue
            items.append(SyncMismatchItem(
                path=path,
                item_kind=SyncMismatchItemKind.FOLDER,
                mismatch_type=mismatch_type,
            ))

        return SyncMismatchReport(
            items=sorted(items, key=lambda item: (item.item_kind.value, item.path)),
        )

    @staticmethod
    def _log_sync_mismatch_report(report: SyncMismatchReport) -> None:
        path_changed_files = [
            (item.local_path or item.path, item.remote_path or item.path)
            for item in report.items
            if item.item_kind == SyncMismatchItemKind.FILE
            and item.mismatch_type == SyncMismatchType.PATH_CHANGED
        ]
        path_changed_dirs = [
            (item.local_path or item.path, item.remote_path or item.path)
            for item in report.items
            if item.item_kind == SyncMismatchItemKind.FOLDER
            and item.mismatch_type == SyncMismatchType.PATH_CHANGED
        ]
        local_only_files = [item.path for item in report.items if item.item_kind == SyncMismatchItemKind.FILE and item.mismatch_type == SyncMismatchType.LOCAL_ONLY]
        remote_only_files = [item.path for item in report.items if item.item_kind == SyncMismatchItemKind.FILE and item.mismatch_type == SyncMismatchType.REMOTE_ONLY]
        changed_files = [item.path for item in report.items if item.item_kind == SyncMismatchItemKind.FILE and item.mismatch_type == SyncMismatchType.CONTENT_DIFF]
        local_only_dirs = [item.path for item in report.items if item.item_kind == SyncMismatchItemKind.FOLDER and item.mismatch_type == SyncMismatchType.LOCAL_ONLY]
        remote_only_dirs = [item.path for item in report.items if item.item_kind == SyncMismatchItemKind.FOLDER and item.mismatch_type == SyncMismatchType.REMOTE_ONLY]

        if path_changed_files:
            logger.warning("Файлы с разным путём/именем: %s", path_changed_files)
        if path_changed_dirs:
            logger.warning("Папки с разным путём/именем: %s", path_changed_dirs)
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

    def apply_sync_mismatch_resolutions(
        self,
        resolutions: List[SyncMismatchResolution],
    ) -> None:
        remote_structure = self.api_client.get_files_server_structure()
        remote_files = flatten_namespace_list(remote_structure)

        file_resolutions = [
            resolution
            for resolution in resolutions
            if resolution.item_kind == SyncMismatchItemKind.FILE
        ]
        folder_resolutions = [
            resolution
            for resolution in resolutions
            if resolution.item_kind == SyncMismatchItemKind.FOLDER
        ]

        for resolution in file_resolutions:
            local_path = resolution.local_path or resolution.path
            remote_path = resolution.remote_path or resolution.path
            remote_meta = remote_files.get(remote_path)
            if resolution.action == SyncResolutionAction.SKIP:
                continue
            if resolution.mismatch_type == SyncMismatchType.PATH_CHANGED:
                if resolution.action == SyncResolutionAction.USE_LOCAL:
                    if not local_path:
                        logger.warning("Не указан локальный путь для path_changed")
                        continue
                    if remote_meta and remote_meta.file_id:
                        local_parent = Path(local_path).parent.as_posix()
                        remote_parent = Path(remote_path).parent.as_posix()
                        if local_parent == remote_parent:
                            rename_ok = self.api_client.rename_server_file(
                                remote_meta.file_id,
                                Path(local_path).name,
                            )
                            if not rename_ok:
                                logger.warning(
                                    "Не удалось переименовать серверную версию файла %s -> %s",
                                    remote_path,
                                    local_path,
                                )
                                continue
                            if (
                                resolution.local_hash
                                and resolution.remote_hash
                                and resolution.local_hash != resolution.remote_hash
                            ):
                                result = self.upload_local_file(
                                    local_path,
                                    server_user_file_id=remote_meta.file_id,
                                )
                                if result.get("status") not in {"uploaded", "skipped"}:
                                    logger.warning(
                                        "Не удалось обновить серверное содержимое файла %s: %s",
                                        local_path,
                                        result.get("message"),
                                    )
                                    continue
                            logger.info(
                                "Расхождение по пути файла разрешено в пользу ПК: %s -> %s",
                                remote_path,
                                local_path,
                            )
                            continue

                        if not self.api_client.delete_server_file(remote_meta.file_id):
                            logger.warning("Не удалось удалить серверную версию файла %s", remote_path)
                            continue
                    result = self.upload_local_file(local_path)
                    if result.get("status") in {"uploaded", "skipped"}:
                        logger.info(
                            "Расхождение по пути файла разрешено в пользу ПК: %s -> %s",
                            remote_path,
                            local_path,
                        )
                    else:
                        logger.warning(
                            "Не удалось применить локальную версию файла %s: %s",
                            local_path,
                            result.get("message"),
                        )
                    continue

                if remote_meta is None:
                    logger.warning("Не удалось найти серверную версию файла для %s", remote_path)
                    continue
                if local_path and local_path != remote_path:
                    if not self.delete_local_file(local_path):
                        logger.warning(
                            "Не удалось удалить локальную версию файла перед заменой: %s",
                            local_path,
                        )
                        continue
                action = self.apply_remote_file_state(remote_meta)
                if action != "failed":
                    logger.info(
                        "Расхождение по пути файла разрешено в пользу сервера: %s -> %s",
                        local_path,
                        remote_path,
                    )
                else:
                    logger.warning("Не удалось применить серверную версию файла %s", remote_path)
                continue

            if resolution.action == SyncResolutionAction.USE_LOCAL:
                if resolution.mismatch_type == SyncMismatchType.REMOTE_ONLY:
                    if remote_meta and remote_meta.file_id and self.api_client.delete_server_file(remote_meta.file_id):
                        remote_files.pop(remote_path, None)
                        logger.info("Расхождение по файлу разрешено в пользу ПК: %s", remote_path)
                    else:
                        logger.warning("Не удалось удалить серверную версию файла %s", remote_path)
                    continue

                self.db.delete_file_state(local_path)
                if remote_meta is None:
                    result = self.upload_local_file(local_path)
                else:
                    result = self.upload_local_file(
                        local_path,
                        server_user_file_id=remote_meta.file_id,
                    )
                if result.get("status") == "uploaded":
                    logger.info("Расхождение по файлу разрешено в пользу ПК: %s", local_path)
                else:
                    logger.warning("Не удалось применить локальную версию файла %s: %s", local_path, result.get("message"))
                continue

            if resolution.mismatch_type == SyncMismatchType.LOCAL_ONLY:
                if self.delete_local_file(local_path):
                    logger.info("Расхождение по файлу разрешено в пользу сервера: %s", local_path)
                else:
                    logger.warning("Не удалось удалить локальную версию файла %s", local_path)
                continue

            if resolution.action == SyncResolutionAction.USE_REMOTE:
                action = self.apply_remote_file_state(remote_meta)
                if action != "failed":
                    logger.info("Расхождение по файлу разрешено в пользу сервера: %s", remote_path)
                else:
                    logger.warning("Не удалось применить серверную версию файла %s", remote_path)
                continue

        for resolution in folder_resolutions:
            if resolution.action == SyncResolutionAction.SKIP:
                continue
            local_path = resolution.local_path or resolution.path
            remote_path = resolution.remote_path or resolution.path
            local_namespace_paths = get_namespace_paths(self.get_local_structure())
            remote_namespace_paths = get_namespace_paths(remote_structure)
            local_subtree_paths = {
                path
                for path in local_namespace_paths
                if self._has_prefix_path(path, local_path)
            }
            remote_subtree_paths = {
                path
                for path in remote_namespace_paths
                if self._has_prefix_path(path, remote_path)
            }
            if resolution.mismatch_type == SyncMismatchType.PATH_CHANGED:
                remote_namespace = build_namespace_path_index(remote_structure).get(remote_path)
                if resolution.action == SyncResolutionAction.USE_REMOTE:
                    if self._move_local_namespace(local_path, remote_path):
                        logger.info(
                            "Расхождение по пути папки разрешено в пользу сервера: %s -> %s",
                            local_path,
                            remote_path,
                        )
                    else:
                        logger.warning("Не удалось применить серверную версию папки %s", remote_path)
                    continue

                if remote_namespace is None or remote_namespace.id is None:
                    logger.warning("Не удалось найти серверный namespace для %s", remote_path)
                    continue
                local_parent_path = Path(local_path).parent.as_posix()
                remote_parent_path = Path(remote_path).parent.as_posix()
                sync_ok = True
                if local_parent_path != remote_parent_path:
                    remote_namespace_by_path = build_namespace_path_index(remote_structure)
                    if local_parent_path in {".", ""}:
                        target_parent = next(
                            (namespace for namespace in remote_structure if namespace.kind == VAULT_ROOT_KIND and namespace.id is not None),
                            None,
                        )
                    else:
                        target_parent = remote_namespace_by_path.get(local_parent_path)
                    if not (target_parent and target_parent.id is not None and self.api_client.move_namespace(remote_namespace.id, target_parent.id)):
                        logger.warning(
                            "Не удалось применить локальное перемещение папки %s -> %s",
                            remote_path,
                            local_path,
                        )
                        sync_ok = False

                if sync_ok and Path(local_path).name != Path(remote_path).name:
                    if not self.api_client.rename_namespace(remote_namespace.id, Path(local_path).name):
                        logger.warning(
                            "Не удалось применить локальное переименование папки %s -> %s",
                            remote_path,
                            local_path,
                        )
                        sync_ok = False

                if sync_ok:
                    logger.info(
                        "Расхождение по пути папки разрешено в пользу ПК: %s -> %s",
                        remote_path,
                        local_path,
                    )
                continue

            if resolution.action == SyncResolutionAction.USE_LOCAL:
                if resolution.mismatch_type == SyncMismatchType.REMOTE_ONLY:
                    deleted = self.delete_remote_namespaces(remote_subtree_paths, remote_structure)
                    if deleted:
                        logger.info("Расхождение по папке разрешено в пользу ПК: %s", remote_path)
                    else:
                        logger.warning("Не удалось удалить серверную версию папки %s", remote_path)
                elif self.create_remote_namespace_tree(local_subtree_paths, remote_structure):
                    logger.info("Расхождение по папке разрешено в пользу ПК: %s", local_path)
                else:
                    logger.warning("Не удалось применить локальную версию папки %s", local_path)
                continue

            if resolution.action == SyncResolutionAction.USE_REMOTE:
                if resolution.mismatch_type == SyncMismatchType.REMOTE_ONLY:
                    created_any = False
                    for namespace_path in sorted(remote_subtree_paths, key=lambda path: (path.count("/"), path)):
                        created_any = self.create_local_folder_if_not_exists(namespace_path) or created_any
                    if created_any:
                        logger.info("Расхождение по папке разрешено в пользу сервера: %s", remote_path)
                    else:
                        logger.info("Папка уже существует локально после выбора серверной версии: %s", remote_path)
                else:
                    deleted_all = True
                    for namespace_path in sorted(local_subtree_paths, key=lambda path: (path.count("/"), path), reverse=True):
                        if not self.delete_local_namespace_if_empty(namespace_path):
                            deleted_all = False
                    if deleted_all:
                        logger.info("Расхождение по папке разрешено в пользу сервера: %s", local_path)
                    else:
                        logger.warning("Не удалось удалить локальную версию папки %s", local_path)
                        continue

        self.replace_folder_states_from_local()

    # Хелперы состояния папок
    def replace_folder_states_from_local(self) -> None:
        local_paths = get_namespace_paths(self.get_local_structure())
        existing_folder_states = {
            state["relative_path"]: state
            for state in self.db.get_all_folder_states()
        }
        folder_states: List[Dict[str, object]] = []
        for relative_path in sorted(local_paths, key=lambda path: (path.count("/"), path)):
            parent_path = Path(relative_path).parent.as_posix()
            existing_state = existing_folder_states.get(relative_path, {})
            folder_states.append({
                "relative_path": relative_path,
                "namespace_id": existing_state.get("namespace_id"),
                "parent_relative_path": None if parent_path in {".", ""} else parent_path,
                "kind": existing_state.get("kind") or self._folder_kind_from_path(relative_path),
                "is_applying_remote": existing_state.get("is_applying_remote", False),
                "last_command_id": existing_state.get("last_command_id"),
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
        vault_root_namespace = next(
            (
                namespace
                for namespace in remote_structure
                if namespace.kind == VAULT_ROOT_KIND and namespace.id is not None
            ),
            None,
        )

        for namespace_path in sorted(namespace_paths, key=lambda path: (path.count("/"), path)):
            if not namespace_path or namespace_path in {INBOX_DIRNAME, TRASH_DIRNAME} or namespace_path in namespace_by_path:
                continue

            parent_path = Path(namespace_path).parent.as_posix()
            if parent_path in {".", ""}:
                parent_namespace_id = vault_root_namespace.id if vault_root_namespace else None
            else:
                parent_namespace = namespace_by_path.get(parent_path)
                parent_namespace_id = parent_namespace.id if parent_namespace and parent_namespace.id is not None else None
            if parent_namespace_id is None:
                logger.warning("Не удалось определить parent namespace для %s", namespace_path)
                continue

            created_namespace = self.api_client.create_namespace(
                name=Path(namespace_path).name,
                parent_namespace_id=parent_namespace_id,
            )
            if created_namespace is None:
                logger.warning(f"Не удалось создать namespace на сервере: {namespace_path}")
                continue

            remote_structure.append(created_namespace)
            namespace_by_path[namespace_path] = created_namespace
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
            file_id = remote_meta.file_id
            if not file_id:
                logger.warning("Не удалось скачать файл без user_file_id: %s", relative_path)
                return False
            downloaded = self.api_client.download_file_by_id(file_id, target_path)
            if not downloaded:
                return False

            self._remember_parent_folders(relative_path, namespace_id=remote_meta.namespace_id)
            self.db.delete_file_states_by_server_user_file_id(
                file_id,
                keep_relative_path=relative_path,
            )
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

    def _move_local_namespace(self, old_relative_path: str, new_relative_path: str) -> bool:
        if old_relative_path == new_relative_path:
            return True

        old_path = self.local_folder / old_relative_path
        new_path = self.local_folder / new_relative_path
        if not old_path.exists() or not old_path.is_dir():
            return False
        if new_path.exists() and new_path != old_path:
            return False

        new_path.parent.mkdir(parents=True, exist_ok=True)
        old_path.rename(new_path)
        self.db.rename_folder_prefix(old_relative_path, new_relative_path)
        self.db.rename_file_prefix(old_relative_path, new_relative_path)
        self._prune_empty_parents(old_path.parent, self.local_folder)
        self.replace_folder_states_from_local()
        return True

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
        self.db.clear_sync_data()

    def refresh_last_sync_snapshot_if_synced(self) -> Optional[SyncMismatchReport]:
        """Проверяет, что сервер и ПК синхронизированы и обновляет last_sync_snapshot."""
        remote_structure = self.api_client.get_files_server_structure()
        mismatch_report = self.build_sync_mismatch_report(remote_structure)

        if not mismatch_report.is_empty():
            logger.warning("ПК и сервер не синхронизированы, пропускаю обновление last_sync_snapshot")
            self._log_sync_mismatch_report(mismatch_report)
            return mismatch_report

        logger.info("ПК и сервер синхронизированы, обновляю last_sync_snapshot...")
        self.db.save_last_sync_snapshot(remote_structure)
        self._replace_folder_states_from_remote(remote_structure)
        return None

    # Высокоуровневые сценарии синхронизации
    def initial_sync(
        self,
        strategy: Optional[SyncStrategy] = None,
    ) -> Optional[SyncMismatchReport]:
        """Первичная инициализация связки token + folder."""
        return run_initial_sync(self, strategy)

    def sync_after_restart(self) -> Optional[SyncMismatchReport]:
        """
        Сверяет ПК и сервер относительно last_sync_snapshot после простоя watcher.
        """
        return RestartSyncRunner(self).run()
