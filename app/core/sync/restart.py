"""Сценарий синхронизации после перезапуска watcher."""

from __future__ import annotations

import logging
from typing import Dict, Optional, Set

from app.core.contracts import IndexedFileMeta, LocalFileMeta

from .namespace_utils import flatten_namespace_list, get_namespace_paths
from .types import RestartSyncContext, RestartSyncFacade


logger = logging.getLogger("app.core.sync")


class RestartSyncRunner:
    """Выполняет reconciliation локального и серверного состояния после рестарта."""

    def __init__(self, file_sync: RestartSyncFacade):
        self.file_sync = file_sync

    def run(self) -> None:
        logger.info("Запущена синхронизация после перезапуска watcher...")
        context = self._build_restart_sync_context()
        self._sync_directory_changes(context)
        for last_path, last_state in context.last_sync.items():
            self._sync_known_file_state(context, last_path, last_state)
        self._upload_new_local_files(context)
        self._apply_new_remote_files(context)
        self._apply_removed_directory_changes(context)
        self.file_sync.refresh_last_sync_snapshot_if_synced()

    def _build_restart_sync_context(self) -> RestartSyncContext:
        local_structure = self.file_sync.get_local_structure()
        remote_structure = self.file_sync.api_client.get_files_server_structure()
        last_structure = self.file_sync.db.get_last_sync_snapshot()

        local_files = self.file_sync.get_local_file_index()
        remote_files = flatten_namespace_list(remote_structure)
        last_sync = {
            state.relative_path: {
                "relative_path": state.relative_path,
                "content_hash": state.content_hash,
                "server_user_file_id": state.file_id,
                "namespace_id": state.namespace_id,
            }
            for state in flatten_namespace_list(last_structure).values()
        }
        last_paths = set(last_sync.keys())
        local_namespace_paths = get_namespace_paths(local_structure)
        remote_namespace_paths = get_namespace_paths(remote_structure)
        last_namespace_paths = get_namespace_paths(last_structure)

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
            if self.file_sync.create_local_folder_if_not_exists(namespace_path):
                context.stats["created_local_dirs"] += 1
                logger.info(f"Создана локальная папка по серверной структуре: {namespace_path}")

        context.stats["created_remote_dirs"] = self.file_sync.create_remote_namespace_tree(
            context.new_local_dirs,
            context.remote_structure,
        )
        context.remote_namespace_paths = get_namespace_paths(context.remote_structure)

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

        if not local_exists and not remote_exists:
            return

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
            if last_file_id and self.file_sync.api_client.delete_server_file(last_file_id):
                context.stats["deleted_remote"] += 1
                logger.info(f"Удалён на сервере после локального удаления: {last_path}")
            return

        result = self.file_sync.upload_local_file(
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
            if self.file_sync.delete_local_file(last_path):
                context.stats["deleted_local"] += 1
                logger.info(f"Удалён локальный файл по серверному удалению: {last_path}")
            return

        action = self.file_sync.apply_remote_file_state(remote_meta, previous_relative_path=last_path)
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
            result = self.file_sync.upload_local_file(
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

        if last_file_id and self.file_sync.api_client.delete_server_file(last_file_id):
            context.stats["deleted_remote"] += 1
            logger.info(f"Конфликт удаления разрешён в пользу ПК: {last_path}")

    def _upload_new_local_files(self, context: RestartSyncContext) -> None:
        for relative_path, local_meta in context.local_files.items():
            if relative_path in context.last_paths or relative_path in context.processed_local_paths:
                continue
            result = self.file_sync.upload_local_file(relative_path)
            if result["status"] == "uploaded":
                context.stats["uploaded"] += 1
                logger.info(f"Новый локальный файл загружен на сервер: {relative_path}")
            elif result["status"] == "failed":
                logger.warning(f"Ошибка загрузки нового локального файла {relative_path}: {result.get('message')}")

    def _apply_new_remote_files(self, context: RestartSyncContext) -> None:
        for relative_path, remote_meta in context.remote_files.items():
            if relative_path in context.last_paths or relative_path in context.processed_remote_paths:
                continue
            action = self.file_sync.apply_remote_file_state(remote_meta)
            if action in {"downloaded", "moved"}:
                context.stats["downloaded"] += 1
            if action != "failed":
                logger.info(f"Новый серверный файл применён локально: {relative_path}")
            else:
                logger.warning(f"Ошибка загрузки нового серверного файла: {relative_path}")

    def _apply_removed_directory_changes(self, context: RestartSyncContext) -> None:
        for namespace_path in sorted(context.removed_remote_dirs, key=lambda path: path.count("/"), reverse=True):
            if self.file_sync.delete_local_namespace_if_empty(namespace_path):
                context.stats["deleted_local_dirs"] += 1
                logger.info(f"Удалена локальная пустая папка по серверной структуре: {namespace_path}")

        context.stats["deleted_remote"] += self.file_sync.delete_remote_files_under_prefixes(
            context.removed_local_dirs,
            context.remote_files,
        )
        context.stats["deleted_remote_dirs"] = self.file_sync.delete_remote_namespaces(
            context.removed_local_dirs,
            context.remote_structure,
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
