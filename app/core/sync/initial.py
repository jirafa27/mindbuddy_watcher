"""Сценарий первичной инициализации синхронизации."""

from __future__ import annotations

import logging
from typing import Optional

from .namespace_utils import flatten_namespace_list, get_namespace_paths
from .types import InitialSyncFacade, SyncStrategy


logger = logging.getLogger("app.core.sync")


def run_initial_sync(file_sync: InitialSyncFacade, strategy: Optional[SyncStrategy] = None) -> None:
    """Выполняет первичную инициализацию связки token + folder."""
    local_structure = file_sync.get_local_structure()
    remote_structure = file_sync.api_client.get_files_server_structure()
    local_files = file_sync.get_local_file_index()
    remote_files = flatten_namespace_list(remote_structure)
    local_namespace_paths = get_namespace_paths(local_structure)
    remote_namespace_paths = get_namespace_paths(remote_structure)

    logger.info("Запущена первичная инициализация синхронизации...")

    if strategy == SyncStrategy.SKIP:
        logger.info("Пропускаю первичную инициализацию синхронизации...")

    elif strategy == SyncStrategy.SERVER_PRIORITY:
        logger.info("Заменяю локальную папку серверной структурой...")
        file_sync.empty_local_folder()
        for namespace_path in sorted(remote_namespace_paths, key=lambda path: (path.count("/"), path)):
            if file_sync.create_local_folder_if_not_exists(namespace_path):
                logger.info(f"Создана локальная папка по серверной структуре: {namespace_path}")
        for relative_path, remote_meta in remote_files.items():
            if file_sync.download_remote_file(relative_path, remote_meta):
                logger.info(f"Скачан файл с сервера: {relative_path}")

    elif strategy == SyncStrategy.PC_PRIORITY:
        logger.info("Заменяю серверное пространство локальной структурой...")
        for relative_path, remote_meta in remote_files.items():
            file_id = remote_meta.file_id
            if file_id and file_sync.api_client.delete_server_file(file_id):
                logger.info(f"Удалён файл на сервере: {relative_path}")
        file_sync.delete_remote_namespaces(
            remote_namespace_paths - local_namespace_paths,
            remote_structure,
        )
        file_sync.create_remote_namespace_tree(
            local_namespace_paths,
            remote_structure,
        )
        for relative_path in sorted(local_files.keys()):
            result = file_sync.uploader.upload_file(file_sync.local_folder / relative_path)
            if result["status"] == "uploaded":
                logger.info(f"Загружен файл на сервер: {relative_path}")
            elif result["status"] == "failed":
                logger.warning(f"Ошибка загрузки {relative_path}: {result.get('message')}")

    file_sync.refresh_last_sync_snapshot_if_synced()
