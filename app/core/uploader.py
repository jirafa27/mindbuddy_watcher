"""Загрузка локальных файлов на backend по новому sync-контракту."""

import logging
from pathlib import Path
from typing import Dict, List, Optional

from app.core.contracts import NamespaceStructureItem
from app.core.db import SettingsDB
from app.core.file_utils import build_local_file_meta, is_temporary_file
from app.core.namespace_constants import VAULT_ROOT_KIND
from app.core.sync.namespace_utils import build_namespace_path_index

logger = logging.getLogger(__name__)


class FileUploader:
    """Загрузчик desktop-версий файлов с учетом локального file state."""

    def __init__(self, api_client, local_folder, db: SettingsDB):
        self.api_client = api_client
        self.local_folder = Path(local_folder)
        self.db = db

    def _resolve_vault_root_namespace_id(
        self,
        snapshot: Optional[List[NamespaceStructureItem]] = None,
    ) -> Optional[int]:
        if snapshot is None:
            snapshot = self.db.get_last_sync_snapshot()
        for namespace in snapshot:
            if namespace.kind == VAULT_ROOT_KIND and namespace.id is not None:
                return namespace.id
        return None

    def _remember_folder_namespace(
        self,
        folder_path: str,
        namespace_id: int,
        kind: Optional[str] = None,
    ) -> None:
        parent_path = Path(folder_path).parent.as_posix()
        self.db.upsert_folder_state(
            folder_path,
            namespace_id=namespace_id,
            parent_relative_path=None if parent_path in {".", ""} else parent_path,
            kind=kind,
        )

    def _create_missing_parent_namespaces(
        self,
        parent_path: str,
        snapshot: List[NamespaceStructureItem],
        namespace_by_path: Dict[str, NamespaceStructureItem],
    ) -> Optional[int]:
        vault_root_namespace_id = self._resolve_vault_root_namespace_id(snapshot)
        if vault_root_namespace_id is None:
            return None

        current_parent_id = vault_root_namespace_id
        current_path = ""
        created_any = False

        for part in Path(parent_path).parts:
            namespace_path = f"{current_path}/{part}" if current_path else part
            existing_namespace = namespace_by_path.get(namespace_path)
            if existing_namespace and existing_namespace.id is not None:
                current_parent_id = existing_namespace.id
                self._remember_folder_namespace(
                    namespace_path,
                    existing_namespace.id,
                    existing_namespace.kind,
                )
                current_path = namespace_path
                continue

            created_namespace = self.api_client.create_namespace(
                name=part,
                parent_namespace_id=current_parent_id,
            )
            if created_namespace is None or created_namespace.id is None:
                logger.warning("Не удалось создать namespace-цепочку для %s", parent_path)
                return None

            snapshot.append(created_namespace)
            namespace_by_path[namespace_path] = created_namespace
            self._remember_folder_namespace(
                namespace_path,
                created_namespace.id,
                created_namespace.kind,
            )
            current_parent_id = created_namespace.id
            current_path = namespace_path
            created_any = True

        if created_any:
            self.db.save_last_sync_snapshot(snapshot)

        return current_parent_id

    def _resolve_parent_namespace_id_from_live(self, relative_path: str) -> Optional[int]:
        parent_path = Path(relative_path).parent.as_posix()
        try:
            live_structure = self.api_client.get_files_server_structure()
        except Exception as error:
            logger.warning(
                "Не удалось получить live структуру для resolve namespace %s: %s",
                relative_path,
                error,
            )
            return None

        if parent_path in {".", ""}:
            return self._resolve_vault_root_namespace_id(live_structure)

        namespace_by_path = build_namespace_path_index(live_structure)
        namespace = namespace_by_path.get(parent_path)
        if namespace and namespace.id is not None:
            self._remember_folder_namespace(parent_path, namespace.id, namespace.kind)
            return namespace.id

        if not (self.local_folder / parent_path).is_dir() and self.db.get_folder_state(parent_path) is None:
            return None

        return self._create_missing_parent_namespaces(
            parent_path,
            live_structure,
            namespace_by_path,
        )

    def _resolve_parent_namespace_id(self, relative_path: str, force_live: bool = False) -> Optional[int]:
        if force_live:
            return self._resolve_parent_namespace_id_from_live(relative_path)

        parent_path = Path(relative_path).parent.as_posix()
        snapshot = self.db.get_last_sync_snapshot()
        if parent_path in {".", ""}:
            return self._resolve_vault_root_namespace_id(snapshot)

        namespace = build_namespace_path_index(snapshot).get(parent_path)
        if namespace and namespace.id is not None:
            self._remember_folder_namespace(parent_path, namespace.id, namespace.kind)
            return namespace.id

        if not (self.local_folder / parent_path).is_dir() and self.db.get_folder_state(parent_path) is None:
            return None

        return self._create_missing_parent_namespaces(
            parent_path,
            snapshot,
            build_namespace_path_index(snapshot),
        )

    @staticmethod
    def _should_retry_with_live_namespace(error_message: Optional[str]) -> bool:
        if not error_message:
            return False
        normalized = error_message.lower()
        return "пространство с id" in normalized or "namespace" in normalized

    def upload_file(self, file_path, server_user_file_id=None):
        """Отправляет локальный файл на сервер"""
        file_path = Path(file_path)
        if not file_path.exists() or not file_path.is_file():
            logger.error("Файл не существует: %s", file_path)
            return {"status": "failed", "message": "Файл не найден"}

        if is_temporary_file(file_path):
            return {"status": "skipped", "message": "Временный файл игнорируется"}

        try:
            file_meta = build_local_file_meta(file_path, self.local_folder)
            relative_path = file_meta.relative_path

            file_state = self.db.get_file_state(relative_path)
            existing_file_state = (
                self.db.get_file_state_by_server_user_file_id(server_user_file_id)
                if server_user_file_id is not None
                else None
            )
            user_file_id = server_user_file_id if server_user_file_id is not None else (
                file_state.get("server_user_file_id") if file_state else None
            )
            if user_file_id is None and existing_file_state:
                user_file_id = existing_file_state.get("server_user_file_id")

            if file_state:
                same_hash = file_state.get("content_hash") == file_meta.content_hash
                same_mtime = file_state.get("last_seen_mtime") == file_meta.last_seen_mtime
                if same_hash and same_mtime:
                    return {"status": "skipped", "message": "Нет изменений"}

            parent_namespace_id = self._resolve_parent_namespace_id(relative_path)
            if parent_namespace_id is None:
                return {
                    "status": "failed",
                    "message": f"Не удалось определить namespace_id для {relative_path}",
                }

            result = self.api_client.upload_desktop_file(
                file_path=file_path,
                parent_namespace_id=parent_namespace_id,
                filename=file_path.name,
                user_file_id=user_file_id,
            )
            if (
                not result.ok
                and self._should_retry_with_live_namespace(result.message)
            ):
                refreshed_parent_namespace_id = self._resolve_parent_namespace_id(
                    relative_path,
                    force_live=True,
                )
                if (
                    refreshed_parent_namespace_id is not None
                    and refreshed_parent_namespace_id != parent_namespace_id
                ):
                    retry_result = self.api_client.upload_desktop_file(
                        file_path=file_path,
                        parent_namespace_id=refreshed_parent_namespace_id,
                        filename=file_path.name,
                        user_file_id=user_file_id,
                    )
                    if retry_result.ok:
                        parent_namespace_id = refreshed_parent_namespace_id
                        result = retry_result

            if not result.ok:
                return {"status": "failed", "message": result.message or "Upload не удался"}

            file_payload = result.raw.get("file", {}) if isinstance(result.raw, dict) else {}
            namespace_id = (
                file_payload.get("namespace_id")
                if isinstance(file_payload, dict)
                else None
            )
            if namespace_id is None:
                namespace_id = parent_namespace_id
            parent_parts = Path(relative_path).parts[:-1]
            for index in range(len(parent_parts)):
                folder_path = Path(*parent_parts[: index + 1]).as_posix()
                parent_path = Path(*parent_parts[:index]).as_posix() if index > 0 else None
                self.db.upsert_folder_state(
                    folder_path,
                    namespace_id=namespace_id if index == len(parent_parts) - 1 else None,
                    parent_relative_path=parent_path,
                )
            if result.file_id is not None:
                self.db.delete_file_states_by_server_user_file_id(
                    result.file_id,
                    keep_relative_path=relative_path,
                )
            self.db.upsert_file_state(
                relative_path,
                content_hash=result.content_hash or file_meta.content_hash,
                last_uploaded_at=result.updated_at or file_meta.desktop_updated_at,
                last_seen_mtime=file_meta.last_seen_mtime,
                server_user_file_id=result.file_id,
                namespace_id=namespace_id,
                is_applying_remote=0,
            )

            return {
                "status": "uploaded",
                "message": "Файл загружен",
                "relative_path": relative_path,
            }
        except Exception as error:
            logger.error("Ошибка при загрузке файла %s: %s", file_path, error, exc_info=True)
            return {"status": "failed", "message": str(error)}
