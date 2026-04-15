"""Загрузка локальных файлов на backend по новому sync-контракту."""

import logging
from pathlib import Path

from app.core.database import SettingsDB
from app.core.file_utils import build_local_file_meta

logger = logging.getLogger(__name__)


class FileUploader:
    """Загрузчик desktop-версий файлов с учетом локального file state."""

    def __init__(self, api_client, local_folder, db: SettingsDB):
        self.api_client = api_client
        self.local_folder = Path(local_folder)
        self.db = db

    @staticmethod
    def is_temporary_file(file_path: Path) -> bool:
        """Отфильтровывает временные файлы, которые не нужно синхронизировать."""
        name = file_path.name
        return (
            name.startswith("~$")
            or name.endswith(".tmp")
            or name.endswith(".temp")
            or name.startswith(".~")
        )

    def upload_file(self, file_path, server_user_file_id=None):
        """Отправляет локальный файл на сервер"""
        file_path = Path(file_path)
        if not file_path.exists() or not file_path.is_file():
            logger.error("Файл не существует: %s", file_path)
            return {"status": "failed", "message": "Файл не найден"}

        if self.is_temporary_file(file_path):
            return {"status": "skipped", "message": "Временный файл игнорируется"}

        try:
            file_meta = build_local_file_meta(file_path, self.local_folder)
            relative_path = file_meta.relative_path

            file_state = self.db.get_file_state(relative_path)
            user_file_id = server_user_file_id if server_user_file_id is not None else (
                file_state.get("server_user_file_id") if file_state else None
            )

            if file_state:
                same_hash = file_state.get("content_hash") == file_meta.content_hash
                same_mtime = file_state.get("last_seen_mtime") == file_meta.last_seen_mtime
                if same_hash and same_mtime:
                    return {"status": "skipped", "message": "Нет изменений"}

            result = self.api_client.upload_desktop_file(
                file_path=file_path,
                relative_path=relative_path,
                user_file_id=user_file_id,
            )

            if not result.ok:
                return {"status": "failed", "message": result.message or "Upload не удался"}

            file_payload = result.raw.get("file", {}) if isinstance(result.raw, dict) else {}
            namespace_id = file_payload.get("namespace_id") if isinstance(file_payload, dict) else None
            parent_parts = Path(relative_path).parts[:-1]
            for index in range(len(parent_parts)):
                folder_path = Path(*parent_parts[: index + 1]).as_posix()
                parent_path = Path(*parent_parts[:index]).as_posix() if index > 0 else None
                self.db.upsert_folder_state(
                    folder_path,
                    namespace_id=namespace_id if index == len(parent_parts) - 1 else None,
                    parent_relative_path=parent_path,
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
