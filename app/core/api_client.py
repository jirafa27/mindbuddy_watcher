"""HTTP клиент синхронизации для watcher и RN-контракта."""

import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

from app.core.contracts import NamespaceStructureItem, SyncCommand, SyncCommandAckStatus, UploadResult

logger = logging.getLogger(__name__)


class SyncAPIClient:
    """
    Делает запросы к API сервера
    """

    def __init__(
        self,
        base_url: str,
        token: str,
        device_id: str,
        timeout: int = 30,
    ):
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.device_id = device_id
        self.timeout = timeout
        self.session = requests.Session()

        self.sync_upload_endpoint = f"{self.base_url}/sync/upload"
        self.sync_commands_endpoint = f"{self.base_url}/sync/commands"
        self.sync_structure_endpoint = f"{self.base_url}/sync/structure"
        self.sync_namespaces_endpoint = f"{self.base_url}/sync/namespaces"
        self.namespaces_endpoint = f"{self.base_url}/namespaces/"

    def _headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self.token}",
            "Accept": "application/json",
            "X-Device-Id": self.device_id,
        }

    def _extract_data(self, response: requests.Response):
        try:
            payload = response.json()
        except ValueError:
            return {}
        if isinstance(payload, dict) and "data" in payload:
            return payload["data"]
        return payload

    def upload_desktop_file(
        self,
        file_path: Path,
        parent_namespace_id: int,
        filename: str,
        user_file_id: Optional[int] = None,
    ) -> UploadResult:
        """Загружает локальную desktop-версию файла на сервер"""
        try:
            payload = {
                "namespace_id": parent_namespace_id,
                "filename": filename,
            }
            if user_file_id is not None:
                payload["user_file_id"] = user_file_id

            with open(file_path, "rb") as file_handle:
                response = self.session.post(
                    self.sync_upload_endpoint,
                    headers=self._headers(),
                    data=payload,
                    files={"file": (file_path.name, file_handle, "application/octet-stream")},
                    timeout=self.timeout,
                )

            payload = self._extract_data(response) if response.content else {}
            if response.ok:
                file_payload = payload.get("file", {}) if isinstance(payload, dict) else {}
                return UploadResult(
                    ok=True,
                    status="uploaded",
                    content_hash=file_payload.get("content_hash") if isinstance(file_payload, dict) else None,
                    updated_at=file_payload.get("updated_at") if isinstance(file_payload, dict) else None,
                    file_id=(
                        int(file_payload["user_file_id"])
                        if isinstance(file_payload, dict) and file_payload.get("user_file_id") is not None
                        else None
                    ),
                    raw=payload if isinstance(payload, dict) else {},
                )

            return UploadResult(
                ok=False,
                status="failed",
                message=f"HTTP {response.status_code}: {response.text[:200]}",
                raw=payload if isinstance(payload, dict) else {},
            )
        except Exception as error:
            logger.error("Ошибка загрузки desktop-файла %s: %s", file_path, error)
            return UploadResult(ok=False, status="failed", message=str(error))

    def get_pending_commands(self, limit: int = 100) -> List[SyncCommand]:
        """Получает pending-команды для текущего desktop устройства."""
        try:
            response = self.session.get(
                self.sync_commands_endpoint,
                headers=self._headers(),
                params={"limit": limit},
                timeout=self.timeout,
            )
            if not response.ok:
                logger.error(
                    "Не удалось получить команды синхронизации: %s - %s",
                    response.status_code,
                    response.text[:400],
                )
                return []

            payload = self._extract_data(response)
            if isinstance(payload, dict):
                items = payload.get("commands", [])
            else:
                items = payload or []

            return [SyncCommand.from_dict(item) for item in items if item]
        except Exception as error:
            logger.error("Ошибка polling-команд: %s", error)
            return []

    def ack_command(
        self,
        command_id: str,
        status: SyncCommandAckStatus = SyncCommandAckStatus.ACKED,
        error_message: Optional[str] = None,
    ) -> bool:
        """Подтверждает применение или провал команды."""
        normalized_command_id = int(command_id) if str(command_id).isdigit() else command_id
        payload: Dict[str, Any] = {
            "command_id": normalized_command_id,
            "status": status.value,
        }
        if error_message:
            payload["error_message"] = error_message

        try:
            response = self.session.post(
                f"{self.sync_commands_endpoint}/ack",
                headers=self._headers(),
                json=payload,
                timeout=self.timeout,
            )
            if not response.ok:
                logger.error(
                    "Не удалось подтвердить команду %s: %s - %s",
                    command_id,
                    response.status_code,
                    response.text[:200],
                )
            return response.ok
        except Exception as error:
            logger.error("Ошибка ACK команды %s: %s", command_id, error)
            return False

    def get_files_server_structure(self) -> List[NamespaceStructureItem]:
        """Получает структуру файлов с backend, возвращает список NamespaceStructureItem."""
        try:
            url = self.sync_structure_endpoint
            logger.info("get_files_server_structure → GET %s", url)
            response = self.session.get(
                url,
                headers=self._headers(),
                timeout=self.timeout,
            )
            logger.info("get_files_structure ← status=%s", response.status_code)
            if not response.ok:
                message = f"Не удалось получить снимок файлов: {response.status_code} - {response.text[:400]}"
                raise RuntimeError(message)

            raw_json = response.json()
            namespaces = raw_json.get("data", [])

            logger.info("get_files_server_structure: итого namespace=%d", len(namespaces))
            return sorted([NamespaceStructureItem.from_dict(item) for item in namespaces], key=lambda x: x.id if x.id is not None else 0)
        except Exception as e:
            logger.error("Ошибка получения структуры файлов и папок: %s", e)
            raise

    def create_namespace(
        self,
        name: str,
        parent_namespace_id: int,
        description: str = "",
    ) -> Optional[NamespaceStructureItem]:
        """Создаёт namespace по sync-эндпоинту watcher."""
        payload: Dict[str, Any] = {
            "name": name,
            "parent_namespace_id": parent_namespace_id,
            "description": description,
        }

        try:
            response = self.session.post(
                self.sync_namespaces_endpoint,
                headers=self._headers(),
                json=payload,
                timeout=self.timeout,
            )
            if not response.ok:
                logger.error(
                    "Не удалось создать namespace %s в parent id=%s: %s - %s",
                    name,
                    parent_namespace_id,
                    response.status_code,
                    response.text[:400],
                )
                return None

            payload = self._extract_data(response)
            if not isinstance(payload, dict):
                return None
            return NamespaceStructureItem.from_dict(payload)
        except Exception as error:
            logger.error(
                "Ошибка создания namespace %s в parent id=%s: %s",
                name,
                parent_namespace_id,
                error,
            )
            return None

    def delete_namespace(self, namespace_id: int) -> bool:
        """Полностью удаляет namespace по sync-эндпоинту watcher."""
        try:
            response = self.session.delete(
                f"{self.sync_namespaces_endpoint}/{namespace_id}",
                headers=self._headers(),
                timeout=self.timeout,
            )
            if not response.ok:
                logger.error(
                    "Не удалось удалить namespace id=%s: %s - %s",
                    namespace_id,
                    response.status_code,
                    response.text[:400],
                )
            return response.ok
        except Exception as error:
            logger.error("Ошибка удаления namespace id=%s: %s", namespace_id, error)
            return False

    def move_namespace(self, namespace_id: int, target_parent_id: int) -> bool:
        """Перемещает namespace в другой parent namespace."""
        try:
            response = self.session.put(
                f"{self.sync_namespaces_endpoint}/{namespace_id}/move",
                headers=self._headers(),
                json={"target_parent_id": target_parent_id},
                timeout=self.timeout,
            )
            if not response.ok:
                logger.error(
                    "Не удалось переместить namespace id=%s в parent id=%s: %s - %s",
                    namespace_id,
                    target_parent_id,
                    response.status_code,
                    response.text[:200],
                )
            return response.ok
        except Exception as error:
            logger.error(
                "Ошибка перемещения namespace id=%s в parent id=%s: %s",
                namespace_id,
                target_parent_id,
                error,
            )
            return False

    def rename_namespace(self, namespace_id: int, new_name: str) -> bool:
        """Переименовывает namespace по sync-эндпоинту watcher."""
        try:
            response = self.session.put(
                f"{self.sync_namespaces_endpoint}/{namespace_id}/rename",
                headers=self._headers(),
                json={"new_name": new_name},
                timeout=self.timeout,
            )
            if not response.ok:
                logger.error(
                    "Не удалось переименовать namespace id=%s в %s: %s - %s",
                    namespace_id,
                    new_name,
                    response.status_code,
                    response.text[:200],
                )
            return response.ok
        except Exception as error:
            logger.error(
                "Ошибка переименования namespace id=%s в %s: %s",
                namespace_id,
                new_name,
                error,
            )
            return False

    def delete_server_file(self, file_id: int) -> bool:
        """Полностью удаляет user_file на сервере по его ID."""
        try:
            response = self.session.delete(
                f"{self.base_url}/sync/files/{file_id}",
                headers=self._headers(),
                timeout=self.timeout,
            )
            if not response.ok:
                logger.error("Не удалось удалить файл id=%s: %s - %s", file_id, response.status_code, response.text[:200])
            return response.ok
        except Exception as error:
            logger.error("Ошибка удаления файла id=%s: %s", file_id, error)
            return False

    def rename_server_file(self, file_id: int, new_name: str) -> bool:
        """Переименовывает файл на сервере по его ID."""
        try:
            response = self.session.put(
                f"{self.base_url}/sync/files/{file_id}",
                headers=self._headers(),
                params={"new_name": new_name},
                timeout=self.timeout,
            )
            if not response.ok:
                logger.error(
                    "Не удалось переименовать файл id=%s в %s: %s - %s",
                    file_id,
                    new_name,
                    response.status_code,
                    response.text[:200],
                )
            return response.ok
        except Exception as error:
            logger.error("Ошибка переименования файла id=%s в %s: %s", file_id, new_name, error)
            return False

    def download_file_by_id(self, file_id: int, destination: Path) -> bool:
        """Скачивает файл по серверному ID (для применения команд поллера)."""
        url = f"{self.base_url}/files/download/{file_id}"
        try:
            response = self.session.get(
                url,
                headers=self._headers(),
                stream=True,
                timeout=self.timeout * 2,
            )
            if not response.ok:
                logger.error(
                    "Не удалось скачать файл id=%s: %s - %s",
                    file_id,
                    response.status_code,
                    response.text[:200],
                )
                return False

            destination.parent.mkdir(parents=True, exist_ok=True)
            with open(destination, "wb") as file_handle:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        file_handle.write(chunk)
            return True
        except Exception as error:
            logger.error("Ошибка скачивания файла id=%s: %s", file_id, error)
            return False
