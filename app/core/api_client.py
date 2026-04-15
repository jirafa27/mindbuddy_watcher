"""HTTP клиент синхронизации для watcher и RN-контракта."""

import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

from app.core.contracts import NamespaceStructureItem, SyncCommand, UploadResult

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
        vault_name: str = "",
        timeout: int = 30,
    ):
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.device_id = device_id
        self.vault_name = vault_name
        self.timeout = timeout
        self.session = requests.Session()

        self.sync_upload_endpoint = f"{self.base_url}/sync/upload"
        self.sync_commands_endpoint = f"{self.base_url}/sync/commands"
        self.sync_structure_endpoint = f"{self.base_url}/sync/structure"
        self.sync_file_content_endpoint = f"{self.base_url}/sync/file-content"
        self.sync_namespaces_endpoint = f"{self.base_url}/sync/namespaces"
        self.namespaces_endpoint = f"{self.base_url}/namespaces/"

    def _headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self.token}",
            "Accept": "application/json",
            "X-Device-Id": self.device_id,
        }

    def _params(self, extra: Optional[Dict] = None) -> Dict:
        params: Dict = {"device_id": self.device_id}
        if self.vault_name:
            params["vault_name"] = self.vault_name
        if extra:
            params.update(extra)
        return params

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
        relative_path: str,
        user_file_id: Optional[int] = None,
    ) -> UploadResult:
        """Загружает локальную desktop-версию файла на сервер"""
        try:
            payload = {
                "relative_path": relative_path,
                "vault_name": self.vault_name,
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
                params=self._params({"limit": limit}),
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
        status: str = "applied",
        error_message: Optional[str] = None,
    ) -> bool:
        """Подтверждает применение или провал команды."""
        normalized_command_id = int(command_id) if str(command_id).isdigit() else command_id
        payload = {"command_ids": [normalized_command_id]}

        try:
            response = self.session.post(
                f"{self.sync_commands_endpoint}/ack",
                headers=self._headers(),
                params=self._params(),
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
        relative_path: str,
        description: str = "",
    ) -> Optional[NamespaceStructureItem]:
        """Создаёт namespace по sync-эндпоинту watcher."""
        payload: Dict[str, Any] = {
            "relative_path": relative_path,
            "vault_name": self.vault_name,
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
                    "Не удалось создать namespace %s: %s - %s",
                    relative_path,
                    response.status_code,
                    response.text[:400],
                )
                return None

            payload = self._extract_data(response)
            if not isinstance(payload, dict):
                return None
            return NamespaceStructureItem.from_dict(payload)
        except Exception as error:
            logger.error("Ошибка создания namespace %s: %s", relative_path, error)
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

    def download_file(
        self,
        relative_path: str,
        destination: Path,
        download_url: Optional[str] = None,
    ) -> bool:
        """Скачивает файл по относительному пути или прямому URL."""
        target_url = download_url or self.sync_file_content_endpoint
        request_kwargs = {
            "headers": self._headers(),
            "timeout": self.timeout * 2,
            "stream": True,
        }

        if download_url:
            request_kwargs["params"] = {}
        else:
            request_kwargs["params"] = self._params({"relative_path": relative_path})

        try:
            response = self.session.get(target_url, **request_kwargs)
            if not response.ok:
                logger.error(
                    "Не удалось скачать файл %s: %s - %s",
                    relative_path,
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
            logger.error("Ошибка скачивания файла %s: %s", relative_path, error)
            return False
