"""HTTP клиент синхронизации для watcher и RN-контракта."""

import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

from app.core.contracts import SyncCommand, SyncFileMeta, UploadResult

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
        desktop_updated_at: str,
        user_file_id: Optional[int] = None,
        known_hash: Optional[str] = None,
    ) -> UploadResult:
        """Загружает локальную desktop-версию файла как каноническую."""
        try:
            payload = {
                "relative_path": relative_path,
                "desktop_updated_at": desktop_updated_at,
                "vault_name": self.vault_name,
            }
            if user_file_id is not None:
                payload["user_file_id"] = user_file_id
            if known_hash is not None:
                payload["known_hash"] = known_hash

            with open(file_path, "rb") as file_handle:
                response = self.session.post(
                    self.sync_upload_endpoint,
                    headers=self._headers(),
                    data=self._params(payload),
                    files={"file": (file_path.name, file_handle, "application/octet-stream")},
                    timeout=self.timeout,
                )

            payload = self._extract_data(response) if response.content else {}
            if response.ok:
                return UploadResult(
                    ok=True,
                    status="uploaded",
                    content_hash=payload.get("content_hash") if isinstance(payload, dict) else None,
                    updated_at=(payload.get("updated_at") if isinstance(payload, dict) else None) or desktop_updated_at,
                    file_id=int(payload["file_id"]) if isinstance(payload, dict) and payload.get("file_id") is not None else None,
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

    def get_files_structure(self) -> Dict[str, SyncFileMeta]:
        """Получает структуру файлов с backend, возвращает плоский словарь {relative_path: SyncFileMeta}."""
        try:
            url = self.sync_structure_endpoint
            params = self._params()
            logger.info("get_files_structure → GET %s params=%s", url, params)
            response = self.session.get(
                url,
                headers=self._headers(),
                params=params,
                timeout=self.timeout,
            )
            logger.info("get_files_structure ← status=%s", response.status_code)
            if not response.ok:
                logger.error(
                    "Не удалось получить снимок файлов: %s - %s",
                    response.status_code,
                    response.text[:400],
                )
                return {}

            raw_json = response.json()
            payload = raw_json.get("data", raw_json) if isinstance(raw_json, dict) else raw_json
            namespaces = payload.get("namespaces", []) if isinstance(payload, dict) else []

            result: Dict[str, SyncFileMeta] = {}
            self._flatten_namespaces(namespaces, result)
            logger.info("get_files_structure: итого файлов=%d", len(result))
            return result
        except Exception as error:
            logger.error("Ошибка получения структуры файлов и папок: %s", error)
            return {}

    def _flatten_namespaces(self, namespaces: List[Dict[str, Any]], result: Dict[str, SyncFileMeta]):
        """Рекурсивно разворачивает дерево namespace-ов в плоский словарь файлов."""
        for ns in namespaces:
            ns_id = ns.get("id")
            for file_dict in ns.get("files", []):
                enriched = {**file_dict, "namespace_id": ns_id}
                meta = SyncFileMeta.from_dict(enriched)
                if meta.relative_path:
                    result[meta.relative_path] = meta
            children = ns.get("children") or []
            if children:
                self._flatten_namespaces(children, result)

    def delete_desktop_file(self, file_id: int) -> bool:
        """Удаляет файл на сервере по его ID."""
        try:
            response = self.session.delete(
                f"{self.base_url}/files/{file_id}",
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
