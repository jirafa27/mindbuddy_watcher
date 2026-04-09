"""Вспомогательные функции для файлов и состояния синхронизации."""

import hashlib
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict


def compute_file_hash(file_path: Path) -> str:
    """Вычисляет SHA-256 хеш файла."""
    digest = hashlib.sha256()

    with open(file_path, "rb") as file_handle:
        for chunk in iter(lambda: file_handle.read(1024 * 1024), b""):
            digest.update(chunk)

    return digest.hexdigest()


def isoformat_from_timestamp(timestamp: float) -> str:
    """Преобразует timestamp файловой системы в UTC ISO-строку."""
    return datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat()


def normalize_relative_path(file_path: Path, root_folder: Path) -> str:
    """Строит единый относительный путь для запросов к backend."""
    return str(file_path.resolve().relative_to(root_folder.resolve())).replace("\\", "/")


def build_local_file_meta(file_path: Path, root_folder: Path) -> Dict[str, str]:
    """Собирает метаданные локального файла для upload/sync."""
    stat_result = file_path.stat()
    return {
        "relative_path": normalize_relative_path(file_path, root_folder),
        "content_hash": compute_file_hash(file_path),
        "desktop_updated_at": isoformat_from_timestamp(stat_result.st_mtime),
        "last_seen_mtime": stat_result.st_mtime,
        "file_size": stat_result.st_size,
    }
