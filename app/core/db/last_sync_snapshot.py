"""Операции над last_sync_snapshot."""

import json
import logging
from typing import List

from app.core.contracts import NamespaceStructureItem

from .common import DBContext, connect


logger = logging.getLogger("app.core.database")


def save_last_sync_snapshot(db: DBContext, structure: List[NamespaceStructureItem]) -> bool:
    """Сохраняет last_sync_snapshot текущего токена одним JSON-снимком."""
    try:
        with connect(db.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                    INSERT OR REPLACE INTO last_sync_snapshot (
                        token, snapshot_json, updated_at
                    ) VALUES (?, ?, CURRENT_TIMESTAMP)
                """,
                (
                    db.token,
                    json.dumps([entry.to_dict() for entry in structure], ensure_ascii=True),
                ),
            )
            conn.commit()
            return True
    except Exception as error:
        logger.error("Ошибка сохранения last_sync_snapshot: %s", error)
        return False


def get_last_sync_snapshot(db: DBContext) -> List[NamespaceStructureItem]:
    """Возвращает сохранённый last_sync_snapshot текущего токена."""
    try:
        with connect(db.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                    SELECT snapshot_json
                    FROM last_sync_snapshot
                    WHERE token = ?
                    LIMIT 1
                """,
                (db.token,),
            )
            row = cursor.fetchone()
            if not row or not row[0]:
                return []
            payload = json.loads(row[0])
            if not isinstance(payload, list):
                return []
            return [NamespaceStructureItem.from_dict(item) for item in payload if item]
    except Exception as error:
        logger.error("Ошибка получения last_sync_snapshot: %s", error)
        return []


def clear_last_sync_snapshot(db: DBContext) -> bool:
    """Удаляет last_sync_snapshot текущего токена."""
    try:
        with connect(db.db_path) as conn:
            conn.cursor().execute(
                "DELETE FROM last_sync_snapshot WHERE token = ?",
                (db.token,),
            )
            conn.commit()
            return True
    except Exception as error:
        logger.error("Ошибка очистки last_sync_snapshot: %s", error)
        return False
