"""Операции над таблицей settings."""

import logging
from typing import Dict, Optional

from .common import DBContext, connect


logger = logging.getLogger("app.core.database")


def save_settings(db: DBContext, token=None, folder_path=None):
    """Сохраняет настройки приложения."""
    try:
        with connect(db.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT id FROM settings LIMIT 1")
            existing = cursor.fetchone()

            if existing:
                cursor.execute(
                    """
                        UPDATE settings
                        SET token = COALESCE(?, token),
                            folder_path = COALESCE(?, folder_path),
                            updated_at = CURRENT_TIMESTAMP
                        WHERE id = ?
                    """,
                    (token, folder_path, existing[0]),
                )
            else:
                cursor.execute(
                    """
                        INSERT INTO settings (token, folder_path)
                        VALUES (?, ?)
                    """,
                    (token, folder_path),
                )

            conn.commit()
            logger.info("Настройки сохранены")
            return True
    except Exception as error:
        logger.error("Ошибка сохранения настроек: %s", error)
        return False


def load_settings(db: DBContext):
    """Загружает настройки приложения."""
    try:
        with connect(db.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                    SELECT token, folder_path
                    FROM settings
                    ORDER BY id DESC
                    LIMIT 1
                """
            )
            row = cursor.fetchone()

            if row:
                settings = {
                    "token": row[0],
                    "folder_path": row[1],
                }
                logger.info("Настройки загружены из БД")
                return settings

            logger.info("Настройки не найдены в БД")
            return None
    except Exception as error:
        logger.error("Ошибка загрузки настроек: %s", error)
        return None


def clear_settings(db: DBContext):
    """Очищает все настройки."""
    try:
        with connect(db.db_path) as conn:
            conn.cursor().execute("DELETE FROM settings")
            conn.commit()
            logger.info("Настройки очищены")
            return True
    except Exception as error:
        logger.error("Ошибка очистки настроек: %s", error)
        return False


def get_last_initialized_binding(db: DBContext) -> Optional[Dict[str, Optional[str]]]:
    """Возвращает последнюю успешно инициализированную связку token + folder_path."""
    try:
        with connect(db.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                    SELECT last_initialized_token, last_initialized_folder_path
                    FROM settings
                    ORDER BY id DESC
                    LIMIT 1
                """
            )
            row = cursor.fetchone()
            if not row or (not row[0] and not row[1]):
                return None
            return {
                "token": row[0],
                "folder_path": row[1],
            }
    except Exception as error:
        logger.error("Ошибка получения initialized binding: %s", error)
        return None


def save_last_initialized_binding(db: DBContext, token: str, folder_path: str) -> bool:
    """Сохраняет последнюю успешно инициализированную связку token + folder_path."""
    try:
        with connect(db.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT id FROM settings ORDER BY id DESC LIMIT 1")
            existing = cursor.fetchone()
            if existing:
                cursor.execute(
                    """
                        UPDATE settings
                        SET last_initialized_token = ?,
                            last_initialized_folder_path = ?,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE id = ?
                    """,
                    (token, folder_path, existing[0]),
                )
            else:
                cursor.execute(
                    """
                        INSERT INTO settings (
                            token, folder_path, last_initialized_token, last_initialized_folder_path
                        )
                        VALUES (?, ?, ?, ?)
                    """,
                    (token, folder_path, token, folder_path),
                )
            conn.commit()
            return True
    except Exception as error:
        logger.error("Ошибка сохранения initialized binding: %s", error)
        return False
