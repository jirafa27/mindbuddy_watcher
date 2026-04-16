"""Операции идемпотентности над applied_commands."""

import logging

from .common import DBContext, connect


logger = logging.getLogger("app.core.database")


def save_applied_command(db: DBContext, command_id: str, relative_path: str) -> bool:
    """Сохраняет факт применения команды для idempotency после рестарта."""
    try:
        with connect(db.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                    INSERT OR REPLACE INTO applied_commands (token, command_id, relative_path, applied_at)
                    VALUES (?, ?, ?, CURRENT_TIMESTAMP)
                """,
                (db.token, command_id, relative_path),
            )
            conn.commit()
            return True
    except Exception as error:
        logger.error("Ошибка сохранения applied_command: %s", error)
        return False


def has_applied_command(db: DBContext, command_id: str) -> bool:
    """Проверяет, применялась ли команда ранее."""
    try:
        with connect(db.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT 1 FROM applied_commands WHERE token = ? AND command_id = ?",
                (db.token, command_id),
            )
            return cursor.fetchone() is not None
    except Exception as error:
        logger.error("Ошибка проверки applied_command: %s", error)
        return False
