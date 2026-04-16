"""Создание схемы и простые миграции локальной SQLite БД."""

import logging
from pathlib import Path

from .common import connect


logger = logging.getLogger("app.core.database")


def init_db(db_path: Path) -> None:
    """Создает таблицы, индексы и применяет совместимые миграции."""
    try:
        with connect(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                    CREATE TABLE IF NOT EXISTS settings (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        token TEXT,
                        folder_path TEXT,
                        last_initialized_token TEXT,
                        last_initialized_folder_path TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """
            )

            cursor.execute("PRAGMA table_info(settings)")
            settings_cols = {row[1] for row in cursor.fetchall()}
            if settings_cols and "last_initialized_token" not in settings_cols:
                cursor.execute("ALTER TABLE settings ADD COLUMN last_initialized_token TEXT")
            if settings_cols and "last_initialized_folder_path" not in settings_cols:
                cursor.execute("ALTER TABLE settings ADD COLUMN last_initialized_folder_path TEXT")

            cursor.execute("DROP TABLE IF EXISTS sync_state")

            cursor.execute(
                """
                    CREATE TABLE IF NOT EXISTS files (
                        token TEXT NOT NULL,
                        relative_path TEXT NOT NULL,
                        content_hash TEXT,
                        last_uploaded_at TEXT,
                        last_downloaded_at TEXT,
                        last_seen_mtime REAL,
                        server_user_file_id INTEGER,
                        namespace_id INTEGER,
                        is_applying_remote INTEGER DEFAULT 0,
                        last_command_id TEXT,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        PRIMARY KEY (token, relative_path)
                    )
                """
            )

            cursor.execute(
                """
                    CREATE TABLE IF NOT EXISTS folders (
                        token TEXT NOT NULL,
                        relative_path TEXT NOT NULL,
                        namespace_id INTEGER,
                        parent_relative_path TEXT,
                        kind TEXT,
                        is_applying_remote INTEGER DEFAULT 0,
                        last_command_id TEXT,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        PRIMARY KEY (token, relative_path)
                    )
                """
            )

            cursor.execute("PRAGMA table_info(applied_commands)")
            cmd_cols = {row[1] for row in cursor.fetchall()}
            if cmd_cols and "token" not in cmd_cols:
                cursor.execute("DROP TABLE applied_commands")
                logger.info("Старая таблица applied_commands удалена для миграции")

            cursor.execute(
                """
                    CREATE TABLE IF NOT EXISTS applied_commands (
                        token TEXT NOT NULL,
                        command_id TEXT NOT NULL,
                        relative_path TEXT NOT NULL,
                        applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        PRIMARY KEY (token, command_id)
                    )
                """
            )

            cursor.execute(
                """
                    CREATE TABLE IF NOT EXISTS last_sync_snapshot (
                        token TEXT PRIMARY KEY,
                        snapshot_json TEXT NOT NULL,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """
            )

            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_files_remote "
                "ON files(token, is_applying_remote)"
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_files_server_user_file_id "
                "ON files(token, server_user_file_id)"
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_folders_remote "
                "ON folders(token, is_applying_remote)"
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_folders_parent "
                "ON folders(token, parent_relative_path)"
            )
            conn.commit()
            logger.info("База данных инициализирована: %s", db_path)
    except Exception as error:
        logger.error("Ошибка инициализации БД: %s", error)
