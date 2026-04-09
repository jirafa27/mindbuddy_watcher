"""Модуль для работы с базой данных настроек"""

import sqlite3
import logging
from pathlib import Path
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)


class SettingsDB:
    """База данных для хранения настроек и состояния синхронизации."""

    def __init__(self, db_path="watcher_settings.db", token: str = ""):
        self.db_path = Path(db_path)
        self.token = token
        self.init_db()

    def set_token(self, token: str) -> None:
        """Устанавливает активный токен для всех sync-операций."""
        self.token = token

    def init_db(self):
        """Создание и миграция таблиц БД"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS settings (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        token TEXT,
                        folder_path TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                ''')

                # Пересоздаём sync_state с составным ключом (token, relative_path)
                # если старая схема без token
                cursor.execute("PRAGMA table_info(sync_state)")
                sync_cols = {row[1] for row in cursor.fetchall()}
                if sync_cols and "token" not in sync_cols:
                    cursor.execute("DROP TABLE sync_state")
                    logger.info("Старая таблица sync_state удалена для миграции")
                elif sync_cols and "namespace_id" not in sync_cols:
                    cursor.execute("ALTER TABLE sync_state ADD COLUMN namespace_id INTEGER")
                    logger.info("Миграция sync_state: добавлена колонка namespace_id")
                elif sync_cols and "server_file_id" in sync_cols and "server_user_file_id" not in sync_cols:
                    cursor.execute("ALTER TABLE sync_state RENAME COLUMN server_file_id TO server_user_file_id")
                    logger.info("Миграция sync_state: переименована колонка server_file_id → server_user_file_id")

                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS sync_state (
                        token TEXT NOT NULL,
                        relative_path TEXT NOT NULL,
                        content_hash TEXT,
                        last_uploaded_at TEXT,
                        last_downloaded_at TEXT,
                        last_seen_mtime REAL,
                        last_server_version TEXT,
                        server_user_file_id INTEGER,
                        namespace_id INTEGER,
                        is_applying_remote INTEGER DEFAULT 0,
                        last_command_id TEXT,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        PRIMARY KEY (token, relative_path)
                    )
                ''')

                # Аналогично applied_commands
                cursor.execute("PRAGMA table_info(applied_commands)")
                cmd_cols = {row[1] for row in cursor.fetchall()}
                if cmd_cols and "token" not in cmd_cols:
                    cursor.execute("DROP TABLE applied_commands")
                    logger.info("Старая таблица applied_commands удалена для миграции")

                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS applied_commands (
                        token TEXT NOT NULL,
                        command_id TEXT NOT NULL,
                        relative_path TEXT NOT NULL,
                        applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        PRIMARY KEY (token, command_id)
                    )
                ''')

                cursor.execute('CREATE INDEX IF NOT EXISTS idx_sync_state_version ON sync_state(last_server_version)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_sync_state_remote ON sync_state(is_applying_remote)')
                conn.commit()
                logger.info(f"База данных инициализирована: {self.db_path}")
        except Exception as e:
            logger.error(f"Ошибка инициализации БД: {e}")
    
    def save_settings(self, token=None, folder_path=None):
        """
        Сохранение настроек
        
        Args:
            token: Токен аутентификации
            folder_path: Путь к папке для файлов
        
        Returns:
            bool: True если успешно сохранено
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Проверяем, есть ли уже настройки
                cursor.execute('SELECT id FROM settings LIMIT 1')
                existing = cursor.fetchone()
                
                if existing:
                    # Обновляем существующие настройки
                    cursor.execute('''
                        UPDATE settings 
                        SET token = COALESCE(?, token),
                            folder_path = COALESCE(?, folder_path),
                            updated_at = CURRENT_TIMESTAMP
                        WHERE id = ?
                    ''', (token, folder_path, existing[0]))
                else:
                    # Создаём новые настройки
                    cursor.execute('''
                        INSERT INTO settings (token, folder_path)
                        VALUES (?, ?)
                    ''', (token, folder_path))
                
                conn.commit()
                logger.info("Настройки сохранены")
                return True
        except Exception as e:
            logger.error(f"Ошибка сохранения настроек: {e}")
            return False
    
    def load_settings(self):
        """
        Загрузка настроек
        
        Returns:
            dict: Словарь с настройками или None
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT token, folder_path 
                    FROM settings 
                    ORDER BY id DESC 
                    LIMIT 1
                ''')
                row = cursor.fetchone()
                
                if row:
                    settings = {
                        'token': row[0],
                        'folder_path': row[1]
                    }
                    logger.info("Настройки загружены из БД")
                    return settings
                else:
                    logger.info("Настройки не найдены в БД")
                    return None
        except Exception as e:
            logger.error(f"Ошибка загрузки настроек: {e}")
            return None
    
    def clear_settings(self):
        """Очистка всех настроек"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('DELETE FROM settings')
                conn.commit()
                logger.info("Настройки очищены")
                return True
        except Exception as e:
            logger.error(f"Ошибка очистки настроек: {e}")
            return False
    
    # ========== Методы для состояния синхронизации ==========

    @staticmethod
    def _sync_state_from_row(row) -> Dict:
        return {
            'relative_path': row[0],
            'content_hash': row[1],
            'last_uploaded_at': row[2],
            'last_downloaded_at': row[3],
            'last_seen_mtime': row[4],
            'last_server_version': row[5],
            'server_user_file_id': row[6],
            'namespace_id': row[7],
            'is_applying_remote': bool(row[8]),
            'last_command_id': row[9],
            'updated_at': row[10],
        }

    def get_sync_state(self, relative_path: str) -> Optional[Dict]:
        """Получение состояния синхронизации для файла."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT relative_path, content_hash, last_uploaded_at, last_downloaded_at,
                           last_seen_mtime, last_server_version, server_user_file_id,
                           namespace_id, is_applying_remote, last_command_id, updated_at
                    FROM sync_state
                    WHERE token = ? AND relative_path = ?
                ''', (self.token, relative_path))
                row = cursor.fetchone()

                if not row:
                    return None

                return self._sync_state_from_row(row)
        except Exception as e:
            logger.error(f"Ошибка получения sync_state: {e}")
            return None

    def get_all_sync_states(self) -> List[Dict]:
        """Возвращает все записи sync_state для текущего токена."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT relative_path, content_hash, last_uploaded_at, last_downloaded_at,
                           last_seen_mtime, last_server_version, server_user_file_id,
                           namespace_id, is_applying_remote, last_command_id, updated_at
                    FROM sync_state
                    WHERE token = ?
                ''', (self.token,))
                return [self._sync_state_from_row(row) for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"Ошибка получения списка sync_state: {e}")
            return []

    def get_sync_state_by_file_id(self, file_id: int) -> Optional[Dict]:
        """Возвращает запись sync_state по server_user_file_id."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT relative_path, content_hash, last_uploaded_at, last_downloaded_at,
                           last_seen_mtime, last_server_version, server_user_file_id,
                           namespace_id, is_applying_remote, last_command_id, updated_at
                    FROM sync_state
                    WHERE token = ? AND server_user_file_id = ?
                    LIMIT 1
                ''', (self.token, file_id))
                row = cursor.fetchone()
                return self._sync_state_from_row(row) if row else None
        except Exception as e:
            logger.error(f"Ошибка получения sync_state по file_id={file_id}: {e}")
            return None

    def upsert_sync_state(
        self,
        relative_path: str,
        content_hash: Optional[str] = None,
        last_uploaded_at: Optional[str] = None,
        last_downloaded_at: Optional[str] = None,
        last_seen_mtime: Optional[float] = None,
        last_server_version: Optional[str] = None,
        server_user_file_id: Optional[int] = None,
        namespace_id: Optional[int] = None,
        is_applying_remote: Optional[int] = None,
        last_command_id: Optional[str] = None,
    ) -> bool:
        """Создание или обновление состояния синхронизации файла."""
        try:
            existing = self.get_sync_state(relative_path) or {}
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT OR REPLACE INTO sync_state (
                        token, relative_path, content_hash, last_uploaded_at, last_downloaded_at,
                        last_seen_mtime, last_server_version, server_user_file_id, namespace_id,
                        is_applying_remote, last_command_id, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ''', (
                    self.token,
                    relative_path,
                    content_hash if content_hash is not None else existing.get('content_hash'),
                    last_uploaded_at if last_uploaded_at is not None else existing.get('last_uploaded_at'),
                    last_downloaded_at if last_downloaded_at is not None else existing.get('last_downloaded_at'),
                    last_seen_mtime if last_seen_mtime is not None else existing.get('last_seen_mtime'),
                    last_server_version if last_server_version is not None else existing.get('last_server_version'),
                    server_user_file_id if server_user_file_id is not None else existing.get('server_user_file_id'),
                    namespace_id if namespace_id is not None else existing.get('namespace_id'),
                    int(is_applying_remote) if is_applying_remote is not None else int(existing.get('is_applying_remote', False)),
                    last_command_id if last_command_id is not None else existing.get('last_command_id'),
                ))
                conn.commit()
                return True
        except Exception as e:
            logger.error(f"Ошибка сохранения sync_state: {e}")
            return False

    def mark_remote_apply(self, relative_path: str, is_applying_remote: bool) -> bool:
        """Помечает файл как временно обновляемый со стороны сервера."""
        return self.upsert_sync_state(
            relative_path,
            is_applying_remote=1 if is_applying_remote else 0,
        )

    def save_applied_command(self, command_id: str, relative_path: str) -> bool:
        """Сохраняет факт применения команды для idempotency после рестарта."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT OR REPLACE INTO applied_commands (token, command_id, relative_path, applied_at)
                    VALUES (?, ?, ?, CURRENT_TIMESTAMP)
                ''', (self.token, command_id, relative_path))
                conn.commit()
                return True
        except Exception as e:
            logger.error(f"Ошибка сохранения applied_command: {e}")
            return False

    def has_applied_command(self, command_id: str) -> bool:
        """Проверяет, применялась ли команда ранее."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    'SELECT 1 FROM applied_commands WHERE token = ? AND command_id = ?',
                    (self.token, command_id),
                )
                return cursor.fetchone() is not None
        except Exception as e:
            logger.error(f"Ошибка проверки applied_command: {e}")
            return False

    def delete_sync_state(self, relative_path: str) -> bool:
        """Удаление записи кэша синхронизации для одного файла."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.cursor().execute(
                    'DELETE FROM sync_state WHERE token = ? AND relative_path = ?',
                    (self.token, relative_path),
                )
                conn.commit()
                return True
        except Exception as e:
            logger.error(f"Ошибка удаления sync_state для {relative_path}: {e}")
            return False

    def rename_sync_state(self, old_path: str, new_path: str) -> bool:
        """Переименовывает ключ relative_path у sync_state в одной транзакции."""
        if old_path == new_path:
            return True

        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT content_hash, last_uploaded_at, last_downloaded_at,
                           last_seen_mtime, last_server_version, server_user_file_id,
                           namespace_id, is_applying_remote, last_command_id
                    FROM sync_state
                    WHERE token = ? AND relative_path = ?
                ''', (self.token, old_path))
                row = cursor.fetchone()
                if not row:
                    return False

                cursor.execute(
                    'DELETE FROM sync_state WHERE token = ? AND relative_path = ?',
                    (self.token, old_path),
                )
                cursor.execute('''
                    INSERT OR REPLACE INTO sync_state (
                        token, relative_path, content_hash, last_uploaded_at, last_downloaded_at,
                        last_seen_mtime, last_server_version, server_user_file_id, namespace_id,
                        is_applying_remote, last_command_id, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ''', (
                    self.token,
                    new_path,
                    row[0],
                    row[1],
                    row[2],
                    row[3],
                    row[4],
                    row[5],
                    row[6],
                    row[7],
                    row[8],
                ))
                conn.commit()
                return True
        except Exception as e:
            logger.error(f"Ошибка rename_sync_state {old_path} -> {new_path}: {e}")
            return False

    def clear_sync_state(self) -> bool:
        """Очистка кэша синхронизации текущего экземпляра."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('DELETE FROM sync_state WHERE token = ?', (self.token,))
                cursor.execute('DELETE FROM applied_commands WHERE token = ?', (self.token,))
                conn.commit()
                logger.info("Состояние синхронизации очищено")
                return True
        except Exception as e:
            logger.error(f"Ошибка очистки состояния синхронизации: {e}")
            return False
