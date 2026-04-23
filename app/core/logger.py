"""Логирование ядра: имена логгеров и мост в GUI."""

from __future__ import annotations

import logging

from app.gui import WatcherGUI

APP_CORE_LOGGER_NAME = "app.core"
DEFAULT_LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"


def configure_logging(level: int = logging.INFO) -> None:
    """Единая базовая настройка логирования приложения."""
    logging.basicConfig(
        level=level,
        format=DEFAULT_LOG_FORMAT,
    )


class SyncGuiLogHandler(logging.Handler):
    """Пересылает логи ядра в окно (через thread-safe WatcherGUI.log)."""

    def __init__(self, gui: WatcherGUI):
        super().__init__(level=logging.DEBUG)
        self.gui = gui
        self.setFormatter(logging.Formatter("%(message)s"))

    def emit(self, record: logging.LogRecord) -> None:
        try:
            self.gui.log(self.format(record))
        except Exception:
            self.handleError(record)


def add_log_handler(gui: WatcherGUI) -> None:
    """Вешает SyncGuiLogHandler на app.core.* логгеры."""
    app_core_log = logging.getLogger(APP_CORE_LOGGER_NAME)
    if any(isinstance(h, SyncGuiLogHandler) for h in app_core_log.handlers):
        return
    app_core_log.addHandler(SyncGuiLogHandler(gui))
    app_core_log.setLevel(logging.INFO)
