"""Конфигурация приложения."""

import platform

# Базовый HTTP API для polling-потока синхронизации.
API_BASE_URL = "http://localhost:8000/api/v1"

# Параметры polling-а.
SYNC_POLL_INTERVAL_SECONDS = 10
SYNC_COMMANDS_BATCH_SIZE = 100
REQUEST_TIMEOUT_SECONDS = 30

# Полная сверка с сервером каждые N циклов polling-а.
RECONCILE_EVERY_N_POLLS = 6

# Идентификатор desktop-устройства для очереди команд.
DEFAULT_DEVICE_ID = platform.node() or "mindbuddy-desktop"
