# MindBuddy Watcher

Приложение для синхронизации файлов между локальной папкой и сервером через WebSocket и HTTP.

## Возможности

- Получение задач на загрузку файлов через WebSocket
- Автоматическое сохранение файлов в выбранную папку
- Отслеживание новых файлов в папке
- Автоматическая загрузка новых файлов на сервер

## Установка

```bash
pip install -r requirements.txt
```


## Использование

1. Запустите приложение одним из способов:
```bash
# Способ 1: через run.py
python run.py

# Способ 2: через модуль
python -m app.main
```

2. В открывшемся окне выберите папку для файлов (кнопка "Выбрать папку")

3. Нажмите кнопку "Запустить" для начала работы

4. Приложение будет:
   - Подключаться к WebSocket серверу и получать задачи на загрузку файлов
   - Сохранять полученные файлы в выбранную папку
   - Отслеживать новые файлы в папке
   - Автоматически загружать новые файлы на сервер через HTTP API

## Настройка

В файле `app/config.py` можно настроить:
- `WEBSOCKET_URL` - URL WebSocket сервера (по умолчанию: `ws://localhost:8000/watcher/ws`)
- `API_BASE_URL` - Базовый URL HTTP API сервера (по умолчанию: `http://localhost:8000/api/v1`)
- `DEFAULT_NAMESPACE_ID` - ID пространства имен по умолчанию (по умолчанию: 1)
- `DEFAULT_USER_ID` - ID пользователя по умолчанию (по умолчанию: 1)

## Формат сообщений WebSocket

Приложение ожидает сообщения в формате JSON:

**Скачивание файла по URL:**
```json
{
  "type": "download_file",
  "url": "http://example.com/file.pdf",
  "filename": "document.pdf"
}
```

**Сохранение файла из base64:**
```json
{
  "type": "download_file",
  "data": "base64_encoded_data",
  "filename": "document.pdf"
}
```

## HTTP API

При обнаружении нового файла в папке, приложение отправляет POST запрос:
```
POST http://localhost:8000/api/v1/files/upload
Content-Type: multipart/form-data

file: <файл>
namespace_id: 1
user_id: 1
```
