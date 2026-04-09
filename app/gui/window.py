"""GUI интерфейс приложения"""

import tkinter as tk
from tkinter import filedialog, scrolledtext, messagebox
from pathlib import Path
import threading
from typing import Dict, Optional

from app.core.database import SettingsDB
from app.core.sync import SyncStrategy


class WatcherGUI:
    def __init__(self, app):
        self.app = app
        self.root = tk.Tk()
        self.root.title("MindBuddy Watcher")
        self.root.geometry("800x600")
        
        self.selected_folder = None
        self.token = None
        
        # Инициализация БД
        self.db = SettingsDB()
        
        self.setup_ui()
        self.load_saved_settings()
        
    def setup_ui(self):
        """Создание интерфейса"""
        # Выбор папки
        folder_frame = tk.Frame(self.root, padx=10, pady=10)
        folder_frame.pack(fill=tk.X)
        
        tk.Label(folder_frame, text="Папка для файлов:").pack(side=tk.LEFT, padx=5)
        
        self.folder_label = tk.Label(folder_frame, text="Не выбрана", 
                                     bg="lightgray", relief=tk.SUNKEN, anchor=tk.W)
        self.folder_label.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=5)
        
        tk.Button(folder_frame, text="Выбрать папку", 
                 command=self.select_folder).pack(side=tk.LEFT, padx=5)
        
        # Токен аутентификации
        token_frame = tk.Frame(self.root, padx=10, pady=5)
        token_frame.pack(fill=tk.X)
        
        tk.Label(token_frame, text="Токен:").pack(side=tk.LEFT, padx=5)
        
        self.token_entry = tk.Entry(token_frame, show="*", width=40)
        self.token_entry.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=5)
        
        self.show_token_var = tk.BooleanVar()
        tk.Checkbutton(token_frame, text="Показать", variable=self.show_token_var,
                      command=self.toggle_token_visibility).pack(side=tk.LEFT, padx=5)
        
        # Статус подключения
        status_frame = tk.Frame(self.root, padx=10, pady=5)
        status_frame.pack(fill=tk.X)
        
        tk.Label(status_frame, text="Статус:").pack(side=tk.LEFT, padx=5)
        self.status_label = tk.Label(status_frame, text="Отключено", 
                                     fg="red", font=("Arial", 10, "bold"))
        self.status_label.pack(side=tk.LEFT, padx=5)
        
        # Кнопки управления
        control_frame = tk.Frame(self.root, padx=10, pady=10)
        control_frame.pack(fill=tk.X)
        
        self.start_button = tk.Button(control_frame, text="Запустить", 
                                      command=self.start_watcher, 
                                      bg="green", fg="white", font=("Arial", 10, "bold"))
        self.start_button.pack(side=tk.LEFT, padx=5)
        
        self.stop_button = tk.Button(control_frame, text="Остановить", 
                                     command=self.stop_watcher, 
                                     bg="red", fg="white", font=("Arial", 10, "bold"),
                                     state=tk.DISABLED)
        self.stop_button.pack(side=tk.LEFT, padx=5)
        
        # Лог
        log_frame = tk.Frame(self.root, padx=10, pady=10)
        log_frame.pack(fill=tk.BOTH, expand=True)
        
        tk.Label(log_frame, text="Лог событий:").pack(anchor=tk.W)
        
        self.log_text = scrolledtext.ScrolledText(log_frame, height=20, 
                                                  wrap=tk.WORD, state=tk.DISABLED)
        self.log_text.pack(fill=tk.BOTH, expand=True)
        
    def select_folder(self):
        """Выбор папки для файлов"""
        folder = filedialog.askdirectory(title="Выберите папку для файлов")
        if folder:
            self.selected_folder = Path(folder)
            self.folder_label.config(text=str(self.selected_folder))
            self.log(f"Выбрана папка: {self.selected_folder}")
            
    def start_watcher(self):
        """Запуск watcher"""
        if not self.selected_folder:
            messagebox.showwarning("Предупреждение", "Пожалуйста, выберите папку!")
            return
            
        # Получаем токен (обязательно)
        self.token = self.token_entry.get().strip()
        if not self.token:
            messagebox.showwarning("Предупреждение", "Пожалуйста, введите токен!")
            return

        self.db.set_token(self.token)
        # Сохраняем настройки перед запуском
        self.save_settings()
            
        self.start_button.config(state=tk.DISABLED)
        self.stop_button.config(state=tk.NORMAL)
        self.status_label.config(text="Подключение...", fg="orange")
        
        # Запуск в отдельном потоке
        threading.Thread(target=self.app.start, daemon=True).start()
        
    def stop_watcher(self):
        """Остановка watcher"""
        self.app.stop()
        self.start_button.config(state=tk.NORMAL)
        self.stop_button.config(state=tk.DISABLED)
        self.status_label.config(text="Отключено", fg="red")
        self.log("Watcher остановлен")
        
    def update_status(self, status, color="black"):
        """Обновление статуса (thread-safe)"""
        def _update():
            self.status_label.config(text=status, fg=color)
        
        # Безопасное обновление из любого потока
        self.root.after(0, _update)
        
    def log(self, message):
        """Добавление сообщения в лог (thread-safe)"""
        def _log():
            self.log_text.config(state=tk.NORMAL)
            self.log_text.insert(tk.END, f"{message}\n")
            self.log_text.see(tk.END)
            self.log_text.config(state=tk.DISABLED)
        
        # Безопасное обновление из любого потока
        self.root.after(0, _log)
        
    def run(self):
        """Запуск GUI"""
        self.root.mainloop()
        
    def load_saved_settings(self):
        """Загрузка сохранённых настроек из БД"""
        settings = self.db.load_settings()
        if settings:
            # Загружаем токен
            if settings.get('token'):
                self.token_entry.insert(0, settings['token'])
            
            # Загружаем путь к папке
            if settings.get('folder_path'):
                folder_path = Path(settings['folder_path'])
                if folder_path.exists():
                    self.selected_folder = folder_path
                    self.folder_label.config(text=str(self.selected_folder))
            
            self.log("Настройки загружены из БД")
    
    def save_settings(self):
        """Сохранение текущих настроек в БД"""
        token = self.token_entry.get().strip()
        folder_path = str(self.selected_folder) if self.selected_folder else None

        if self.db.save_settings(token=token, folder_path=folder_path):
            self.log("Настройки сохранены в БД")
            return True
        else:
            self.log("Ошибка сохранения настроек")
            return False
    
    def toggle_token_visibility(self):
        """Переключение видимости токена"""
        if self.show_token_var.get():
            self.token_entry.config(show="")
        else:
            self.token_entry.config(show="*")
    
    def ask_sync_strategy(self, discrepancies: Dict) -> Optional[SyncStrategy]:
        """Модальный диалог выбора направления синхронизации.

        Возвращает SyncStrategy или None, если пользователь нажал «Отмена».
        """
        only_local = discrepancies["only_local"]
        only_server = discrepancies["only_server"]
        conflicts = discrepancies["content_conflict"]
        moved = discrepancies["path_moved"]

        lines = []
        if only_local:
            lines.append(f"Только на ПК: {len(only_local)} файл(ов)")
        if only_server:
            lines.append(f"Только на сервере: {len(only_server)} файл(ов)")
        if conflicts:
            lines.append(f"Конфликт содержимого: {len(conflicts)} файл(ов)")
        if moved:
            lines.append(f"Перемещено / переименовано: {len(moved)} файл(ов)")
        summary = "\n".join(lines)

        result = {"strategy": None}

        dialog = tk.Toplevel(self.root)
        dialog.title("Синхронизация")
        dialog.resizable(False, False)
        dialog.grab_set()
        dialog.transient(self.root)

        tk.Label(
            dialog,
            text="Обнаружены расхождения между ПК и сервером:",
            font=("Arial", 11, "bold"),
        ).pack(anchor=tk.W, padx=15, pady=(15, 5))

        tk.Label(
            dialog,
            text=summary,
            justify=tk.LEFT,
            font=("Arial", 10),
        ).pack(anchor=tk.W, padx=20, pady=(0, 10))

        tk.Label(
            dialog,
            text="Файлы, существующие только с одной стороны, "
                 "всегда копируются.\n"
                 "Выберите, как разрешить конфликты содержимого:",
            justify=tk.LEFT,
            wraplength=420,
        ).pack(anchor=tk.W, padx=15, pady=(0, 10))

        btn_frame = tk.Frame(dialog)
        btn_frame.pack(fill=tk.X, padx=15, pady=(0, 15))

        def choose(strategy):
            result["strategy"] = strategy
            dialog.destroy()

        tk.Button(
            btn_frame,
            text="ПК → Сервер\n(версии ПК перезапишут сервер)",
            command=lambda: choose(SyncStrategy.PC_PRIORITY),
            width=25,
            height=3,
        ).pack(side=tk.LEFT, padx=5)

        tk.Button(
            btn_frame,
            text="Сервер → ПК\n(версии сервера перезапишут ПК)",
            command=lambda: choose(SyncStrategy.SERVER_PRIORITY),
            width=25,
            height=3,
        ).pack(side=tk.LEFT, padx=5)

        tk.Button(
            btn_frame,
            text="Отмена",
            command=dialog.destroy,
            width=10,
            height=3,
        ).pack(side=tk.LEFT, padx=5)

        dialog.update_idletasks()
        x = self.root.winfo_x() + (self.root.winfo_width() - dialog.winfo_width()) // 2
        y = self.root.winfo_y() + (self.root.winfo_height() - dialog.winfo_height()) // 2
        dialog.geometry(f"+{x}+{y}")

        self.root.wait_window(dialog)
        return result["strategy"]

    def on_closing(self):
        """Обработка закрытия окна"""
        self.app.stop()
        self.root.destroy()
