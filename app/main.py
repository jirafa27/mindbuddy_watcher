"""Точка входа в приложение"""

from app.core import MindBuddyWatcher


def main():
    """Запуск приложения"""
    app = MindBuddyWatcher()
    app.run()


if __name__ == "__main__":
    main()
