"""Общие namespace-константы для синхронизации."""

INBOX_DIRNAME = "Inbox"
TRASH_DIRNAME = "Trash"

INBOX_KIND = "inbox"
TRASH_KIND = "trash"
REGULAR_KIND = "regular"

PROTECTED_NAMESPACE_NAMES = frozenset({INBOX_DIRNAME, TRASH_DIRNAME})
PROTECTED_NAMESPACE_KINDS = frozenset({INBOX_KIND, TRASH_KIND})
