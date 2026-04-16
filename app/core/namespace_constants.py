"""Общие namespace-константы для синхронизации."""

VAULT_ROOT_NAME = "Vault"
INBOX_DIRNAME = "Inbox"
TRASH_DIRNAME = "Trash"

INBOX_KIND = "inbox"
TRASH_KIND = "trash"
REGULAR_KIND = "regular"
VAULT_ROOT_KIND = "vault_root"

PROTECTED_NAMESPACE_NAMES = frozenset({INBOX_DIRNAME, TRASH_DIRNAME})
PROTECTED_NAMESPACE_KINDS = frozenset({INBOX_KIND, TRASH_KIND})
