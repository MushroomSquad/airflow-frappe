"""Fernet encryption/decryption using the same key as Airflow.

Reads AIRFLOW_FERNET_KEY from environment. This must be identical to
AIRFLOW__CORE__FERNET_KEY in the Airflow deployment.
"""
from __future__ import annotations

import os

from cryptography.fernet import Fernet, InvalidToken


def _get_key() -> bytes:
    key = os.environ.get("AIRFLOW_FERNET_KEY", "")
    if not key:
        raise RuntimeError("AIRFLOW_FERNET_KEY environment variable is not set")
    return key.encode() if isinstance(key, str) else key


def encrypt(value: str) -> str:
    """Encrypt a plain-text string. Returns URL-safe base64 Fernet token."""
    return Fernet(_get_key()).encrypt(value.encode()).decode()


def decrypt(value: str) -> str:
    """Decrypt a Fernet token. Raises InvalidToken if key is wrong."""
    return Fernet(_get_key()).decrypt(value.encode()).decode()


def is_encrypted(value: str) -> bool:
    """Heuristic: Fernet tokens start with 'gAAA'."""
    return value.startswith("gAAA")
