import os

import pytest
from cryptography.fernet import Fernet

# Generate a test key once for the whole module
_TEST_KEY = Fernet.generate_key().decode()


@pytest.fixture(autouse=True)
def set_fernet_key(monkeypatch):
    monkeypatch.setenv("AIRFLOW_FERNET_KEY", _TEST_KEY)


from frappe_airflow.airflow_db.fernet import encrypt, decrypt, is_encrypted


def test_roundtrip():
    plain = "super-secret-api-token"
    assert decrypt(encrypt(plain)) == plain


def test_encrypt_returns_string():
    result = encrypt("hello")
    assert isinstance(result, str)
    assert len(result) > 0


def test_is_encrypted_true():
    assert is_encrypted(encrypt("something")) is True


def test_is_encrypted_false():
    assert is_encrypted("plain-text") is False


def test_missing_key_raises(monkeypatch):
    monkeypatch.delenv("AIRFLOW_FERNET_KEY", raising=False)
    with pytest.raises(RuntimeError, match="AIRFLOW_FERNET_KEY"):
        encrypt("value")
