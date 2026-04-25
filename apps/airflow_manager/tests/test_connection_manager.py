from unittest.mock import MagicMock, patch

import pytest
from cryptography.fernet import Fernet

_KEY = Fernet.generate_key().decode()


@pytest.fixture(autouse=True)
def set_env(monkeypatch):
    monkeypatch.setenv("AIRFLOW_FERNET_KEY", _KEY)
    monkeypatch.setenv("AIRFLOW_DB_URL", "postgresql+psycopg2://x:x@localhost/x")


def _mock_session(rows=None, scalar=None):
    """Return a mock context manager that yields a mock session."""
    session = MagicMock()
    session.execute.return_value.fetchall.return_value = rows or []
    session.execute.return_value.fetchone.return_value = None
    session.execute.return_value.scalar.return_value = scalar or 0
    cm = MagicMock()
    cm.__enter__ = MagicMock(return_value=session)
    cm.__exit__ = MagicMock(return_value=False)
    return cm, session


def test_list_connections_returns_list():
    cm, session = _mock_session(rows=[])
    with patch("airflow_manager.airflow_db.connection_manager.get_session", return_value=cm):
        from airflow_manager.airflow_db.connection_manager import list_connections

        result = list_connections()
    assert isinstance(result, list)


def test_upsert_encrypts_password():
    cm, session = _mock_session()
    session.execute.return_value.fetchone.return_value = None  # no existing row
    with patch("airflow_manager.airflow_db.connection_manager.get_session", return_value=cm):
        from airflow_manager.airflow_db.connection_manager import upsert_connection

        upsert_connection({"conn_id": "test_conn", "password": "secret"})
    # Find the INSERT call and verify password is encrypted
    all_calls = session.execute.call_args_list
    # text() objects don't show SQL in str(call) — extract via .text attribute
    insert_call = [c for c in all_calls if "INSERT" in getattr(c[0][0], "text", "")]
    assert insert_call, "No INSERT call found"
    params = insert_call[0][0][1]
    assert params["password"] != "secret"
    assert params["is_encrypted"] is True
    # Verify it decrypts correctly
    from airflow_manager.airflow_db.fernet import decrypt

    assert decrypt(params["password"]) == "secret"


def test_upsert_preserves_existing_password_when_blank():
    existing = MagicMock()
    existing.password = "existing_encrypted"
    existing.is_encrypted = True
    cm, session = _mock_session()
    session.execute.return_value.fetchone.return_value = existing
    with patch("airflow_manager.airflow_db.connection_manager.get_session", return_value=cm):
        from airflow_manager.airflow_db.connection_manager import upsert_connection

        upsert_connection({"conn_id": "test_conn", "password": ""})
    update_call = [c for c in session.execute.call_args_list if "UPDATE" in getattr(c[0][0], "text", "")]
    assert update_call
    params = update_call[0][0][1]
    assert params["password"] == "existing_encrypted"


def test_get_connection_decrypts_password():
    from airflow_manager.airflow_db.fernet import encrypt

    encrypted_pwd = encrypt("real-secret")
    row = MagicMock()
    row.conn_id = "test"
    row.conn_type = "generic"
    row.description = ""
    row.host = ""
    row.schema = ""
    row.login = ""
    row.password = encrypted_pwd
    row.port = None
    row.is_encrypted = True
    row.extra = ""
    cm, session = _mock_session()
    session.execute.return_value.fetchone.return_value = row
    with patch("airflow_manager.airflow_db.connection_manager.get_session", return_value=cm):
        from airflow_manager.airflow_db.connection_manager import get_connection

        result = get_connection("test")
    assert result["password"] == "real-secret"
