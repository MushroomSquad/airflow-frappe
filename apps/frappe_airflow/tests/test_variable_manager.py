from unittest.mock import MagicMock, patch

import pytest


@pytest.fixture(autouse=True)
def set_env(monkeypatch):
    monkeypatch.setenv("AIRFLOW_DB_URL", "postgresql+psycopg2://x:x@localhost/x")


def _mock_session(fetchone_return=None, fetchall_return=None):
    session = MagicMock()
    session.execute.return_value.fetchone.return_value = fetchone_return
    session.execute.return_value.fetchall.return_value = fetchall_return or []
    cm = MagicMock()
    cm.__enter__ = MagicMock(return_value=session)
    cm.__exit__ = MagicMock(return_value=False)
    return cm, session


def test_get_variable_returns_value():
    row = MagicMock()
    row.val = '{"key": "value"}'
    cm, _ = _mock_session(fetchone_return=row)
    with patch("frappe_airflow.airflow_db.variable_manager.get_session", return_value=cm):
        from frappe_airflow.airflow_db.variable_manager import get_variable

        assert get_variable("MY_VAR") == '{"key": "value"}'


def test_get_variable_returns_none_when_missing():
    cm, _ = _mock_session(fetchone_return=None)
    with patch("frappe_airflow.airflow_db.variable_manager.get_session", return_value=cm):
        from frappe_airflow.airflow_db.variable_manager import get_variable

        assert get_variable("MISSING") is None


def test_set_variable_inserts_when_new():
    cm, session = _mock_session(fetchone_return=None)
    with patch("frappe_airflow.airflow_db.variable_manager.get_session", return_value=cm):
        from frappe_airflow.airflow_db.variable_manager import set_variable

        set_variable("NEW_VAR", '{"a": 1}')
    calls = [getattr(c[0][0], "text", "") for c in session.execute.call_args_list]
    assert any("INSERT" in c for c in calls)


def test_set_variable_updates_when_existing():
    existing = MagicMock()
    existing.key = "EXISTING"
    cm, session = _mock_session(fetchone_return=existing)
    with patch("frappe_airflow.airflow_db.variable_manager.get_session", return_value=cm):
        from frappe_airflow.airflow_db.variable_manager import set_variable

        set_variable("EXISTING", '{"b": 2}')
    calls = [getattr(c[0][0], "text", "") for c in session.execute.call_args_list]
    assert any("UPDATE" in c for c in calls)
