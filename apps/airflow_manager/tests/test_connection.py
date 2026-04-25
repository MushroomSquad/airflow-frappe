import pytest


def test_missing_url_raises(monkeypatch):
    import airflow_manager.airflow_db.connection as conn_module

    monkeypatch.delenv("AIRFLOW_DB_URL", raising=False)
    conn_module._engine = None  # reset singleton
    with pytest.raises(RuntimeError, match="AIRFLOW_DB_URL"):
        conn_module._get_engine()
    conn_module._engine = None


def test_check_connection_returns_false_on_bad_url(monkeypatch):
    import airflow_manager.airflow_db.connection as conn_module

    monkeypatch.setenv(
        "AIRFLOW_DB_URL", "postgresql+psycopg2://bad:bad@localhost:9999/noexist"
    )
    conn_module._engine = None
    result = conn_module.check_connection()
    assert result is False
    conn_module._engine = None
