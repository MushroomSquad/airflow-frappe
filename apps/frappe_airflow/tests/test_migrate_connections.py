import pytest

from frappe_airflow.migrate_connections import _parse_client_registry


def test_parse_client_registry_direct():
    raw = '{"efendem": {"db": "pg_efendem", "wb": {"shop": {"active": true}}}}'
    data = _parse_client_registry(raw)
    assert "efendem" in data
    assert data["efendem"]["db"] == "pg_efendem"


def test_parse_client_registry_import_wrapper():
    raw = '{"CLIENT_REGISTRY": {"efendem": {"db": "pg_efendem"}}}'
    data = _parse_client_registry(raw)
    assert data == {"efendem": {"db": "pg_efendem"}}


def test_parse_client_registry_empty_raises():
    with pytest.raises(ValueError, match="empty"):
        _parse_client_registry("   ")


def test_parse_client_registry_invalid_json_raises():
    with pytest.raises(ValueError, match="not valid JSON"):
        _parse_client_registry("not-json")


def test_resolve_target_db_conn_found():
    from unittest.mock import patch

    from frappe_airflow.migrate_connections import _resolve_target_db_conn

    with patch(
        "frappe_airflow.airflow_db.connection_manager.get_connection",
        return_value={"conn_type": "postgres"},
    ):
        assert _resolve_target_db_conn("09brg_postgres_cred") == "09brg_postgres_cred"


def test_resolve_target_db_conn_missing():
    from unittest.mock import patch

    from frappe_airflow.migrate_connections import _resolve_target_db_conn

    with patch(
        "frappe_airflow.airflow_db.connection_manager.get_connection",
        return_value=None,
    ):
        assert _resolve_target_db_conn("09brg_postgres_cred") == ""
