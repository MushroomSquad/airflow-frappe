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
