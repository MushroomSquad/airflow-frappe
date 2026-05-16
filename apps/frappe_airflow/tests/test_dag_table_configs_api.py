from unittest.mock import patch

import frappe_airflow.api as api


def test_get_dag_table_configs_empty_without_dag_id():
    assert api.get_dag_table_configs("") == []


def test_get_dag_table_configs_returns_rows():
    rows = [
        {
            "name": "TC-1",
            "table_name": "sales",
            "scope": "_default",
            "connection": "",
            "enabled": 1,
            "load_strategy": "append",
            "incremental_days": 0,
            "auto_alter": 0,
        }
    ]
    with patch.object(api.frappe, "get_all", return_value=rows) as mock_get_all:
        result = api.get_dag_table_configs("wb_sales_etl_dag")
    mock_get_all.assert_called_once()
    assert result[0]["table_name"] == "sales"
    assert result[0]["enabled"] is True
