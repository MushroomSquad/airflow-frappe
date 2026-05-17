import json
from unittest.mock import patch

from frappe_airflow.airflow_db.config_sync import build_dag_table_config, rebuild_dag_table_config

CONFIGS = [
    {
        "dag_id": "wb_orders_etl_dag",
        "scope": "_default",
        "connection": "",
        "table_name": "wb_orders",
        "enabled": True,
        "load_strategy": "append",
        "incremental_days": 15,
        "auto_alter": False,
        "exclude_fields": ["internal_field"],
        "rename_fields": {"old": "new"},
        "targets": [{"db": None, "table": "wb_orders"}],
    },
    {
        "dag_id": "wb_orders_etl_dag",
        "scope": "connection",
        "connection": "wb_api_token_pharm_legend",
        "table_name": "wb_orders",
        "enabled": True,
        "load_strategy": "append",
        "incremental_days": 30,
        "auto_alter": False,
        "exclude_fields": [],
        "rename_fields": {},
        "targets": [],
    },
]


def test_build_config_structure():
    result = build_dag_table_config(CONFIGS)
    assert "_default" in result
    assert "wb_api_token_pharm_legend" in result


def test_build_config_default_table():
    result = build_dag_table_config(CONFIGS)
    default_cfg = result["_default"]["wb_orders"]
    assert default_cfg["incremental_days"] == 15
    assert default_cfg["exclude_fields"] == ["internal_field"]
    assert default_cfg["rename_fields"] == {"old": "new"}


def test_build_config_connection_override():
    result = build_dag_table_config(CONFIGS)
    conn_cfg = result["wb_api_token_pharm_legend"]["wb_orders"]
    assert conn_cfg["incremental_days"] == 30


def test_rebuild_calls_set_variable_with_correct_key():
    with patch("frappe_airflow.airflow_db.config_sync.set_variable") as mock_set:
        rebuild_dag_table_config("wb_orders_etl_dag", CONFIGS)
    mock_set.assert_called_once()
    key = mock_set.call_args[0][0]
    assert key == "dag_table_config_wb_orders_etl_dag"
    val = json.loads(mock_set.call_args[0][1])
    assert "_default" in val
