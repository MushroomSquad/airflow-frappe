import json
from unittest.mock import patch

from airflow_manager.airflow_db.config_sync import build_client_config, rebuild_client_config

CONFIGS = [
    {
        "dag_id": "wb_orders_etl_dag",
        "scope": "_default",
        "cabinet_slug": "",
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
        "scope": "cabinet",
        "cabinet_slug": "pharm_legend",
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
    result = build_client_config(CONFIGS)
    assert "wb_orders_etl_dag" in result
    dag = result["wb_orders_etl_dag"]
    assert "_default" in dag
    assert "pharm_legend" in dag


def test_build_config_default_table():
    result = build_client_config(CONFIGS)
    default_cfg = result["wb_orders_etl_dag"]["_default"]["wb_orders"]
    assert default_cfg["incremental_days"] == 15
    assert default_cfg["exclude_fields"] == ["internal_field"]
    assert default_cfg["rename_fields"] == {"old": "new"}


def test_build_config_cabinet_override():
    result = build_client_config(CONFIGS)
    cab_cfg = result["wb_orders_etl_dag"]["pharm_legend"]["wb_orders"]
    assert cab_cfg["incremental_days"] == 30


def test_rebuild_calls_set_variable_with_correct_key():
    with patch("airflow_manager.airflow_db.config_sync.set_variable") as mock_set:
        rebuild_client_config("efendem", CONFIGS)
    mock_set.assert_called_once()
    key = mock_set.call_args[0][0]
    assert key == "client_config_efendem"
    val = json.loads(mock_set.call_args[0][1])
    assert "wb_orders_etl_dag" in val
