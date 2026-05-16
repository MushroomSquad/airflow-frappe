from frappe_airflow.airflow_db.config_sync import build_dag_table_config


def test_build_dag_table_config_default_and_connection():
    configs = [
        {
            "dag_id": "wb_orders_etl_dag",
            "scope": "_default",
            "connection": "",
            "table_name": "wb_orders",
            "enabled": True,
            "load_strategy": "append",
            "incremental_days": None,
            "auto_alter": False,
            "exclude_fields": [],
            "rename_fields": {},
            "targets": [],
        },
        {
            "dag_id": "wb_orders_etl_dag",
            "scope": "connection",
            "connection": "wb_api_token_filippov",
            "table_name": "wb_orders",
            "enabled": True,
            "load_strategy": "delete_and_append",
            "incremental_days": 7,
            "auto_alter": True,
            "exclude_fields": ["x"],
            "rename_fields": {},
            "targets": [],
        },
    ]
    built = build_dag_table_config(configs)
    assert built["_default"]["wb_orders"]["load_strategy"] == "append"
    assert built["wb_api_token_filippov"]["wb_orders"]["load_strategy"] == "delete_and_append"
    assert built["wb_api_token_filippov"]["wb_orders"]["incremental_days"] == 7
