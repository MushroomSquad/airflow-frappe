from unittest.mock import patch

from frappe_airflow.sync_all import run


def test_run_rebuilds_registries_and_table_configs():
    with patch("frappe_airflow.sync_all.rebuild_connection_registry") as conn_mock, patch(
        "frappe_airflow.sync_all.rebuild_dag_registry"
    ) as dag_mock, patch("frappe_airflow.sync_all.reload_dag_table_config_from_db") as table_mock, patch(
        "frappe_airflow.sync_all.frappe"
    ) as frappe_mock:
        frappe_mock.db.exists.return_value = True
        frappe_mock.db.sql_list.return_value = ["wb_sales_etl_dag", "wb_orders_etl_dag"]
        result = run()

    conn_mock.assert_called_once()
    dag_mock.assert_called_once()
    assert table_mock.call_count == 2
    assert result["table_config_dags"] == 2
