"""One-shot sync of all Airflow Variables from Frappe state."""
from __future__ import annotations

import frappe

from frappe_airflow.airflow_db.config_sync import reload_dag_table_config_from_db
from frappe_airflow.airflow_db.connection_registry_sync import rebuild_connection_registry
from frappe_airflow.airflow_db.dag_registry_sync import rebuild_dag_registry


def run(*, sync_table_configs: bool = True) -> dict:
    """Rebuild CONNECTION_REGISTRY, DAG_REGISTRY, and all dag_table_config_* variables."""
    rebuild_connection_registry()
    rebuild_dag_registry()

    table_dags: list[str] = []
    if sync_table_configs and frappe.db.exists("DocType", "AM Table Config"):
        table_dags = frappe.db.sql_list(
            "SELECT DISTINCT dag_id FROM `tabAM Table Config` WHERE dag_id IS NOT NULL AND dag_id != ''"
        )
        for dag_id in table_dags:
            reload_dag_table_config_from_db(dag_id)

    frappe.db.commit()
    return {
        "connection_registry": True,
        "dag_registry": True,
        "table_config_dags": len(table_dags),
    }
