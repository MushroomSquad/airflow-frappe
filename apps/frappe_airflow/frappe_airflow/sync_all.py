"""One-shot sync of all Airflow Variables from Frappe state."""
from __future__ import annotations

import frappe

from frappe_airflow.airflow_db.config_sync import reload_dag_table_config_from_db
from frappe_airflow.airflow_db.connection_registry_sync import rebuild_connection_registry
from frappe_airflow.airflow_db.dag_registry_sync import rebuild_dag_registry


def run(*, sync_table_configs: bool = True) -> dict:
    """Rebuild CONNECTION_REGISTRY, DAG_REGISTRY, and all dag_table_config_* variables.

    Also purges:
    - Orphan AM DAG Config records whose dag_id no longer exists in Airflow.
    - Stale dag_table_config_* Airflow Variables for DAGs not in Airflow and
      not referenced by any AM Table Config row.
    """
    rebuild_connection_registry()
    rebuild_dag_registry()

    table_dags: list[str] = []
    if sync_table_configs and frappe.db.exists("DocType", "AM Table Config"):
        table_dags = frappe.db.sql_list(
            "SELECT DISTINCT dag_id FROM `tabAM Table Config`"
            " WHERE dag_id IS NOT NULL AND dag_id != ''"
        )
        for dag_id in table_dags:
            reload_dag_table_config_from_db(dag_id)

    purged = _purge_stale_dag_state(set(table_dags))

    frappe.db.commit()
    return {
        "connection_registry": True,
        "dag_registry": True,
        "table_config_dags": len(table_dags),
        "stale_purged": purged,
    }


def _purge_stale_dag_state(table_config_dag_ids: set[str]) -> int:
    """Remove AM DAG Config records and dag_table_config_* variables for gone DAGs.

    - AM DAG Config orphan: dag_id exists in Frappe but not in Airflow's dag table.
    - Stale variable: dag_table_config_{x} where x is not in Airflow AND has no
      AM Table Config rows (so we don't wipe a variable we just rebuilt).

    Returns count of purged items. Runs best-effort — any error aborts silently.
    """
    try:
        from frappe_airflow.airflow_db.dag_reader import list_dags
        from frappe_airflow.airflow_db.variable_manager import delete_variable, list_variables

        live_dag_ids = {d["dag_id"] for d in list_dags()}
    except Exception:
        return 0  # Airflow unreachable — skip purge entirely

    purge_count = 0

    # ── Orphan AM DAG Config records ────────────────────────────────────────
    for row in frappe.get_all("AM DAG Config", fields=["dag_id"]):
        dag_id = row["dag_id"]
        if dag_id not in live_dag_ids:
            try:
                frappe.delete_doc(
                    "AM DAG Config", dag_id, force=True, ignore_permissions=True
                )
                purge_count += 1
            except Exception:
                pass

    # ── Stale dag_table_config_* Variables ──────────────────────────────────
    prefix = "dag_table_config_"
    for var in list_variables():
        key = var["key"]
        if not key.startswith(prefix):
            continue
        dag_id = key[len(prefix):]
        # Only delete if: not a live Airflow DAG AND not referenced by AM Table Config
        if dag_id not in live_dag_ids and dag_id not in table_config_dag_ids:
            try:
                delete_variable(key)
                purge_count += 1
            except Exception:
                pass

    return purge_count
