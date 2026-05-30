"""Delete marketplace Airflow connections with Frappe/Airflow cascade cleanup."""
from __future__ import annotations

import frappe

from frappe_airflow.airflow_db.connection_manager import delete_connection, get_connection
from frappe_airflow.airflow_db.connection_meta import unpack_extra
from frappe_airflow.airflow_db.connection_registry_sync import remove_connection_registry_entry
from frappe_airflow.airflow_db.connection_sync import remove_companion_connections
from frappe_airflow.airflow_db.dag_connection_sync import (
    get_selected_connections,
    set_selected_connections,
)

BULK_DELETE_LIMIT = 500


def remove_connection_from_all_dag_configs(conn_id: str) -> None:
    """Remove conn_id from every AM DAG Config and sync DAG_REGISTRY."""
    if not frappe.db.exists("DocType", "AM DAG Config"):
        return

    for row in frappe.get_all("AM DAG Config", fields=["dag_id"]):
        dag_id = row["dag_id"]
        selected = get_selected_connections(dag_id)
        if conn_id not in selected:
            continue
        set_selected_connections(dag_id, [c for c in selected if c != conn_id])


def remove_connection_table_configs(conn_id: str) -> list[str]:
    """Delete AM Table Config rows scoped to this connection; return affected dag_ids."""
    if not frappe.db.exists("DocType", "AM Table Config"):
        return []

    rows = frappe.get_all(
        "AM Table Config",
        filters={"connection": conn_id},
        fields=["name", "dag_id"],
    )
    dag_ids = sorted({r["dag_id"] for r in rows if r.get("dag_id")})
    for row in rows:
        frappe.delete_doc("AM Table Config", row["name"], force=True, ignore_permissions=True)
    return dag_ids


def reload_dag_table_configs_for_dags(dag_ids: list[str]) -> None:
    if not dag_ids:
        return
    from frappe_airflow.airflow_db.config_sync import reload_dag_table_config_from_db

    for dag_id in dag_ids:
        reload_dag_table_config_from_db(dag_id)


def delete_marketplace_connection(conn_id: str) -> None:
    """Delete connection from Airflow PG and clean up all Frappe/Airflow references."""
    conn_id = (conn_id or "").strip()
    if not conn_id:
        frappe.throw("Connection ID is required")

    row = get_connection(conn_id) or {}
    meta = unpack_extra(row.get("extra"))
    slug = meta.get("slug", "") or ""
    conn_type = row.get("conn_type") or ""

    remove_connection_from_all_dag_configs(conn_id)

    if frappe.db.exists("DocType", "AM DAG Connection"):
        frappe.db.delete("AM DAG Connection", {"connection": conn_id})

    affected_dags = remove_connection_table_configs(conn_id)
    reload_dag_table_configs_for_dags(affected_dags)

    remove_companion_connections(conn_type, slug)
    delete_connection(conn_id)
    remove_connection_registry_entry(conn_id)

    frappe.db.commit()
