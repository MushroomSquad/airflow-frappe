"""One-time migration: CLIENT_REGISTRY -> connections + DAG_REGISTRY.

bench --site SITE execute frappe_airflow.migrate_connections.run
"""
from __future__ import annotations

import json
import os

DRY_RUN = os.environ.get("DRY_RUN", "").lower() in ("1", "true", "yes")


def _platform_conn_field(platform: str) -> str:
    return {
        "wb": "wb_token_connection",
        "oz": "oz_seller_connection",
        "ms": "ms_token_connection",
        "ym": "ym_token_connection",
        "amo": "amocrm_token_connection",
    }.get(platform, "")


def run():
    import frappe

    from frappe_airflow.airflow_db.connection_registry_sync import rebuild_connection_registry
    from frappe_airflow.airflow_db.dag_connection_sync import set_selected_connections
    from frappe_airflow.airflow_db.dag_platform import build_conn_id, conn_matches_dag, default_conn_type_for_platform, infer_dag_platform
    from frappe_airflow.airflow_db.dag_reader import list_dags
    from frappe_airflow.airflow_db.dag_registry_sync import rebuild_dag_registry
    from frappe_airflow.airflow_db.variable_manager import get_variable

    raw = get_variable("CLIENT_REGISTRY")
    if not raw:
        print("CLIENT_REGISTRY is empty or missing — nothing to migrate")
        return

    registry = json.loads(raw)
    created = 0
    dag_updates: dict[str, set[str]] = {}

    for client_id, client in registry.items():
        db_conn = client.get("db") or ""
        for platform in ("wb", "oz", "ms", "ym", "amo"):
            cabinets = client.get(platform) or {}
            if not isinstance(cabinets, dict):
                continue
            for slug, cab in cabinets.items():
                if not cab.get("active", True):
                    continue
                display_name = cab.get("display_name") or slug
                field = _platform_conn_field(platform)
                conn_id = cab.get(field) if field else ""
                if not conn_id:
                    conn_id = build_conn_id(default_conn_type_for_platform(platform), slug)

                if DRY_RUN:
                    print(f"Would upsert {conn_id} db={db_conn} platform={platform}")
                    created += 1
                else:
                    _upsert_frappe_connection(
                        conn_id=conn_id,
                        platform=platform,
                        slug=slug,
                        display_name=display_name,
                        target_db=db_conn,
                    )
                    created += 1

                for dag_id in cab.get("dags") or []:
                    dag_updates.setdefault(dag_id, set()).add(conn_id)
                if not cab.get("dags"):
                    conn_type = default_conn_type_for_platform(platform)
                    for dag in list_dags():
                        dag_id = dag["dag_id"]
                        if infer_dag_platform(dag_id) == platform and conn_matches_dag(
                            conn_type, platform, dag_id, conn_id=conn_id
                        ):
                            dag_updates.setdefault(dag_id, set()).add(conn_id)

    for dag_id, conn_ids in dag_updates.items():
        if DRY_RUN:
            print(f"Would set DAG {dag_id}: {sorted(conn_ids)}")
        else:
            set_selected_connections(dag_id, sorted(conn_ids))

    if not DRY_RUN:
        rebuild_connection_registry()
        rebuild_dag_registry()
        frappe.db.commit()

    print(f"Done. Connections: {created}, DAGs: {len(dag_updates)}")


def _upsert_frappe_connection(
    conn_id: str,
    platform: str,
    slug: str,
    display_name: str,
    target_db: str,
) -> None:
    import frappe

    from frappe_airflow.airflow_db.dag_platform import default_conn_type_for_platform

    conn_type = default_conn_type_for_platform(platform)
    if frappe.db.exists("AM Airflow Connection", conn_id):
        doc = frappe.get_doc("AM Airflow Connection", conn_id)
    else:
        doc = frappe.new_doc("AM Airflow Connection")
        doc.conn_id = conn_id

    doc.platform = platform
    doc.slug = slug
    doc.display_name = display_name
    doc.conn_type = conn_type
    doc.target_db_connection = target_db
    doc.save(ignore_permissions=True)
