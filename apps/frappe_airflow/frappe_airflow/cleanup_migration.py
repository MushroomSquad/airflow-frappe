"""Remove marketplace/connection garbage left by migrate_connections.

Dry-run (default):
  DRY_RUN=1 bench --site SITE execute frappe_airflow.cleanup_migration.run

Apply:
  DRY_RUN=0 bench --site SITE execute frappe_airflow.cleanup_migration.run

Options (env):
  DELETE_EMPTY=1       delete marketplace rows with no password (default on)
  STRIP_EXTRA=1        remove platform/slug/display_name/target_db from extra (default on)
  CLEAR_DAG_CONFIG=1   delete all AM DAG Config rows (default on)
  CLEAR_REGISTRIES=1   delete CONNECTION_REGISTRY and DAG_REGISTRY (default on)
"""
from __future__ import annotations

import json
import os

_META_KEYS = ("platform", "slug", "display_name", "target_db_connection")


def _env_bool(name: str, default: bool = True) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.lower() in ("1", "true", "yes")


def run():
    import frappe

    from frappe_airflow.airflow_db.connection_manager import (
        delete_connection,
        get_connection,
        list_marketplace_connections,
        upsert_connection,
    )
    from frappe_airflow.airflow_db.connection_meta import unpack_extra
    from frappe_airflow.airflow_db.dag_platform import is_companion_conn_id
    from frappe_airflow.airflow_db.variable_manager import delete_variable

    dry_run = _env_bool("DRY_RUN", True)
    delete_empty = _env_bool("DELETE_EMPTY", True)
    strip_extra = _env_bool("STRIP_EXTRA", True)
    clear_dag_config = _env_bool("CLEAR_DAG_CONFIG", True)
    clear_registries = _env_bool("CLEAR_REGISTRIES", True)

    empty_ids: list[str] = []
    strip_ids: list[str] = []

    for row in list_marketplace_connections(limit=5000):
        conn_id = row["conn_id"]
        meta = unpack_extra(row.get("extra"))
        if is_companion_conn_id(conn_id, meta):
            continue

        full = get_connection(conn_id)
        if not full:
            continue

        password = (full.get("password") or "").strip()
        has_migration_meta = any(meta.get(k) for k in _META_KEYS)

        if delete_empty and not password and has_migration_meta:
            empty_ids.append(conn_id)
            continue

        if strip_extra and has_migration_meta and password:
            strip_ids.append(conn_id)

    dag_config_names = frappe.get_all("AM DAG Config", pluck="name") if clear_dag_config else []

    print(f"DRY_RUN={dry_run}")
    print(f"Empty marketplace connections to delete: {len(empty_ids)}")
    for cid in empty_ids[:20]:
        print(f"  - {cid}")
    if len(empty_ids) > 20:
        print(f"  ... and {len(empty_ids) - 20} more")

    print(f"Connections to strip migration extra (keep password): {len(strip_ids)}")
    print(f"AM DAG Config rows to delete: {len(dag_config_names)}")
    if clear_registries:
        print("Variables to delete: CONNECTION_REGISTRY, DAG_REGISTRY")

    if dry_run:
        print("No changes applied (DRY_RUN=1).")
        return

    deleted = 0
    for conn_id in empty_ids:
        delete_connection(conn_id)
        deleted += 1

    stripped = 0
    for conn_id in strip_ids:
        full = get_connection(conn_id)
        if not full:
            continue
        meta = unpack_extra(full.get("extra"))
        for key in _META_KEYS:
            meta.pop(key, None)
        new_extra = json.dumps(meta, ensure_ascii=False) if meta else ""
        upsert_connection(
            {
                "conn_id": conn_id,
                "conn_type": full.get("conn_type", ""),
                "description": full.get("description", ""),
                "host": full.get("host", ""),
                "schema": full.get("schema", ""),
                "login": full.get("login", ""),
                "port": full.get("port"),
                "extra": new_extra,
            }
        )
        stripped += 1

    for name in dag_config_names:
        frappe.delete_doc("AM DAG Config", name, force=True, ignore_permissions=True)

    if clear_registries:
        delete_variable("CONNECTION_REGISTRY")
        delete_variable("DAG_REGISTRY")

    frappe.db.commit()
    print(
        f"Done. Deleted empty: {deleted}, stripped extra: {stripped}, "
        f"DAG configs removed: {len(dag_config_names)}"
    )
