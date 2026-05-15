from __future__ import annotations

import frappe
from frappe import _
from frappe.desk.listview import get_group_by_count as core_get_group_by_count

from frappe_airflow.airflow_db.connection_manager import list_marketplace_connections
from frappe_airflow.airflow_db.connection_meta import unpack_extra
from frappe_airflow.airflow_db.dag_connection_sync import get_selected_connections
from frappe_airflow.airflow_db.dag_platform import (
    CONN_TYPE_BY_PLATFORM,
    build_conn_id,
    conn_matches_dag,
    default_conn_type_for_platform,
    infer_dag_platform,
)


@frappe.whitelist()
def get_group_by_count(doctype: str, current_filters=None, field: str | None = None):
    """Return empty sidebar counts for virtual DocTypes.

    Frappe's core implementation always issues SQL against `tab{doctype}`,
    which breaks for virtual DocTypes backed by external systems.
    """
    if not doctype:
        frappe.throw(_("DocType is required"))

    meta = frappe.get_meta(doctype)
    if getattr(meta, "is_virtual", 0):
        return []

    return core_get_group_by_count(
        doctype=doctype,
        current_filters=current_filters,
        field=field,
    )


@frappe.whitelist()
def get_dag_connection_options(dag_id: str) -> list[dict[str, str]]:
    """Return MultiCheck options for connections compatible with this DAG."""
    platform = infer_dag_platform(dag_id)
    options: list[dict[str, str]] = []

    for row in list_marketplace_connections(limit=500):
        meta = unpack_extra(row.get("extra"))
        if meta.get("is_companion"):
            continue
        conn_type = row.get("conn_type") or ""
        if not conn_matches_dag(conn_type, platform, dag_id):
            continue
        slug = meta.get("slug", "")
        label = row["conn_id"]
        if slug:
            label = f"{row['conn_id']} ({slug})"
        options.append({"label": label, "value": row["conn_id"]})

    return options


@frappe.whitelist()
def preview_conn_id(platform: str = "", conn_type: str = "", slug: str = "") -> str:
    resolved_type = conn_type or default_conn_type_for_platform(platform)
    if not slug or not resolved_type:
        return ""
    return build_conn_id(resolved_type, slug)


@frappe.whitelist()
def get_conn_type_options(platform: str) -> list[str]:
    return list(CONN_TYPE_BY_PLATFORM.get(platform, ("other",)))
