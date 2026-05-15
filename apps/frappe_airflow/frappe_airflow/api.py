from __future__ import annotations

import json

import frappe
from frappe import _
from frappe.desk.listview import get_group_by_count as core_get_group_by_count

from frappe_airflow.airflow_db.dag_connection_options import build_dag_connection_options
from frappe_airflow.airflow_db.dag_platform import (
    CONN_TYPE_BY_PLATFORM,
    build_conn_id,
    default_conn_type_for_platform,
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
    """Return checkbox options for connections compatible with this DAG."""
    return build_dag_connection_options(dag_id)


@frappe.whitelist()
def prepare_dag_config_form(dag_id: str) -> dict[str, str]:
    """Populate hidden connection_options before rendering MultiCheck."""
    options = build_dag_connection_options(dag_id)
    return {
        "connection_options": json.dumps(options, ensure_ascii=False),
    }


@frappe.whitelist()
def preview_conn_id(platform: str = "", conn_type: str = "", slug: str = "") -> str:
    resolved_type = conn_type or default_conn_type_for_platform(platform)
    if not slug or not resolved_type:
        return ""
    return build_conn_id(resolved_type, slug)


@frappe.whitelist()
def get_conn_type_options(platform: str) -> list[str]:
    return list(CONN_TYPE_BY_PLATFORM.get(platform, ("other",)))
