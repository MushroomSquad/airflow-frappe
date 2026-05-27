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


@frappe.whitelist()
def sync_all_to_airflow() -> dict:
    """Rebuild CONNECTION_REGISTRY, DAG_REGISTRY, and all dag_table_config_* in one call."""
    from frappe_airflow.sync_all import run

    return run()


@frappe.whitelist()
def import_airparse_xlsx(file_name: str) -> dict:
    """Import connections and variables from an airparse .xlsx export.

    ``file_name`` is the Frappe File docname (e.g. "export.xlsx" or the full
    docname returned after uploading via Frappe's file manager).  The file must
    already be uploaded to the Frappe site (private or public files).
    """
    from frappe_airflow.importer.airparse_importer import import_from_xlsx

    file_doc = frappe.get_doc("File", {"file_name": file_name})
    file_path = file_doc.get_full_path()
    return import_from_xlsx(file_path)


@frappe.whitelist()
def get_dag_table_configs(dag_id: str) -> list[dict]:
    """List AM Table Config rows for a DAG (for embedded DAG form UI)."""
    if not dag_id:
        return []
    rows = frappe.get_all(
        "AM Table Config",
        filters={"dag_id": dag_id},
        fields=[
            "name",
            "table_name",
            "scope",
            "connection",
            "enabled",
            "load_strategy",
            "incremental_days",
            "auto_alter",
        ],
        order_by="table_name asc",
    )
    for row in rows:
        row["enabled"] = bool(row.get("enabled"))
        row["auto_alter"] = bool(row.get("auto_alter"))
    return rows
