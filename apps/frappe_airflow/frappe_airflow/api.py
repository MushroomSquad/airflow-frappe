from __future__ import annotations

import frappe
from frappe import _
from frappe.desk.listview import get_group_by_count as core_get_group_by_count


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
