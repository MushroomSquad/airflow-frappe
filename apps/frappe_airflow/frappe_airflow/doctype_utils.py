from __future__ import annotations


def apply_virtual_row(doc, values: dict) -> None:
    """Populate a virtual DocType instance during load_from_db.

    `Document.update()` is too early here for Frappe virtual documents and may
    access internal table metadata before it is initialized.
    """
    doc._table_fieldnames = getattr(doc, "_table_fieldnames", {})
    doc.__dict__.update(values)
