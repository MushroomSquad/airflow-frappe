from __future__ import annotations


def apply_virtual_row(doc, values: dict) -> None:
    """Populate a virtual DocType instance during load_from_db.

    `Document.update()` is too early here for Frappe virtual documents and may
    access internal table metadata before it is initialized.
    """
    doc._table_fieldnames = getattr(doc, "_table_fieldnames", {})
    doc.__dict__.update(values)


def is_link_search(args: dict | None) -> bool:
    """True when Frappe desk is resolving options for a Link field."""
    args = args or {}
    if args.get("as_list"):
        return True
    if args.get("reference_doctype"):
        return True

    for item in args.get("or_filters") or []:
        if not isinstance(item, (list, tuple)) or len(item) < 4:
            continue
        fieldname = item[1]
        operator = str(item[2]).lower()
        if operator == "like" and fieldname in ("name", "conn_id"):
            return True
    return False


def extract_search_text(args: dict | None) -> str | None:
    args = args or {}
    for key in ("txt", "search"):
        value = args.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()

    for filter_group in ("filters", "or_filters"):
        for item in args.get(filter_group) or []:
            if not isinstance(item, (list, tuple)) or len(item) < 4:
                continue
            operator = str(item[2]).lower()
            value = item[3]
            if operator == "like" and isinstance(value, str):
                value = value.strip("%").strip()
                if value:
                    return value
    return None


def as_link_search_rows(rows: list[dict], description_fields: tuple[str, ...]) -> list[tuple]:
    """Format virtual rows for frappe.desk.search.search_link."""
    results = []
    for row in rows:
        description = ", ".join(
            part for field in description_fields if (part := (row.get(field) or "").strip())
        )
        # search_link adds a relevance column internally and strips the last value.
        results.append((row["name"], description, 1))
    return results
