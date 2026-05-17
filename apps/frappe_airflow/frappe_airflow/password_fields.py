"""Helpers for Frappe Password fields on virtual Airflow DocTypes."""
from __future__ import annotations


def normalize_submitted_password(value: str | None) -> str:
    """Return a password to store, or '' when the user did not submit a new value."""
    text = (value or "").strip()
    if not text:
        return ""
    # Desk placeholder when the field is left unchanged.
    if not text.replace("*", "").replace("•", ""):
        return ""
    return text


def read_password_from_doc(doc, fieldname: str) -> str:
    """Read a Password field from a Document; ``as_dict()`` omits secrets."""
    if hasattr(doc, "get_password"):
        raw = doc.get_password(fieldname, raise_exception=False) or ""
    elif hasattr(doc, "get"):
        raw = doc.get(fieldname) or ""
    else:
        raw = doc.get(fieldname, "") if isinstance(doc, dict) else ""
    return normalize_submitted_password(raw)
