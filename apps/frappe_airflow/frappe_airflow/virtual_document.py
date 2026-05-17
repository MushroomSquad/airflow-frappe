"""Base class for virtual DocTypes backed by Airflow metadata DB."""
from __future__ import annotations

from frappe.model.document import Document


class VirtualAirflowDocument(Document):
    """Skip MariaDB row versioning — records are not stored in tab{doctype}."""

    def check_if_latest(self) -> None:
        return
