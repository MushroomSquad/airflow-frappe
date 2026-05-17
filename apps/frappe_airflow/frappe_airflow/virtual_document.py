"""Base class for virtual DocTypes backed by Airflow metadata DB."""
from __future__ import annotations

import frappe
from frappe.model.document import Document


class VirtualAirflowDocument(Document):
    """Virtual rows live outside tab{doctype}; skip MariaDB-only document behaviour."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._ensure_runtime_state()

    def _ensure_runtime_state(self) -> None:
        if not getattr(self, "flags", None):
            self.flags = frappe._dict()
        if not hasattr(self, "_action"):
            self._action = "save"

    def check_if_latest(self) -> None:
        return

    def _validate_links(self):
        self._ensure_runtime_state()
        return super()._validate_links()

    def save(self, *args, **kwargs):
        self._ensure_runtime_state()
        return super().save(*args, **kwargs)
