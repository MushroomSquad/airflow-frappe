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
        if not hasattr(self, "_doc_before_save"):
            self._doc_before_save = None

    def save(self, *args, **kwargs):
        self._ensure_runtime_state()
        if not self.is_new() and self._doc_before_save is None:
            self.load_doc_before_save()
        return super().save(*args, **kwargs)

    def check_if_latest(self) -> None:
        """Load previous row for Frappe validations without MariaDB modified lock.

        Frappe always validates set-once fields (``creation``, ``owner``) on update.
        That requires ``_doc_before_save``, normally filled in ``check_if_latest``.
        Virtual rows do not store ``modified`` in MariaDB, so timestamp comparison
        would false-positive on every desk save.
        """
        self.load_doc_before_save(raise_exception=True)
        self._action = "save"
        previous = self._doc_before_save
        if not previous:
            self.check_docstatus_transition(0)
            return
        if not self.meta.issingle:
            self.check_docstatus_transition(previous.docstatus)

    def load_doc_before_save(self, *, raise_exception: bool = False):
        """Load the current backend row; virtual DocTypes are not in ``tab*`` tables."""
        self._doc_before_save = None
        if self.is_new():
            return
        try:
            # No row lock: data lives in Airflow metadata, not site MariaDB.
            self._doc_before_save = frappe.get_doc(self.doctype, self.name)
        except frappe.DoesNotExistError:
            if raise_exception:
                raise
            frappe.clear_last_message()
