import frappe
from frappe.model.document import Document


class AMCabinet(Document):
    def after_save(self):
        from airflow_manager.airflow_manager.doctype.am_client.am_client import (
            _trigger_registry_sync,
        )

        _trigger_registry_sync()

    def on_trash(self):
        from airflow_manager.airflow_manager.doctype.am_client.am_client import (
            _trigger_registry_sync,
        )

        _trigger_registry_sync()
