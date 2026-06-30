import frappe
from frappe.model.document import Document


class AMClient(Document):
    def on_update(self):
        from frappe_airflow.airflow_db.client_directory_sync import rebuild_client_directory

        rebuild_client_directory()

    def on_trash(self):
        from frappe_airflow.airflow_db.client_directory_sync import rebuild_client_directory

        rebuild_client_directory()
