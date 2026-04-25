import frappe
from frappe.model.document import Document


class AMClient(Document):
    def after_save(self):
        _trigger_registry_sync()

    def on_trash(self):
        _trigger_registry_sync()


def _trigger_registry_sync():
    from airflow_manager.airflow_db.registry_sync import rebuild_client_registry

    clients_raw = frappe.get_all(
        "AM Client",
        fields=["client_id", "display_name", "db_connection"],
    )
    clients = []
    for c in clients_raw:
        cabinets_raw = frappe.get_all(
            "AM Cabinet",
            filters={"client": c["client_id"]},
            fields=["slug", "display_name", "platform", "active", "dags"],
        )
        clients.append(
            {
                "id": c["client_id"],
                "display_name": c["display_name"],
                "db": c.get("db_connection") or "",
                "cabinets": cabinets_raw,
            }
        )
    rebuild_client_registry(clients)
