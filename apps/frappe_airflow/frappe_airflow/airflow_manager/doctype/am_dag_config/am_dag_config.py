import json

import frappe
from frappe.model.document import Document

from frappe_airflow.airflow_db.dag_connection_sync import _dump_selected


def _parse_multicheck(value) -> list[str]:
    if not value:
        return []
    if isinstance(value, list):
        return [str(item) for item in value if item]
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return []
        if raw.startswith("["):
            try:
                data = json.loads(raw)
                if isinstance(data, list):
                    return [str(item) for item in data if item]
            except json.JSONDecodeError:
                pass
        return [line.strip() for line in raw.splitlines() if line.strip()]
    return []


class AMDAGConfig(Document):
    def validate(self):
        conn_ids = _parse_multicheck(self.selected_connections)
        self.selected_connections = _dump_selected(conn_ids)

    def on_update(self):
        from frappe_airflow.airflow_db.dag_registry_sync import rebuild_dag_registry_entry

        rebuild_dag_registry_entry(self.dag_id)

    def on_trash(self):
        from frappe_airflow.airflow_db.dag_registry_sync import rebuild_dag_registry_entry

        rebuild_dag_registry_entry(self.dag_id)
