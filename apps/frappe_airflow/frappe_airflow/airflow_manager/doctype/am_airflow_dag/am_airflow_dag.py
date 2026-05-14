import frappe
from frappe.model.document import Document

from frappe_airflow.airflow_db.dag_reader import count_dags, get_dag, list_dags, set_dag_paused
from frappe_airflow.doctype_utils import apply_virtual_row


def _load_dag_config(dag_id: str) -> dict:
    """Load config from AM DAG Config if it exists."""
    if not frappe.db.exists("AM DAG Config", dag_id):
        return {}
    config = frappe.get_doc("AM DAG Config", dag_id)
    return {
        "db_connection": config.db_connection or "",
        "connections": [{"connection": row.connection} for row in config.connections],
    }


def _save_dag_config(dag_id: str, db_connection: str, connections: list) -> None:
    """Upsert AM DAG Config in MariaDB."""
    if frappe.db.exists("AM DAG Config", dag_id):
        config = frappe.get_doc("AM DAG Config", dag_id)
        config.db_connection = db_connection
        config.set("connections", connections)
        config.save(ignore_permissions=True)
    else:
        config = frappe.get_doc({
            "doctype": "AM DAG Config",
            "dag_id": dag_id,
            "db_connection": db_connection,
            "connections": connections,
        })
        config.insert(ignore_permissions=True)


class AMAirflowDAG(Document):
    def load_from_db(self):
        row = get_dag(self.name)
        if not row:
            frappe.throw(f"DAG {self.name} not found in Airflow")
        apply_virtual_row(self, row)
        config = _load_dag_config(self.name)
        if config:
            self.db_connection = config.get("db_connection", "")
            self.set("connections", config.get("connections", []))

    def db_insert(self, *args, **kwargs):
        frappe.throw("AM Airflow DAG cannot be created manually")

    def db_update(self, *args, **kwargs):
        set_dag_paused(self.dag_id, bool(self.is_paused))
        connections = [{"connection": row.connection} for row in (self.connections or [])]
        _save_dag_config(self.dag_id, self.db_connection or "", connections)

    def delete(self):
        frappe.throw("AM Airflow DAG cannot be deleted from here")

    @staticmethod
    def get_list(args):
        try:
            return list_dags()
        except Exception:
            frappe.log_error(frappe.get_traceback(), "AM Airflow DAG get_list failed")
            return []

    @staticmethod
    def get_count(args):
        try:
            return count_dags()
        except Exception:
            frappe.log_error(frappe.get_traceback(), "AM Airflow DAG get_count failed")
            return 0

    @staticmethod
    def get_stats(args):
        return {}
