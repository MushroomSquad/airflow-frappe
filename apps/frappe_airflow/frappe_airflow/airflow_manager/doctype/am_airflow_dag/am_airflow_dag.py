import json

import frappe
from frappe.model.document import Document

from frappe_airflow.airflow_db.dag_connection_sync import (
    get_selected_connections,
    set_selected_connections,
)
from frappe_airflow.airflow_db.dag_reader import count_dags, get_dag, list_dags, set_dag_paused
from frappe_airflow.doctype_utils import apply_virtual_row


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


def _dump_multicheck(conn_ids: list[str]) -> str:
    return json.dumps(conn_ids, ensure_ascii=False)


def _load_dag_config(dag_id: str) -> dict:
    if not frappe.db.exists("AM DAG Config", dag_id):
        return {}
    config = frappe.get_doc("AM DAG Config", dag_id)
    selected = get_selected_connections(dag_id)
    if not selected and config.connections:
        selected = [row.connection for row in config.connections if row.connection]
    return {
        "db_connection": config.db_connection or "",
        "selected_connections": _dump_multicheck(selected),
    }


def _save_dag_config(dag_id: str, db_connection: str, selected_connections) -> None:
    conn_ids = _parse_multicheck(selected_connections)
    set_selected_connections(dag_id, conn_ids, db_connection=db_connection or "")


class AMAirflowDAG(Document):
    def load_from_db(self):
        row = get_dag(self.name)
        if not row:
            frappe.throw(f"DAG {self.name} not found in Airflow")
        apply_virtual_row(self, row)
        config = _load_dag_config(self.name)
        if config:
            self.db_connection = config.get("db_connection", "")
            self.selected_connections = config.get("selected_connections", "[]")

    def db_insert(self, *args, **kwargs):
        frappe.throw("AM Airflow DAG cannot be created manually")

    def db_update(self, *args, **kwargs):
        set_dag_paused(self.dag_id, bool(self.is_paused))
        _save_dag_config(self.dag_id, self.db_connection or "", self.selected_connections)

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
