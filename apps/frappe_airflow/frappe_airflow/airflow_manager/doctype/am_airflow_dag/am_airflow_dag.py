import json

import frappe
from frappe import _
from frappe.model.document import Document

from frappe_airflow.airflow_db.dag_connection_options import build_dag_connection_options
from frappe_airflow.airflow_db.dag_connection_sync import (
    get_selected_connections,
    set_selected_connections,
)
from frappe_airflow.airflow_db.dag_reader import count_dags, get_dag, list_dags, set_dag_paused
from frappe_airflow.doctype_utils import apply_virtual_row


def _ensure_dag_config(dag_id: str) -> None:
    if frappe.db.exists("AM DAG Config", dag_id):
        return
    frappe.get_doc(
        {
            "doctype": "AM DAG Config",
            "dag_id": dag_id,
            "selected_connections": "[]",
        }
    ).insert(ignore_permissions=True)
    frappe.db.commit()


def _connections_summary(dag_id: str) -> str:
    selected = get_selected_connections(dag_id)
    if not selected:
        return _("No connections selected. Open Configure Connections.")
    return ", ".join(selected)


def _load_dag_config(dag_id: str) -> dict:
    if not frappe.db.exists("AM DAG Config", dag_id):
        return {}
    config = frappe.get_doc("AM DAG Config", dag_id)
    return {
        "db_connection": config.db_connection or "",
    }


def _save_dag_config_db(dag_id: str, db_connection: str) -> None:
    _ensure_dag_config(dag_id)
    set_selected_connections(
        dag_id,
        get_selected_connections(dag_id),
        db_connection=db_connection or "",
    )


class AMAirflowDAG(Document):
    def load_from_db(self):
        row = get_dag(self.name)
        if not row:
            frappe.throw(f"DAG {self.name} not found in Airflow")
        apply_virtual_row(self, row)

        _ensure_dag_config(self.name)
        self.dag_config = self.name
        self.connections_summary = _connections_summary(self.name)

        config = _load_dag_config(self.name)
        self.db_connection = config.get("db_connection", "")

        try:
            build_dag_connection_options(self.name)
        except Exception:
            frappe.log_error(frappe.get_traceback(), "AM Airflow DAG connection options failed")

    def db_insert(self, *args, **kwargs):
        frappe.throw("AM Airflow DAG cannot be created manually")

    def db_update(self, *args, **kwargs):
        set_dag_paused(self.dag_id, bool(self.is_paused))
        _save_dag_config_db(self.dag_id, self.db_connection or "")

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
