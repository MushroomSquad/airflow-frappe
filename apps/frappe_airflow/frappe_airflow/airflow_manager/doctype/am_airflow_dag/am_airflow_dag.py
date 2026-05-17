import json

import frappe

from frappe_airflow.airflow_db.dag_connection_options import build_dag_connection_options
from frappe_airflow.airflow_db.dag_connection_sync import (
    get_selected_connections,
    set_selected_connections,
)
from frappe_airflow.airflow_db.dag_reader import count_dags, get_dag, list_dags, set_dag_paused
from frappe_airflow.doctype_utils import apply_virtual_row
from frappe_airflow.virtual_document import VirtualAirflowDocument


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


def _sync_dag_table_config_variable(dag_id: str) -> None:
    try:
        from frappe_airflow.airflow_db.config_sync import reload_dag_table_config_from_db

        reload_dag_table_config_from_db(dag_id)
    except Exception:
        frappe.log_error(frappe.get_traceback(), "AM Airflow DAG table config sync failed")


def _parse_selected(value) -> list[str]:
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


class AMAirflowDAG(VirtualAirflowDocument):
    def load_from_db(self):
        row = get_dag(self.name)
        if not row:
            frappe.throw(f"DAG {self.name} not found in Airflow")
        apply_virtual_row(self, row)

        _ensure_dag_config(self.name)
        selected = get_selected_connections(self.name)
        self.selected_connections = json.dumps(selected, ensure_ascii=False)

        try:
            options = build_dag_connection_options(self.name)
            self.connection_options = json.dumps(options, ensure_ascii=False)
        except Exception:
            frappe.log_error(frappe.get_traceback(), "AM Airflow DAG connection_options failed")
            self.connection_options = "[]"

    def db_insert(self, *args, **kwargs):
        frappe.throw("AM Airflow DAG cannot be created manually")

    def db_update(self, *args, **kwargs):
        set_dag_paused(self.dag_id, bool(self.is_paused))
        _ensure_dag_config(self.dag_id)
        conn_ids = _parse_selected(self.selected_connections)
        set_selected_connections(self.dag_id, conn_ids)
        _sync_dag_table_config_variable(self.dag_id)

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
