import frappe
from frappe.model.document import Document

from frappe_airflow.airflow_db.dag_reader import count_dags, get_dag, list_dags


class AMAirflowDAG(Document):
    def load_from_db(self):
        row = get_dag(self.name)
        if not row:
            frappe.throw(f"DAG {self.name} not found in Airflow")
        self.update(row)

    def db_insert(self, *args, **kwargs):
        frappe.throw("AM Airflow DAG is read-only")

    def db_update(self, *args, **kwargs):
        frappe.throw("AM Airflow DAG is read-only")

    def delete(self):
        frappe.throw("AM Airflow DAG is read-only")

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
