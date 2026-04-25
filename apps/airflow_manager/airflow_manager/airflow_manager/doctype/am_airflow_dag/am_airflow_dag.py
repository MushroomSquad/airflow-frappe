import frappe
from frappe.model.document import Document

from airflow_manager.airflow_db.dag_reader import count_dags, get_dag, list_dags


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
        return list_dags()

    @staticmethod
    def get_count(args):
        return count_dags()

    @staticmethod
    def get_stats(args):
        return {}
