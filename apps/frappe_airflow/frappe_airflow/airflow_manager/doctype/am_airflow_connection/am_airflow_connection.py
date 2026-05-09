"""Virtual DocType - reads and writes directly to Airflow's connection table."""
import frappe
from frappe.model.document import Document

from frappe_airflow.airflow_db.connection_manager import (
    count_connections,
    delete_connection,
    get_connection,
    list_connections,
    upsert_connection,
)
from frappe_airflow.doctype_utils import apply_virtual_row

_PLATFORM_FIELDS = {
    "wb_token": {"api_token": "password"},
    "oz_seller": {"api_token": "password", "client_seller_id": "login"},
    "oz_performance": {"perf_id": "login", "perf_secret": "password"},
    "ms_token": {"ms_token": "password"},
}


def _to_airflow_payload(doc_data: dict) -> dict:
    """Map form fields to Airflow connection columns."""
    conn_type = doc_data.get("conn_type", "other")
    mapping = _PLATFORM_FIELDS.get(conn_type, {})
    payload = {
        "conn_id": doc_data["conn_id"],
        "conn_type": conn_type,
        "description": doc_data.get("description", ""),
    }
    for form_field, airflow_field in mapping.items():
        val = doc_data.get(form_field) or ""
        if airflow_field == "password":
            payload["password"] = val
        elif airflow_field == "login":
            payload["login"] = val
    return payload


def _from_airflow_row(row: dict, conn_type_hint: str = "") -> dict:
    """Map Airflow connection row to form fields."""
    conn_type = row.get("conn_type") or conn_type_hint
    mapping = _PLATFORM_FIELDS.get(conn_type, {})
    doc = {
        "conn_id": row["conn_id"],
        "conn_type": conn_type,
        "description": row.get("description", ""),
    }
    for form_field, airflow_field in mapping.items():
        if airflow_field == "password":
            doc[form_field] = None  # never expose password in form
        elif airflow_field == "login":
            doc[form_field] = row.get("login", "")
    return doc


class AMAirflowConnection(Document):
    def load_from_db(self):
        row = get_connection(self.name)
        if not row:
            frappe.throw(f"Connection {self.name} not found in Airflow")
        apply_virtual_row(self, _from_airflow_row(row))

    def db_insert(self, *args, **kwargs):
        upsert_connection(_to_airflow_payload(self.as_dict()))

    def db_update(self, *args, **kwargs):
        upsert_connection(_to_airflow_payload(self.as_dict()))

    def delete(self):
        delete_connection(self.name)

    @staticmethod
    def get_list(args):
        try:
            limit = args.get("page_length") or 20
            offset = args.get("start") or 0
            return list_connections(limit=limit, offset=offset)
        except Exception:
            return []

    @staticmethod
    def get_count(args):
        try:
            return count_connections()
        except Exception:
            return 0

    @staticmethod
    def get_stats(args):
        return {}
