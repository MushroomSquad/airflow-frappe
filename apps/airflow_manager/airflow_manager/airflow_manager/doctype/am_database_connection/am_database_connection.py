import frappe
from frappe.model.document import Document

from airflow_manager.airflow_db.connection_manager import (
    count_connections,
    delete_connection,
    get_connection,
    list_connections,
    upsert_connection,
)


class AMDatabaseConnection(Document):
    def load_from_db(self):
        row = get_connection(self.name)
        if not row:
            frappe.throw(f"Connection {self.name} not found in Airflow")
        self.update(
            {
                "conn_id": row["conn_id"],
                "host": row.get("host", ""),
                "port": row.get("port"),
                "schema": row.get("schema", ""),
                "login": row.get("login", ""),
                "password": None,
                "description": row.get("description", ""),
            }
        )

    def db_insert(self, *args, **kwargs):
        upsert_connection(
            {
                "conn_id": self.conn_id,
                "conn_type": "postgres",
                "host": self.host or "",
                "port": self.port,
                "schema": self.schema or "",
                "login": self.login or "",
                "password": self.get_password("password", raise_exception=False) or "",
                "description": self.description or "",
            }
        )

    def db_update(self, *args, **kwargs):
        self.db_insert()

    def delete(self):
        delete_connection(self.name)

    @staticmethod
    def get_list(args):
        return list_connections(
            conn_type="postgres",
            limit=args.get("page_length") or 20,
            offset=args.get("start") or 0,
        )

    @staticmethod
    def get_count(args):
        return count_connections(conn_type="postgres")

    @staticmethod
    def get_stats(args):
        return {}
