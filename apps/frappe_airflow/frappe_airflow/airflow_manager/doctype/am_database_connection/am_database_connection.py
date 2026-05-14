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


def _extract_search(args: dict) -> str | None:
    for key in ("txt", "search"):
        value = (args or {}).get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()

    for filter_group in ("filters", "or_filters"):
        for item in (args or {}).get(filter_group) or []:
            if not isinstance(item, (list, tuple)) or len(item) < 4:
                continue
            operator = str(item[2]).lower()
            value = item[3]
            if operator == "like" and isinstance(value, str):
                value = value.strip("%").strip()
                if value:
                    return value
    return None


def _as_link_results(rows: list[dict]) -> list[tuple]:
    results = []
    for row in rows:
        description = ", ".join(
            part
            for part in (
                row.get("host", ""),
                row.get("schema", ""),
                row.get("description", ""),
            )
            if part
        )
        # Frappe search_link strips the last tuple item as an internal relevance column.
        results.append((row["name"], description, 1))
    return results


class AMDatabaseConnection(Document):
    def load_from_db(self):
        row = get_connection(self.name)
        if not row:
            frappe.throw(f"Connection {self.name} not found in Airflow")
        apply_virtual_row(
            self,
            {
                "conn_id": row["conn_id"],
                "host": row.get("host", ""),
                "port": row.get("port"),
                "schema": row.get("schema", ""),
                "login": row.get("login", ""),
                "password": None,
                "description": row.get("description", ""),
            },
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
        try:
            rows = list_connections(
                conn_type="postgres",
                search=_extract_search(args),
                limit=(args.get("page_length") or args.get("limit_page_length") or 20),
                offset=(args.get("start") or args.get("limit_start") or 0),
            )
            if args.get("as_list"):
                return _as_link_results(rows)
            return rows
        except Exception:
            return []

    @staticmethod
    def get_count(args):
        try:
            return count_connections(conn_type="postgres")
        except Exception:
            return 0

    @staticmethod
    def get_stats(args):
        return {}
