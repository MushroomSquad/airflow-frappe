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
from frappe_airflow.airflow_db.connection_meta import pack_extra, unpack_extra
from frappe_airflow.airflow_db.connection_sync import (
    remove_companion_connections,
    sync_companion_connections,
)
from frappe_airflow.airflow_db.dag_connection_sync import assign_connection_to_matching_dags
from frappe_airflow.airflow_db.dag_platform import (
    CONN_TYPE_BY_PLATFORM,
    build_conn_id,
    default_conn_type_for_platform,
)
from frappe_airflow.doctype_utils import (
    apply_virtual_row,
    as_link_search_rows,
    extract_search_text,
    is_link_search,
)

_PLATFORM_FIELDS = {
    "wb": {"api_token": "password"},
    "oz_seller": {"api_token": "password", "client_seller_id": "login"},
    "oz_perf": {"perf_id": "login", "perf_secret": "password"},
    "ms": {"ms_token": "password"},
}


def _resolve_conn_type(doc_data: dict) -> str:
    conn_type = doc_data.get("conn_type") or ""
    if conn_type and conn_type != "other":
        return conn_type
    platform = doc_data.get("platform") or ""
    return default_conn_type_for_platform(platform)


def _resolve_conn_id(doc_data: dict) -> str:
    conn_id = (doc_data.get("conn_id") or "").strip()
    if conn_id:
        return conn_id
    slug = (doc_data.get("slug") or "").strip()
    conn_type = _resolve_conn_type(doc_data)
    if slug and conn_type:
        return build_conn_id(conn_type, slug)
    return conn_id


def _to_airflow_payload(doc_data: dict) -> dict:
    conn_type = _resolve_conn_type(doc_data)
    mapping = _PLATFORM_FIELDS.get(conn_type, {})
    payload = {
        "conn_id": _resolve_conn_id(doc_data),
        "conn_type": conn_type,
        "description": doc_data.get("description", ""),
        "extra": pack_extra(
            platform=doc_data.get("platform", ""),
            slug=doc_data.get("slug", ""),
            display_name=doc_data.get("display_name", ""),
        ),
    }
    for form_field, airflow_field in mapping.items():
        val = doc_data.get(form_field) or ""
        if airflow_field == "password":
            payload["password"] = val
        elif airflow_field == "login":
            payload["login"] = val
    return payload


def _from_airflow_row(row: dict) -> dict:
    conn_type = row.get("conn_type") or "other"
    meta = unpack_extra(row.get("extra"))
    mapping = _PLATFORM_FIELDS.get(conn_type, {})
    doc = {
        "conn_id": row["conn_id"],
        "conn_type": conn_type,
        "description": row.get("description", ""),
        "platform": meta.get("platform", ""),
        "slug": meta.get("slug", ""),
        "display_name": meta.get("display_name", ""),
    }
    for form_field, airflow_field in mapping.items():
        if airflow_field == "password":
            doc[form_field] = None
        elif airflow_field == "login":
            doc[form_field] = row.get("login", "")
    return doc


class AMAirflowConnection(Document):
    def validate(self):
        if self.platform and not self.conn_type:
            self.conn_type = default_conn_type_for_platform(self.platform)
        allowed = CONN_TYPE_BY_PLATFORM.get(self.platform or "", ())
        if self.platform and self.conn_type and self.conn_type not in allowed:
            frappe.throw(f"Connection type {self.conn_type} is not valid for {self.platform}")
        if self.slug and not self.conn_id:
            self.conn_id = build_conn_id(_resolve_conn_type(self.as_dict()), self.slug)

    def load_from_db(self):
        row = get_connection(self.name)
        if not row:
            frappe.throw(f"Connection {self.name} not found in Airflow")
        if unpack_extra(row.get("extra")).get("is_companion"):
            frappe.throw(f"Connection {self.name} is a system companion row")
        payload = _from_airflow_row(row)
        apply_virtual_row(self, payload)

    def db_insert(self, *args, **kwargs):
        doc_data = self.as_dict()
        payload = _to_airflow_payload(doc_data)
        upsert_connection(payload)
        sync_companion_connections(doc_data)
        assign_connection_to_matching_dags(payload["conn_id"], payload["conn_type"])

    def db_update(self, *args, **kwargs):
        doc_data = self.as_dict()
        payload = _to_airflow_payload(doc_data)
        upsert_connection(payload)
        sync_companion_connections(doc_data)

    def delete(self):
        row = get_connection(self.name) or {}
        meta = unpack_extra(row.get("extra"))
        slug = meta.get("slug", "") or self.slug or ""
        conn_type = row.get("conn_type") or self.conn_type or ""
        remove_companion_connections(conn_type, slug)
        delete_connection(self.name)

    @staticmethod
    def get_list(args):
        try:
            rows = list_connections(
                search=extract_search_text(args),
                limit=args.get("page_length") or args.get("limit_page_length") or 20,
                offset=args.get("start") or args.get("limit_start") or 0,
            )
            filtered = [
                row for row in rows
                if not unpack_extra(row.get("extra")).get("is_companion")
            ]
            if is_link_search(args):
                return as_link_search_rows(filtered, ("conn_type", "slug", "description"))
            return filtered
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
