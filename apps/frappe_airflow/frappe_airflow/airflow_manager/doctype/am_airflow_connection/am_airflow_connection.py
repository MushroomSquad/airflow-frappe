"""Virtual DocType - reads and writes directly to Airflow's connection table."""
import frappe
from frappe import _
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
    infer_connection_profile,
)
from frappe_airflow.doctype_utils import (
    apply_virtual_row,
    as_link_search_rows,
    extract_search_text,
    is_link_search,
)
from frappe_airflow.virtual_document import VirtualAirflowDocument

# form_field -> airflow connection column
_PLATFORM_FIELDS: dict[str, dict[str, str]] = {
    "wb": {"api_token": "password"},
    "oz_seller": {"api_token": "password", "client_seller_id": "login"},
    "oz_perf": {"perf_id": "login", "perf_secret": "password"},
    "ms": {"ms_token": "password"},
    "ym": {"api_token": "password"},
    "amo": {"api_token": "password"},
    "bitrix": {"host": "host", "login": "login", "password": "password"},
    "iiko": {"host": "host", "login": "login", "password": "password"},
}


def _resolve_conn_type(doc_data: dict) -> str:
    conn_type = doc_data.get("conn_type") or ""
    if conn_type and conn_type not in ("other",):
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


def _existing_extra(conn_id: str) -> str | None:
    if not conn_id:
        return None
    row = get_connection(conn_id)
    if not row:
        return None
    return row.get("extra")


def _frappe_meta_kwargs(doc_data: dict, existing_extra: str | None) -> dict[str, str]:
    meta = unpack_extra(existing_extra)
    kwargs: dict[str, str] = {}
    creation = doc_data.get("creation")
    owner = doc_data.get("owner")
    if creation and not meta.get("frappe_creation"):
        kwargs["frappe_creation"] = str(creation)
    if owner and not meta.get("frappe_owner"):
        kwargs["frappe_owner"] = str(owner)
    return kwargs


def _to_airflow_payload(doc_data: dict, existing_extra: str | None = None) -> dict:
    conn_type = _resolve_conn_type(doc_data)
    mapping = _PLATFORM_FIELDS.get(conn_type, {})
    conn_id = _resolve_conn_id(doc_data)
    merged_extra = existing_extra or _existing_extra(conn_id)
    payload = {
        "conn_id": conn_id,
        "conn_type": conn_type,
        "description": doc_data.get("description", ""),
        "extra": pack_extra(
            platform=doc_data.get("platform", ""),
            slug=doc_data.get("slug", ""),
            display_name=doc_data.get("display_name", ""),
            target_db_connection=doc_data.get("target_db_connection", ""),
            existing_extra=merged_extra,
            **_frappe_meta_kwargs(doc_data, merged_extra),
        ),
    }
    for form_field, airflow_field in mapping.items():
        val = doc_data.get(form_field) or ""
        payload[airflow_field] = val
    return payload


def _from_airflow_row(row: dict) -> dict:
    meta = unpack_extra(row.get("extra"))
    profile = infer_connection_profile(row["conn_id"], row.get("conn_type") or "", meta)
    conn_type = profile.get("conn_type") or row.get("conn_type") or "ym"
    mapping = _PLATFORM_FIELDS.get(conn_type, {})
    doc = {
        "conn_id": row["conn_id"],
        "conn_type": conn_type,
        "description": row.get("description", ""),
        "platform": meta.get("platform") or profile.get("platform", ""),
        "slug": meta.get("slug") or profile.get("slug", ""),
        "display_name": meta.get("display_name", ""),
        "target_db_connection": meta.get("target_db_connection", ""),
        "frappe_creation": meta.get("frappe_creation", ""),
        "frappe_owner": meta.get("frappe_owner", ""),
    }
    for form_field, airflow_field in mapping.items():
        if airflow_field == "password":
            doc[form_field] = None
        else:
            doc[form_field] = row.get(airflow_field, "") or ""
    return doc


def _sync_connection_registry(conn_id: str) -> None:
    from frappe_airflow.airflow_db.connection_registry_sync import rebuild_connection_registry_entry

    rebuild_connection_registry_entry(conn_id)


def _remove_connection_registry(conn_id: str) -> None:
    from frappe_airflow.airflow_db.connection_registry_sync import remove_connection_registry_entry

    remove_connection_registry_entry(conn_id)


class AMAirflowConnection(VirtualAirflowDocument):
    def validate(self):
        if not (self.display_name or "").strip():
            frappe.throw(_("Display Name is required"))
        if not (self.target_db_connection or "").strip():
            frappe.throw(_("Target Database is required"))
        if self.platform and not self.conn_type:
            self.conn_type = default_conn_type_for_platform(self.platform)
        allowed = CONN_TYPE_BY_PLATFORM.get(self.platform or "", ())
        if self.platform and self.conn_type and self.conn_type not in allowed:
            frappe.throw(_("Connection type {0} is not valid for {1}").format(self.conn_type, self.platform))
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
        if frappe.utils.cint(doc_data.get("assign_to_matching_dags", 1)):
            assign_connection_to_matching_dags(payload["conn_id"], payload["conn_type"])
        _sync_connection_registry(payload["conn_id"])

    def db_update(self, *args, **kwargs):
        doc_data = self.as_dict()
        existing = _existing_extra(self.conn_id or self.name)
        payload = _to_airflow_payload(doc_data, existing_extra=existing)
        upsert_connection(payload)
        sync_companion_connections(doc_data)
        _sync_connection_registry(payload["conn_id"])

    def delete(self):
        row = get_connection(self.name) or {}
        meta = unpack_extra(row.get("extra"))
        slug = meta.get("slug", "") or self.slug or ""
        conn_type = row.get("conn_type") or self.conn_type or ""
        conn_id = self.name
        remove_companion_connections(conn_type, slug)
        delete_connection(conn_id)
        _remove_connection_registry(conn_id)

    @staticmethod
    def get_list(args):
        try:
            rows = list_connections(
                search=extract_search_text(args),
                limit=args.get("page_length") or args.get("limit_page_length") or 20,
                offset=args.get("start") or args.get("limit_start") or 0,
            )
            filtered = []
            for row in rows:
                meta = unpack_extra(row.get("extra"))
                profile = infer_connection_profile(
                    row["conn_id"], row.get("conn_type") or "", meta
                )
                if profile.get("is_companion"):
                    continue
                doc = _from_airflow_row(row)
                doc["name"] = row["conn_id"]
                filtered.append(doc)
            if is_link_search(args):
                return as_link_search_rows(
                    filtered,
                    ("display_name", "conn_id", "platform", "conn_type", "slug"),
                )
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
