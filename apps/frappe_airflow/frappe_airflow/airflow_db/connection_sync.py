"""Sync marketplace connections including Ozon companion rows."""
from __future__ import annotations

from frappe_airflow.airflow_db.connection_manager import delete_connection, upsert_connection
from frappe_airflow.airflow_db.connection_meta import pack_extra
from frappe_airflow.airflow_db.dag_platform import companion_conn_ids


def _companion_payload(conn_id: str, password: str, conn_type: str = "other") -> dict:
    return {
        "conn_id": conn_id,
        "conn_type": conn_type,
        "password": password,
        "description": "",
        "extra": pack_extra(is_companion=True),
    }


def sync_companion_connections(doc_data: dict) -> None:
    """Create/update companion Airflow connections for Oz seller/perf types."""
    conn_type = doc_data.get("conn_type", "")
    slug = (doc_data.get("slug") or "").strip()
    if not slug:
        return

    for companion_id in companion_conn_ids(conn_type, slug):
        if conn_type == "oz_seller" and companion_id.endswith(f"oz_client_seller_id_{slug}"):
            password = doc_data.get("client_seller_id") or ""
        elif conn_type == "oz_perf" and companion_id.endswith(f"oz_client_perf_secret_{slug}"):
            password = doc_data.get("perf_secret") or ""
        else:
            password = ""
        upsert_connection(_companion_payload(companion_id, password))


def remove_companion_connections(conn_type: str, slug: str) -> None:
    for companion_id in companion_conn_ids(conn_type, slug):
        delete_connection(companion_id)
