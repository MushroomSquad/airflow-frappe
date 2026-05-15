"""Build connection checkbox options for a DAG form."""
from __future__ import annotations

from frappe_airflow.airflow_db.connection_manager import list_marketplace_connections
from frappe_airflow.airflow_db.connection_meta import unpack_extra
from frappe_airflow.airflow_db.dag_platform import (
    conn_matches_dag,
    infer_connection_profile,
    infer_dag_platform,
)


def build_dag_connection_options(dag_id: str) -> list[dict[str, str]]:
    platform = infer_dag_platform(dag_id)
    options: list[dict[str, str]] = []

    for row in list_marketplace_connections(limit=500):
        meta = unpack_extra(row.get("extra"))
        profile = infer_connection_profile(row["conn_id"], row.get("conn_type") or "", meta)
        if profile.get("is_companion"):
            continue
        conn_type = profile.get("conn_type") or ""
        if not conn_matches_dag(conn_type, platform, dag_id, conn_id=row["conn_id"]):
            continue
        slug = profile.get("slug") or meta.get("slug", "")
        label = row["conn_id"]
        if slug:
            label = f"{row['conn_id']} ({slug})"
        options.append({"label": label, "value": row["conn_id"]})

    return options
