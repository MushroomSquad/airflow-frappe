"""One-time migration: CLIENT_REGISTRY -> connections + DAG_REGISTRY.

bench --site SITE execute frappe_airflow.migrate_connections.run
"""
from __future__ import annotations

import json
import os

DRY_RUN = os.environ.get("DRY_RUN", "").lower() in ("1", "true", "yes")
REBUILD_ONLY = os.environ.get("REBUILD_ONLY", "").lower() in ("1", "true", "yes")


def _platform_conn_field(platform: str) -> str:
    return {
        "wb": "wb_token_connection",
        "oz": "oz_seller_connection",
        "ms": "ms_token_connection",
        "ym": "ym_token_connection",
        "amo": "amocrm_token_connection",
    }.get(platform, "")


def _parse_client_registry(raw: str) -> dict:
    text_val = raw.strip()
    if not text_val:
        raise ValueError("CLIENT_REGISTRY is empty")
    try:
        data = json.loads(text_val)
    except json.JSONDecodeError as exc:
        preview = text_val[:120].replace("\n", " ")
        raise ValueError(
            f"CLIENT_REGISTRY is not valid JSON (len={len(text_val)}, preview={preview!r})"
        ) from exc
    if not isinstance(data, dict):
        raise ValueError(f"CLIENT_REGISTRY must be a JSON object, got {type(data).__name__}")
    if "CLIENT_REGISTRY" in data and len(data) == 1:
        inner = data["CLIENT_REGISTRY"]
        if isinstance(inner, dict):
            return inner
    return data


def diagnose_client_registry():
    """Print how CLIENT_REGISTRY is stored in Airflow metadata DB (for troubleshooting)."""
    from sqlalchemy import text

    from frappe_airflow.airflow_db.connection import get_session
    from frappe_airflow.airflow_db.fernet import is_encrypted
    from frappe_airflow.airflow_db.variable_manager import get_variable
    import os

    print(f"AIRFLOW_DB_URL={os.environ.get('AIRFLOW_DB_URL', '')}")
    print(f"AIRFLOW_FERNET_KEY set={bool(os.environ.get('AIRFLOW_FERNET_KEY'))}")
    with get_session() as s:
        row = s.execute(
            text(
                "SELECT key, length(val::text) AS val_len, is_encrypted, "
                "left(val::text, 40) AS val_prefix FROM variable WHERE key = 'CLIENT_REGISTRY'"
            ),
        ).fetchone()
    if not row:
        print("No CLIENT_REGISTRY row in variable table")
        return
    print(
        f"DB row: len={row.val_len} is_encrypted={row.is_encrypted} prefix={row.val_prefix!r} "
        f"looks_fernet={is_encrypted(row.val_prefix or '')}"
    )
    try:
        decoded = get_variable("CLIENT_REGISTRY")
    except Exception as exc:
        print(f"get_variable failed: {exc}")
        return
    if not decoded:
        print("get_variable returned empty")
        return
    print(f"get_variable: len={len(decoded)} starts_with={decoded[:60]!r}...")


def rebuild_registries():
    """Rebuild CONNECTION_REGISTRY and DAG_REGISTRY from current Frappe/Airflow state."""
    import frappe

    from frappe_airflow.airflow_db.connection_registry_sync import rebuild_connection_registry
    from frappe_airflow.airflow_db.dag_registry_sync import rebuild_dag_registry

    if DRY_RUN:
        print("DRY_RUN: would rebuild CONNECTION_REGISTRY and DAG_REGISTRY")
        return
    rebuild_connection_registry()
    rebuild_dag_registry()
    frappe.db.commit()
    print("Rebuilt CONNECTION_REGISTRY and DAG_REGISTRY")


def run():
    import frappe
    from cryptography.fernet import InvalidToken

    from frappe_airflow.airflow_db.connection_registry_sync import rebuild_connection_registry
    from frappe_airflow.airflow_db.dag_connection_sync import set_selected_connections
    from frappe_airflow.airflow_db.dag_platform import build_conn_id, conn_matches_dag, default_conn_type_for_platform, infer_dag_platform
    from frappe_airflow.airflow_db.dag_reader import list_dags
    from frappe_airflow.airflow_db.dag_registry_sync import rebuild_dag_registry
    from frappe_airflow.airflow_db.variable_manager import get_variable

    if REBUILD_ONLY:
        rebuild_registries()
        return

    try:
        raw = get_variable("CLIENT_REGISTRY")
    except InvalidToken:
        print(
            "CLIENT_REGISTRY is encrypted but AIRFLOW_FERNET_KEY does not match Airflow. "
            "Fix AIRFLOW_FERNET_KEY in .env / compose, or re-import CLIENT_REGISTRY as plain JSON."
        )
        return

    if not raw or not raw.strip():
        conn_count = frappe.db.count("AM Airflow Connection")
        if conn_count:
            print(
                f"CLIENT_REGISTRY missing; {conn_count} AM Airflow Connection row(s) exist. "
                "Run: REBUILD_ONLY=1 bench --site SITE execute frappe_airflow.migrate_connections.run"
            )
        else:
            print(
                "CLIENT_REGISTRY is empty or missing and no AM Airflow Connection rows. "
                "Import CLIENT_REGISTRY from scripts/export_registry_to_variables.py, "
                "or create connections in Frappe UI, then REBUILD_ONLY=1."
            )
        return

    try:
        registry = _parse_client_registry(raw)
    except ValueError as exc:
        print(str(exc))
        return
    created = 0
    dag_updates: dict[str, set[str]] = {}

    for client_id, client in registry.items():
        db_conn = client.get("db") or ""
        for platform in ("wb", "oz", "ms", "ym", "amo"):
            cabinets = client.get(platform) or {}
            if not isinstance(cabinets, dict):
                continue
            for slug, cab in cabinets.items():
                if not cab.get("active", True):
                    continue
                display_name = cab.get("display_name") or slug
                field = _platform_conn_field(platform)
                conn_id = cab.get(field) if field else ""
                if not conn_id:
                    conn_id = build_conn_id(default_conn_type_for_platform(platform), slug)

                if DRY_RUN:
                    print(f"Would upsert {conn_id} db={db_conn} platform={platform}")
                    created += 1
                else:
                    _upsert_frappe_connection(
                        conn_id=conn_id,
                        platform=platform,
                        slug=slug,
                        display_name=display_name,
                        target_db=db_conn,
                    )
                    created += 1

                for dag_id in cab.get("dags") or []:
                    dag_updates.setdefault(dag_id, set()).add(conn_id)
                if not cab.get("dags"):
                    conn_type = default_conn_type_for_platform(platform)
                    for dag in list_dags():
                        dag_id = dag["dag_id"]
                        if infer_dag_platform(dag_id) == platform and conn_matches_dag(
                            conn_type, platform, dag_id, conn_id=conn_id
                        ):
                            dag_updates.setdefault(dag_id, set()).add(conn_id)

    for dag_id, conn_ids in dag_updates.items():
        if DRY_RUN:
            print(f"Would set DAG {dag_id}: {sorted(conn_ids)}")
        else:
            set_selected_connections(dag_id, sorted(conn_ids))

    if not DRY_RUN:
        rebuild_connection_registry()
        rebuild_dag_registry()
        frappe.db.commit()

    print(f"Done. Connections: {created}, DAGs: {len(dag_updates)}")


def _resolve_target_db_conn(target_db: str) -> str:
    """Return target_db if a postgres connection row exists in Airflow (for warnings only)."""
    from frappe_airflow.airflow_db.connection_manager import get_connection

    if not (target_db or "").strip():
        return ""
    row = get_connection(target_db.strip())
    if not row:
        return ""
    ctype = (row.get("conn_type") or "").lower()
    if ctype in ("postgres", "postgresql"):
        return target_db.strip()
    return ""


def _upsert_frappe_connection(
    conn_id: str,
    platform: str,
    slug: str,
    display_name: str,
    target_db: str,
) -> None:
    """Write connection metadata into Airflow ``connection.extra`` (bypasses Frappe Link checks)."""
    from frappe_airflow.airflow_db.connection_manager import get_connection, upsert_connection
    from frappe_airflow.airflow_db.connection_meta import pack_extra
    from frappe_airflow.airflow_db.dag_platform import default_conn_type_for_platform

    conn_type = default_conn_type_for_platform(platform)
    target_db = (target_db or "").strip()
    if target_db and not _resolve_target_db_conn(target_db):
        print(
            f"WARN: postgres connection {target_db!r} not in Airflow table — "
            f"still storing in extra for {conn_id}"
        )

    existing = get_connection(conn_id)
    extra = pack_extra(
        platform=platform,
        slug=slug,
        display_name=display_name,
        target_db_connection=target_db,
        existing_extra=(existing or {}).get("extra"),
    )
    upsert_connection(
        {
            "conn_id": conn_id,
            "conn_type": conn_type,
            "description": (existing or {}).get("description", ""),
            "host": (existing or {}).get("host", ""),
            "schema": (existing or {}).get("schema", ""),
            "login": (existing or {}).get("login", ""),
            "port": (existing or {}).get("port"),
            "extra": extra,
        }
    )
