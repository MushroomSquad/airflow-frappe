"""Serialize AM Table Config records into dag_table_config_{dag_id} Airflow Variables."""
from __future__ import annotations

import json

import frappe

from frappe_airflow.airflow_db.variable_manager import set_variable


def build_dag_table_config(configs: list[dict]) -> dict:
    """Build config dict: scope_key -> table_name -> settings.

    scope_key is ``_default`` or marketplace ``conn_id``.
    """
    result: dict = {}

    for cfg in configs:
        scope_key = "_default" if cfg["scope"] == "_default" else (cfg.get("connection") or "")
        if not scope_key:
            continue
        table_name = cfg["table_name"]

        table_cfg: dict = {
            "enabled": cfg.get("enabled", True),
            "load_strategy": cfg.get("load_strategy", "append"),
            "incremental_days": cfg.get("incremental_days"),
            "auto_alter": cfg.get("auto_alter", False),
            "exclude_fields": cfg.get("exclude_fields", []),
            "rename_fields": cfg.get("rename_fields", {}),
            "targets": cfg.get("targets", []),
        }

        result.setdefault(scope_key, {})[table_name] = table_cfg

    return result


def load_table_configs_for_dag(dag_id: str) -> list[dict]:
    """Load AM Table Config rows from MariaDB for one DAG."""
    if not dag_id or not frappe.db.exists("DocType", "AM Table Config"):
        return []

    raw_configs = frappe.get_all(
        "AM Table Config",
        filters={"dag_id": dag_id},
        fields=["name"],
    )

    configs: list[dict] = []
    for cfg in raw_configs:
        doc = frappe.get_doc("AM Table Config", cfg["name"])
        configs.append(
            {
                "dag_id": doc.dag_id,
                "scope": doc.scope,
                "connection": doc.connection or "",
                "table_name": doc.table_name,
                "enabled": bool(doc.enabled),
                "load_strategy": doc.load_strategy,
                "incremental_days": doc.incremental_days,
                "auto_alter": bool(doc.auto_alter),
                "exclude_fields": [r.field_name for r in doc.exclude_fields],
                "rename_fields": {r.source_field: r.target_field for r in doc.rename_fields},
                "targets": [
                    {"db": r.db_connection or None, "table": r.table_name}
                    for r in doc.targets
                ],
            }
        )
    return configs


def rebuild_dag_table_config(dag_id: str, configs: list[dict]) -> None:
    """Write dag_table_config_{dag_id} Variable."""
    built = build_dag_table_config(configs)
    key = f"dag_table_config_{dag_id}"
    set_variable(key, json.dumps(built, ensure_ascii=False), description="")


def reload_dag_table_config_from_db(dag_id: str) -> None:
    """Rebuild dag_table_config_{dag_id} from AM Table Config rows."""
    rebuild_dag_table_config(dag_id, load_table_configs_for_dag(dag_id))
