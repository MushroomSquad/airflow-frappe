"""Serialize AM Table Config records into dag_table_config_{dag_id} Airflow Variables."""
from __future__ import annotations

import json

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


def rebuild_dag_table_config(dag_id: str, configs: list[dict]) -> None:
    """Write dag_table_config_{dag_id} Variable."""
    built = build_dag_table_config(configs)
    key = f"dag_table_config_{dag_id}"
    set_variable(key, json.dumps(built, ensure_ascii=False), description="")
