"""Serialize AM Table Config records into client_config_{id} Airflow Variable.

Three-level structure: dag_id -> _default|cabinet_slug -> table_name -> config dict.
"""
from __future__ import annotations

import json

from frappe_airflow.airflow_db.variable_manager import set_variable


def build_client_config(configs: list[dict]) -> dict:
    """Build client_config dict from a list of TableConfig records.

    Each config dict has:
      dag_id, scope ("_default" or "cabinet"), cabinet_slug,
      table_name, enabled, load_strategy, incremental_days, auto_alter,
      exclude_fields: list[str], rename_fields: dict[str,str],
      targets: list[dict(db, table)]
    """
    result: dict = {}

    for cfg in configs:
        dag_id = cfg["dag_id"]
        scope_key = "_default" if cfg["scope"] == "_default" else cfg["cabinet_slug"]
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

        result.setdefault(dag_id, {}).setdefault(scope_key, {})[table_name] = table_cfg

    return result


def rebuild_client_config(client_id: str, configs: list[dict]) -> None:
    """Serialize configs to JSON and write to client_config_{client_id} Variable."""
    built = build_client_config(configs)
    key = f"client_config_{client_id}"
    set_variable(key, json.dumps(built, ensure_ascii=False), description="")
