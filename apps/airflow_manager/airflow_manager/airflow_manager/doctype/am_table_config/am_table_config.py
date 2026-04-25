import frappe
from frappe.model.document import Document


class AMTableConfig(Document):
    def after_save(self):
        _trigger_config_sync(self.client)

    def on_trash(self):
        _trigger_config_sync(self.client)


def _trigger_config_sync(client_id: str):
    from airflow_manager.airflow_db.config_sync import rebuild_client_config

    raw_configs = frappe.get_all(
        "AM Table Config",
        filters={"client": client_id},
        fields=[
            "name",
            "dag_id",
            "scope",
            "cabinet",
            "table_name",
            "enabled",
            "load_strategy",
            "incremental_days",
            "auto_alter",
        ],
    )

    configs = []
    for cfg in raw_configs:
        doc = frappe.get_doc("AM Table Config", cfg["name"])
        cabinet_slug = ""
        if doc.cabinet:
            cabinet_slug = frappe.db.get_value("AM Cabinet", doc.cabinet, "slug") or ""

        configs.append(
            {
                "dag_id": doc.dag_id,
                "scope": doc.scope,
                "cabinet_slug": cabinet_slug,
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

    rebuild_client_config(client_id, configs)
