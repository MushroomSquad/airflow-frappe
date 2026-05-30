import json
import sys
from types import ModuleType, SimpleNamespace
from unittest.mock import MagicMock, patch

# Stub sqlalchemy before airflow_db imports.
if "sqlalchemy" not in sys.modules:
    _sqla = ModuleType("sqlalchemy")
    _sqla.text = lambda s: s
    _sqla.create_engine = MagicMock()
    _orm = ModuleType("sqlalchemy.orm")
    _orm.Session = MagicMock()
    _orm.sessionmaker = MagicMock()
    sys.modules["sqlalchemy"] = _sqla
    sys.modules["sqlalchemy.orm"] = _orm

# Minimal frappe stub for importing connection_delete without a site.
def _default_throw(msg, exc=None):
    raise (exc or ValueError)(msg)


_frappe = SimpleNamespace(
    throw=_default_throw,
    whitelist=lambda **kwargs: lambda fn: fn,
    form_dict={},
    db=SimpleNamespace(
        exists=MagicMock(return_value=True),
        delete=MagicMock(),
        commit=MagicMock(),
    ),
    get_all=MagicMock(return_value=[]),
    delete_doc=MagicMock(),
    has_permission=MagicMock(return_value=True),
    publish_progress=MagicMock(),
    log_error=MagicMock(),
    get_traceback=lambda: "",
    get_meta=MagicMock(return_value=SimpleNamespace(is_virtual=0)),
)

_frappe.exceptions = SimpleNamespace(PermissionError=type("PermissionError", (Exception,), {}))
_frappe._ = lambda s, *a, **k: s.format(*a) if a else s

if "cryptography" not in sys.modules:
    _crypto = ModuleType("cryptography")
    _crypto_fernet = ModuleType("cryptography.fernet")
    _crypto_fernet.Fernet = MagicMock()
    _crypto_fernet.InvalidToken = type("InvalidToken", (Exception,), {})
    sys.modules["cryptography"] = _crypto
    sys.modules["cryptography.fernet"] = _crypto_fernet

if "frappe" not in sys.modules:
    sys.modules["frappe"] = _frappe

from frappe_airflow.airflow_db import connection_delete


def _reset_frappe_mocks():
    _frappe.db.exists.reset_mock()
    _frappe.db.exists.return_value = True
    _frappe.db.delete.reset_mock()
    _frappe.db.commit.reset_mock()
    _frappe.get_all.reset_mock()
    _frappe.get_all.return_value = []
    _frappe.delete_doc.reset_mock()
    _frappe.has_permission.return_value = True


def test_remove_connection_from_all_dag_configs_updates_matching_dags():
    _reset_frappe_mocks()
    _frappe.get_all.return_value = [{"dag_id": "wb_orders_etl_dag"}]
    with patch(
        "frappe_airflow.airflow_db.connection_delete.get_selected_connections",
        return_value=["wb_api_token_a", "wb_api_token_b"],
    ), patch(
        "frappe_airflow.airflow_db.connection_delete.set_selected_connections"
    ) as set_sel:
        connection_delete.remove_connection_from_all_dag_configs("wb_api_token_a")
    set_sel.assert_called_once_with("wb_orders_etl_dag", ["wb_api_token_b"])


def test_remove_connection_table_configs_deletes_rows_and_returns_dag_ids():
    _reset_frappe_mocks()
    _frappe.get_all.return_value = [
        {"name": "TC-1", "dag_id": "wb_orders_etl_dag"},
        {"name": "TC-2", "dag_id": "wb_orders_etl_dag"},
    ]
    dag_ids = connection_delete.remove_connection_table_configs("wb_api_token_a")
    assert dag_ids == ["wb_orders_etl_dag"]
    assert _frappe.delete_doc.call_count == 2


def test_delete_marketplace_connection_full_cascade():
    _reset_frappe_mocks()
    row = {
        "conn_id": "wb_api_token_test",
        "conn_type": "wb",
        "extra": '{"platform":"wb","slug":"test","display_name":"Test"}',
    }
    with patch(
        "frappe_airflow.airflow_db.connection_delete.get_connection",
        return_value=row,
    ), patch(
        "frappe_airflow.airflow_db.connection_delete.remove_connection_from_all_dag_configs"
    ) as rm_dag, patch(
        "frappe_airflow.airflow_db.connection_delete.remove_connection_table_configs",
        return_value=["wb_orders_etl_dag"],
    ) as rm_table, patch(
        "frappe_airflow.airflow_db.connection_delete.reload_dag_table_configs_for_dags"
    ) as reload_cfg, patch(
        "frappe_airflow.airflow_db.connection_delete.remove_companion_connections"
    ) as rm_comp, patch(
        "frappe_airflow.airflow_db.connection_delete.delete_connection"
    ) as del_conn, patch(
        "frappe_airflow.airflow_db.connection_delete.remove_connection_registry_entry"
    ) as rm_reg:
        connection_delete.delete_marketplace_connection("wb_api_token_test")

    rm_dag.assert_called_once_with("wb_api_token_test")
    _frappe.db.delete.assert_called_once_with("AM DAG Connection", {"connection": "wb_api_token_test"})
    rm_table.assert_called_once_with("wb_api_token_test")
    reload_cfg.assert_called_once_with(["wb_orders_etl_dag"])
    rm_comp.assert_called_once_with("wb", "test")
    del_conn.assert_called_once_with("wb_api_token_test")
    rm_reg.assert_called_once_with("wb_api_token_test")
    _frappe.db.commit.assert_called_once()


def _ensure_frappe_package_stubs():
    if not hasattr(_frappe, "desk"):
        _desk = ModuleType("frappe.desk")
        _listview = ModuleType("frappe.desk.listview")
        _listview.get_group_by_count = MagicMock(return_value=[])
        _desk.listview = _listview
        _frappe.desk = _desk
        sys.modules["frappe.desk"] = _desk
        sys.modules["frappe.desk.listview"] = _listview


def test_bulk_delete_connections_collects_failures():
    _reset_frappe_mocks()
    _ensure_frappe_package_stubs()
    from frappe_airflow import api as api_module

    def _delete(conn_id):
        if conn_id == "bad_conn":
            raise RuntimeError("db error")

    with patch(
        "frappe_airflow.api.delete_marketplace_connection",
        side_effect=_delete,
    ):
        result = api_module.bulk_delete_connections(["ok_conn", "bad_conn"])

    assert result["deleted"] == ["ok_conn"]
    assert len(result["failed"]) == 1
    assert result["failed"][0]["conn_id"] == "bad_conn"
    assert "db error" in result["failed"][0]["error"]


def test_bulk_delete_connections_rejects_empty_list():
    _reset_frappe_mocks()
    _ensure_frappe_package_stubs()
    from frappe_airflow import api as api_module

    _frappe.throw = _default_throw
    try:
        api_module.bulk_delete_connections([])
        raise AssertionError("expected throw for empty list")
    except ValueError:
        pass


def test_bulk_delete_connections_parses_json_string():
    _reset_frappe_mocks()
    _ensure_frappe_package_stubs()
    from frappe_airflow import api as api_module

    with patch("frappe_airflow.api.delete_marketplace_connection") as del_fn:
        api_module.bulk_delete_connections(json.dumps(["wb_a", "wb_b"]))
    assert del_fn.call_count == 2


def test_delete_items_routes_airflow_connections():
    _reset_frappe_mocks()
    _ensure_frappe_package_stubs()
    from frappe_airflow import api as api_module

    _frappe.form_dict = {
        "doctype": "AM Airflow Connection",
        "items": json.dumps(["wb_a", "wb_b"]),
    }
    with patch(
        "frappe_airflow.api.bulk_delete_connections",
        return_value={"deleted": ["wb_a", "wb_b"], "failed": [], "total": 2},
    ) as bulk_fn, patch("frappe_airflow.api._delete_airflow_connection_items") as inner:
        inner.return_value = []
        result = api_module.delete_items()
    inner.assert_called_once()
    bulk_fn.assert_not_called()


def test_delete_items_delegates_other_doctypes():
    _reset_frappe_mocks()
    _ensure_frappe_package_stubs()
    from frappe_airflow import api as api_module

    _frappe.form_dict = {"doctype": "User", "items": "[]"}
    core_fn = MagicMock(return_value=[])
    fake_reportview = ModuleType("frappe.desk.reportview")
    fake_reportview.delete_items = core_fn
    sys.modules["frappe.desk.reportview"] = fake_reportview
    if not hasattr(_frappe, "desk"):
        _frappe.desk = ModuleType("frappe.desk")
    _frappe.desk.reportview = fake_reportview

    result = api_module.delete_items()
    core_fn.assert_called_once()
    assert result == []


if __name__ == "__main__":
    for fn in [
        test_remove_connection_from_all_dag_configs_updates_matching_dags,
        test_remove_connection_table_configs_deletes_rows_and_returns_dag_ids,
        test_delete_marketplace_connection_full_cascade,
        test_bulk_delete_connections_collects_failures,
        test_bulk_delete_connections_rejects_empty_list,
        test_bulk_delete_connections_parses_json_string,
        test_delete_items_routes_airflow_connections,
        test_delete_items_delegates_other_doctypes,
    ]:
        fn()
        print("ok", fn.__name__)
    print("all passed")
