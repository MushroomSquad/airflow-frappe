from unittest.mock import patch

from frappe_airflow.cleanup_migration import _env_bool, run


def test_env_bool_default():
    assert _env_bool("MISSING", True) is True
    assert _env_bool("MISSING", False) is False


def test_env_bool_parses():
    with patch.dict("os.environ", {"DRY_RUN": "0"}):
        assert _env_bool("DRY_RUN", True) is False


def test_run_dry_run_counts(capsys):
    rows = [{"conn_id": "wb_api_token_a", "extra": '{"platform":"wb"}', "conn_type": "wb"}]
    with (
        patch.dict("os.environ", {"DRY_RUN": "1"}, clear=False),
        patch(
            "frappe_airflow.airflow_db.connection_manager.list_marketplace_connections",
            return_value=rows,
        ),
        patch(
            "frappe_airflow.airflow_db.connection_manager.get_connection",
            return_value={"password": "", "conn_type": "wb", "extra": '{"platform":"wb"}'},
        ),
        patch(
            "frappe_airflow.airflow_db.dag_platform.is_companion_conn_id",
            return_value=False,
        ),
        patch("frappe_airflow.cleanup_migration.frappe") as mock_frappe,
    ):
        mock_frappe.get_all.return_value = ["dag1"]
        run()
    out = capsys.readouterr().out
    assert "DRY_RUN=True" in out
    assert "wb_api_token_a" in out
