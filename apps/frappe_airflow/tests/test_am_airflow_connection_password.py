from unittest.mock import MagicMock, patch

from frappe_airflow.airflow_manager.doctype.am_airflow_connection.am_airflow_connection import (
    _to_airflow_payload,
)


def test_to_airflow_payload_reads_password_via_get_password():
    doc = MagicMock()
    doc.as_dict.return_value = {
        "conn_id": "wb_api_token_test",
        "conn_type": "wb",
        "platform": "wb",
        "slug": "test",
        "display_name": "Test",
        "target_db_connection": "test_conn_id",
        "api_token": "",
    }
    doc.get_password.return_value = "real-wb-token"

    with patch(
        "frappe_airflow.airflow_manager.doctype.am_airflow_connection.am_airflow_connection._existing_extra",
        return_value=None,
    ):
        payload = _to_airflow_payload(doc)

    assert payload["password"] == "real-wb-token"
    doc.get_password.assert_called_with("api_token", raise_exception=False)
