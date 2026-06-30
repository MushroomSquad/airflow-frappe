from unittest.mock import patch

from frappe_airflow.airflow_db.client_directory_sync import REGISTRY_KEY, build_client_directory


def test_build_client_directory_groups_connections():
    clients = [
        {"name": "ИП Ишина А.Н.", "client_name": "ИП Ишина А.Н."},
    ]
    rows = [
        {
            "conn_id": "oz_client_perf_id_ishina_an",
            "conn_type": "oz_perf",
            "extra": '{"client":"ИП Ишина А.Н.","platform":"oz","slug":"ishina_an"}',
        },
        {
            "conn_id": "wb_api_token_ishina_an",
            "conn_type": "wb",
            "extra": '{"client":"ИП Ишина А.Н.","platform":"wb","slug":"ishina_an"}',
        },
    ]

    with patch("frappe_airflow.airflow_db.client_directory_sync.frappe") as frappe_mock, patch(
        "frappe_airflow.airflow_db.client_directory_sync.list_marketplace_connections",
        return_value=rows,
    ):
        frappe_mock.db.exists.return_value = True
        frappe_mock.get_all.return_value = clients

        registry = build_client_directory()

    assert REGISTRY_KEY == "CLIENT_DIRECTORY"
    assert registry["ИП Ишина А.Н."]["client_name"] == "ИП Ишина А.Н."
    assert registry["ИП Ишина А.Н."]["connections"] == [
        "oz_client_perf_id_ishina_an",
        "wb_api_token_ishina_an",
    ]
