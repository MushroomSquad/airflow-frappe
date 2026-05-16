from frappe_airflow.airflow_db.connection_registry_sync import _entry_from_row


def test_entry_from_row():
    row = {
        "conn_id": "wb_api_token_filippov",
        "conn_type": "wb",
        "extra": '{"platform":"wb","slug":"filippov","display_name":"ИП Филиппов","target_db_connection":"05efendem_postgres_cred"}',
    }
    entry = _entry_from_row(row)
    assert entry["display_name"] == "ИП Филиппов"
    assert entry["target_db_connection"] == "05efendem_postgres_cred"
    assert entry["slug"] == "filippov"
