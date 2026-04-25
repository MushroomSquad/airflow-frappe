import json
from unittest.mock import patch

from airflow_manager.airflow_db.registry_sync import build_registry, rebuild_client_registry


CLIENTS = [
    {
        "id": "efendem",
        "display_name": "Efendem",
        "db": "05efendem_postgres",
        "cabinets": [
            {
                "slug": "filippov_sv",
                "display_name": "Филиппов",
                "platform": "wb",
                "active": True,
                "dags": "wb_orders_etl_dag, wb_stocks_etl_dag",
            },
            {
                "slug": "pharm_legend",
                "display_name": "Pharm",
                "platform": "wb",
                "active": True,
                "dags": "",
            },
            {
                "slug": "filippov_oz",
                "display_name": "Филиппов OZ",
                "platform": "oz",
                "active": False,
                "dags": "",
            },
        ],
    }
]


def test_build_registry_structure():
    result = build_registry(CLIENTS)
    assert "efendem" in result
    efendem = result["efendem"]
    assert efendem["display_name"] == "Efendem"
    assert efendem["db"] == "05efendem_postgres"
    assert "wb" in efendem
    assert "filippov_sv" in efendem["wb"]


def test_build_registry_parses_dags():
    result = build_registry(CLIENTS)
    assert result["efendem"]["wb"]["filippov_sv"]["dags"] == [
        "wb_orders_etl_dag",
        "wb_stocks_etl_dag",
    ]
    assert result["efendem"]["wb"]["pharm_legend"]["dags"] == []


def test_build_registry_preserves_active_flag():
    result = build_registry(CLIENTS)
    assert result["efendem"]["oz"]["filippov_oz"]["active"] is False


def test_build_registry_separates_platforms():
    result = build_registry(CLIENTS)
    assert "wb" in result["efendem"]
    assert "oz" in result["efendem"]
    assert "ms" not in result["efendem"]


def test_rebuild_calls_set_variable():
    with patch("airflow_manager.airflow_db.registry_sync.set_variable") as mock_set:
        rebuild_client_registry(CLIENTS)
    mock_set.assert_called_once()
    key, val = mock_set.call_args[0][:2]
    assert key == "CLIENT_REGISTRY"
    parsed = json.loads(val)
    assert "efendem" in parsed
