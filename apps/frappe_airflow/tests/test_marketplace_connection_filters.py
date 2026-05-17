from frappe_airflow.airflow_db.connection_manager import _marketplace_connection_filters


def test_marketplace_filters_exclude_postgres_and_companions():
    clauses, params = _marketplace_connection_filters()
    assert "conn_type != 'postgres'" in clauses
    assert "conn_id NOT LIKE 'oz_client_seller_id_%'" in clauses
    assert "conn_id NOT LIKE 'oz_client_perf_secret_%'" in clauses
    assert params == {}


def test_marketplace_filters_with_search():
    clauses, params = _marketplace_connection_filters(search="minakov")
    assert params["search"] == "%minakov%"
