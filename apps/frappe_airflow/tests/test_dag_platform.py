from frappe_airflow.airflow_db.dag_platform import (
    build_conn_id,
    companion_conn_ids,
    conn_matches_dag,
    infer_connection_profile,
    infer_dag_platform,
    is_perf_dag,
    normalize_conn_type,
)


def test_infer_dag_platform_wb():
    assert infer_dag_platform("wb_stocks_etl_dag") == "wb"


def test_infer_dag_platform_oz():
    assert infer_dag_platform("oz_analytics_stocks_etl_dag") == "oz"


def test_infer_dag_platform_ms():
    assert infer_dag_platform("ms_warehouse_etl_dag") == "ms"


def test_is_perf_dag():
    assert is_perf_dag("oz_adv_list_detail_etl_dag") is True
    assert is_perf_dag("oz_analytics_stocks_etl_dag") is False


def test_conn_matches_oz_perf_only_on_adv_dags():
    assert conn_matches_dag("oz_perf", "oz", "oz_adv_list_detail_etl_dag") is True
    assert conn_matches_dag("oz_perf", "oz", "oz_analytics_stocks_etl_dag") is False


def test_conn_matches_oz_seller_not_on_adv_dags():
    assert conn_matches_dag("oz_seller", "oz", "oz_analytics_stocks_etl_dag") is True
    assert conn_matches_dag("oz_seller", "oz", "oz_adv_list_detail_etl_dag") is False


def test_build_conn_id_wb():
    assert build_conn_id("wb", "filippov_sv") == "wb_api_token_filippov_sv"


def test_companion_conn_ids_oz():
    assert companion_conn_ids("oz_seller", "foo") == ["oz_client_seller_id_foo"]
    assert companion_conn_ids("oz_perf", "foo") == ["oz_client_perf_secret_foo"]


def test_normalize_legacy_wb_conn_id():
    assert normalize_conn_type("wb_filippov", "other") == "wb"


def test_infer_connection_profile_legacy_wb():
    profile = infer_connection_profile("wb_filippov", "other", {})
    assert profile["platform"] == "wb"
    assert profile["slug"] == "filippov"
    assert profile["conn_type"] == "wb"


def test_conn_matches_legacy_wb_on_wb_dag():
    assert conn_matches_dag("other", "wb", "wb_orders_etl_dag", conn_id="wb_filippov") is True


def test_conn_matches_amo_on_amo_dag():
    assert conn_matches_dag("amo", "amo", "amo_leads_etl_dag", conn_id="amocrm_api_token_demo") is True


def test_build_conn_id_ym():
    assert build_conn_id("ym", "shop1") == "ym_api_token_shop1"


def test_build_conn_id_amo():
    assert build_conn_id("amo", "demo") == "amocrm_api_token_demo"
