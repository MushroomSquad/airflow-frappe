from frappe_airflow.doctype_utils import as_link_search_rows, extract_search_text, is_link_search


def test_is_link_search_from_reference_doctype():
    assert is_link_search({"reference_doctype": "AM Airflow DAG"}) is True


def test_is_link_search_from_as_list():
    assert is_link_search({"as_list": True}) is True


def test_is_link_search_from_name_filter():
    assert is_link_search(
        {"or_filters": [["AM Database Connection", "name", "like", "%pg%"]]}
    ) is True


def test_is_link_search_false_for_plain_list():
    assert is_link_search({"page_length": 20}) is False


def test_extract_search_text_from_or_filters():
    assert (
        extract_search_text({"or_filters": [["AM Database Connection", "name", "like", "%prod%"]]})
        == "prod"
    )


def test_as_link_search_rows_returns_tuples():
    rows = [
        {
            "name": "05efendem_postgres_cred",
            "host": "db.example",
            "schema": "warehouse",
            "description": "main db",
        }
    ]
    assert as_link_search_rows(rows, ("host", "schema", "description")) == [
        ("05efendem_postgres_cred", "db.example, warehouse, main db", 1)
    ]
