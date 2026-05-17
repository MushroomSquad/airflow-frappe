from unittest.mock import MagicMock

from frappe_airflow.password_fields import normalize_submitted_password, read_password_from_doc


def test_normalize_submitted_password_empty():
    assert normalize_submitted_password("") == ""
    assert normalize_submitted_password(None) == ""


def test_normalize_submitted_password_masks():
    assert normalize_submitted_password("********") == ""
    assert normalize_submitted_password("••••••") == ""


def test_normalize_submitted_password_keeps_real_token():
    token = "eyJhbGciOiJIUzI1NiJ9.test"
    assert normalize_submitted_password(token) == token


def test_read_password_from_doc_uses_get_password():
    doc = MagicMock()
    doc.get_password.return_value = "wb-secret-token"
    assert read_password_from_doc(doc, "api_token") == "wb-secret-token"
    doc.get_password.assert_called_once_with("api_token", raise_exception=False)


def test_read_password_from_doc_ignores_mask_placeholder():
    doc = MagicMock()
    doc.get_password.return_value = "********"
    assert read_password_from_doc(doc, "api_token") == ""
