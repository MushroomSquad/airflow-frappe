import sys
from types import SimpleNamespace

# Minimal frappe stub so we can import VirtualAirflowDocument without a site.
class _Document:
    pass


_frappe = SimpleNamespace(
    _dict=lambda **kwargs: dict(kwargs),
    DoesNotExistError=type("DoesNotExistError", (Exception,), {}),
    clear_last_message=lambda: None,
)
_doc_mod = SimpleNamespace(Document=_Document)
_frappe.model = SimpleNamespace(document=_doc_mod)
sys.modules.setdefault("frappe", _frappe)
sys.modules.setdefault("frappe.model", _frappe.model)
sys.modules.setdefault("frappe.model.document", _doc_mod)

from frappe_airflow.virtual_document import VirtualAirflowDocument


class _VirtualDoc(VirtualAirflowDocument):
    doctype = "AM Airflow Connection"

    def __init__(self, name="wb_api_token_testik"):
        self.flags = _frappe._dict()
        self.name = name
        self._action = "save"
        self.meta = _frappe._dict(issingle=0)

    def is_new(self):
        return False

    def check_docstatus_transition(self, _to_docstatus):
        return None


def test_check_if_latest_sets_doc_before_save(monkeypatch):
    previous = _frappe._dict(docstatus=0, creation="2026-01-01", owner="Administrator")
    monkeypatch.setattr(_frappe, "get_doc", lambda doctype, name: previous)
    doc = _VirtualDoc()

    doc.check_if_latest()

    assert doc._doc_before_save is previous
    assert doc._action == "save"
