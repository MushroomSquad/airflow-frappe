# Airflow Manager — Frappe App Build

You are building a Frappe v15 web application that manages Airflow configuration
(clients, cabinets, connections, variables) by connecting directly to Airflow's
PostgreSQL metadata database — no Airflow API, no CLI.

Work through each task in order. After each task: run tests, fix failures, then commit.

## Project context

- Frappe v15, Python 3.11
- No apache-airflow dependency — we implement Fernet ourselves using `cryptography`
- Two databases: Frappe MariaDB (our entities) + Airflow PG (connections, variables, dags)
- Run tests with: `python -m pytest tests/ -x -q`
- All paths are relative to `apps/airflow_manager/`

## Airflow DB schema we use

```
connection: conn_id, conn_type, description, host, schema, login,
            password (Fernet), port, is_encrypted, is_extra_encrypted, extra (Fernet)

variable:   key, val (plain JSON in our setup), description, is_encrypted

dag:        dag_id, is_paused, schedule_interval, last_parsed_time
```

## Variable schemas we produce

CLIENT_REGISTRY:
```json
{
  "efendem": {
    "display_name": "Efendem",
    "db": "05efendem_postgres_cred",
    "wb": {
      "filippov_sv": {"display_name": "И.П. Филиппов С.В.", "active": true, "dags": []}
    },
    "oz": {}, "ms": {}, "ym": {}
  }
}
```

client_config_{id}:
```json
{
  "dag_id": {
    "_default": {
      "table_name": {
        "enabled": true, "load_strategy": "append", "incremental_days": 15,
        "auto_alter": false, "exclude_fields": [], "rename_fields": {},
        "targets": [{"db": null, "table": "table_name"}]
      }
    },
    "cabinet_slug": {"table_name": {"incremental_days": 7}}
  }
}
```

---

## Task 1 — Repository Scaffold

Create the full directory structure. All paths relative to repo root.

Create `apps/airflow_manager/setup.py`:
```python
from setuptools import setup, find_packages

setup(
    name="airflow_manager",
    version="0.1.0",
    description="Frappe UI for Airflow configuration management",
    author="Fldrspro",
    packages=find_packages(),
    zip_safe=False,
    include_package_data=True,
    python_requires=">=3.11",
)
```

Create `apps/airflow_manager/MANIFEST.in`:
```
recursive-include airflow_manager *.json *.js *.css *.html *.txt *.md
```

Create `apps/airflow_manager/airflow_manager/__init__.py`:
```python
__version__ = "0.1.0"
```

Create `apps/airflow_manager/airflow_manager/hooks.py`:
```python
app_name = "airflow_manager"
app_title = "Airflow Manager"
app_publisher = "Fldrspro"
app_description = "Manage Airflow connections, variables, and configuration"
app_version = "0.1.0"

# Frappe module
app_include_js = []
app_include_css = []

fixtures = []

doc_events = {}
```

Create `apps/airflow_manager/airflow_manager/airflow_manager/__init__.py`:
```python
```

Create empty `apps/airflow_manager/airflow_manager/airflow_db/__init__.py`.

Create directories (touch empty `__init__.py` in each):
- `apps/airflow_manager/airflow_manager/airflow_manager/doctype/`
- `apps/airflow_manager/tests/`
- `apps/airflow_manager/tests/__init__.py`

Create `requirements.txt` at repo root:
```
sqlalchemy>=2.0,<3.0
psycopg2-binary>=2.9
cryptography>=41.0
frappe
```

Run: `python -m pytest tests/ -x -q` (will find 0 tests — that's OK)
Commit: `chore: scaffold airflow_manager Frappe app structure`

---

## Task 2 — Fernet Layer

Create `apps/airflow_manager/airflow_manager/airflow_db/fernet.py`:

```python
"""Fernet encryption/decryption using the same key as Airflow.

Reads AIRFLOW_FERNET_KEY from environment. This must be identical to
AIRFLOW__CORE__FERNET_KEY in the Airflow deployment.
"""
from __future__ import annotations

import os

from cryptography.fernet import Fernet, InvalidToken


def _get_key() -> bytes:
    key = os.environ.get("AIRFLOW_FERNET_KEY", "")
    if not key:
        raise RuntimeError("AIRFLOW_FERNET_KEY environment variable is not set")
    return key.encode() if isinstance(key, str) else key


def encrypt(value: str) -> str:
    """Encrypt a plain-text string. Returns URL-safe base64 Fernet token."""
    return Fernet(_get_key()).encrypt(value.encode()).decode()


def decrypt(value: str) -> str:
    """Decrypt a Fernet token. Raises InvalidToken if key is wrong."""
    return Fernet(_get_key()).decrypt(value.encode()).decode()


def is_encrypted(value: str) -> bool:
    """Heuristic: Fernet tokens start with 'gAAA'."""
    return value.startswith("gAAA")
```

Create `apps/airflow_manager/tests/test_fernet.py`:

```python
import os
import pytest
from cryptography.fernet import Fernet

# Generate a test key once for the whole module
_TEST_KEY = Fernet.generate_key().decode()


@pytest.fixture(autouse=True)
def set_fernet_key(monkeypatch):
    monkeypatch.setenv("AIRFLOW_FERNET_KEY", _TEST_KEY)


from airflow_manager.airflow_db.fernet import encrypt, decrypt, is_encrypted


def test_roundtrip():
    plain = "super-secret-api-token"
    assert decrypt(encrypt(plain)) == plain


def test_encrypt_returns_string():
    result = encrypt("hello")
    assert isinstance(result, str)
    assert len(result) > 0


def test_is_encrypted_true():
    assert is_encrypted(encrypt("something")) is True


def test_is_encrypted_false():
    assert is_encrypted("plain-text") is False


def test_missing_key_raises(monkeypatch):
    monkeypatch.delenv("AIRFLOW_FERNET_KEY", raising=False)
    with pytest.raises(RuntimeError, match="AIRFLOW_FERNET_KEY"):
        encrypt("value")
```

Run: `python -m pytest tests/test_fernet.py -v`
Commit: `feat: add Fernet encrypt/decrypt layer`

---

## Task 3 — SQLAlchemy Engine

Create `apps/airflow_manager/airflow_manager/airflow_db/connection.py`:

```python
"""SQLAlchemy engine connecting to Airflow's PostgreSQL metadata database.

Reads AIRFLOW_DB_URL from environment.
Example: postgresql+psycopg2://airflow_ui:pass@airflow-postgres:5432/airflow
"""
from __future__ import annotations

import os
from contextlib import contextmanager
from typing import Generator

from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, sessionmaker

_engine = None
_SessionFactory = None


def _get_engine():
    global _engine, _SessionFactory
    if _engine is None:
        url = os.environ.get("AIRFLOW_DB_URL", "")
        if not url:
            raise RuntimeError("AIRFLOW_DB_URL environment variable is not set")
        _engine = create_engine(url, pool_pre_ping=True)
        _SessionFactory = sessionmaker(bind=_engine)
    return _engine


@contextmanager
def get_session() -> Generator[Session, None, None]:
    """Yield a SQLAlchemy session, commit on success, rollback on error."""
    engine = _get_engine()
    factory = sessionmaker(bind=engine)
    session = factory()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def check_connection() -> bool:
    """Return True if Airflow DB is reachable."""
    try:
        with get_session() as s:
            s.execute(text("SELECT 1"))
        return True
    except Exception:
        return False
```

Create `apps/airflow_manager/tests/test_connection.py`:

```python
import os
import pytest
from unittest.mock import patch, MagicMock


def test_missing_url_raises(monkeypatch):
    import importlib
    import airflow_manager.airflow_db.connection as conn_module
    monkeypatch.delenv("AIRFLOW_DB_URL", raising=False)
    conn_module._engine = None  # reset singleton
    with pytest.raises(RuntimeError, match="AIRFLOW_DB_URL"):
        conn_module._get_engine()
    conn_module._engine = None


def test_check_connection_returns_false_on_bad_url(monkeypatch):
    import airflow_manager.airflow_db.connection as conn_module
    monkeypatch.setenv("AIRFLOW_DB_URL", "postgresql+psycopg2://bad:bad@localhost:9999/noexist")
    conn_module._engine = None
    result = conn_module.check_connection()
    assert result is False
    conn_module._engine = None
```

Run: `python -m pytest tests/test_connection.py -v`
Commit: `feat: add SQLAlchemy engine for Airflow PostgreSQL`

---

## Task 4 — Connection Manager

Create `apps/airflow_manager/airflow_manager/airflow_db/connection_manager.py`:

```python
"""CRUD operations on Airflow's `connection` table.

Passwords are Fernet-encrypted at rest. On read for list views, password is
never returned. On read for edit form, password is decrypted and returned.
On write, if password is blank/None, the existing encrypted value is preserved.
"""
from __future__ import annotations

from typing import Any

from sqlalchemy import text

from airflow_manager.airflow_db.connection import get_session
from airflow_manager.airflow_db.fernet import decrypt, encrypt


def _row_to_dict(row, include_password: bool = False) -> dict[str, Any]:
    d = {
        "conn_id": row.conn_id,
        "conn_type": row.conn_type or "",
        "description": row.description or "",
        "host": row.host or "",
        "schema": row.schema or "",
        "login": row.login or "",
        "port": row.port,
        "extra": row.extra or "",
        "is_encrypted": bool(row.is_encrypted),
    }
    if include_password:
        raw = row.password or ""
        if raw and row.is_encrypted:
            try:
                d["password"] = decrypt(raw)
            except Exception:
                d["password"] = ""
        else:
            d["password"] = raw
    return d


def list_connections(
    conn_type: str | None = None,
    search: str | None = None,
    limit: int = 50,
    offset: int = 0,
) -> list[dict]:
    """Return connections without passwords."""
    with get_session() as s:
        filters = []
        params: dict[str, Any] = {"limit": limit, "offset": offset}
        if conn_type:
            filters.append("conn_type = :conn_type")
            params["conn_type"] = conn_type
        if search:
            filters.append("conn_id ILIKE :search")
            params["search"] = f"%{search}%"
        where = ("WHERE " + " AND ".join(filters)) if filters else ""
        sql = text(
            f"SELECT conn_id, conn_type, description, host, schema, login, port, "
            f"is_encrypted FROM connection {where} ORDER BY conn_id LIMIT :limit OFFSET :offset"
        )
        rows = s.execute(sql, params).fetchall()
        return [
            {
                "conn_id": r.conn_id,
                "conn_type": r.conn_type or "",
                "description": r.description or "",
                "host": r.host or "",
                "login": r.login or "",
                "port": r.port,
            }
            for r in rows
        ]


def count_connections(conn_type: str | None = None, search: str | None = None) -> int:
    with get_session() as s:
        filters = []
        params: dict[str, Any] = {}
        if conn_type:
            filters.append("conn_type = :conn_type")
            params["conn_type"] = conn_type
        if search:
            filters.append("conn_id ILIKE :search")
            params["search"] = f"%{search}%"
        where = ("WHERE " + " AND ".join(filters)) if filters else ""
        sql = text(f"SELECT COUNT(*) FROM connection {where}")
        return s.execute(sql, params).scalar() or 0


def get_connection(conn_id: str) -> dict | None:
    """Return single connection with decrypted password."""
    with get_session() as s:
        sql = text(
            "SELECT conn_id, conn_type, description, host, schema, login, password, "
            "port, is_encrypted, extra FROM connection WHERE conn_id = :conn_id"
        )
        row = s.execute(sql, {"conn_id": conn_id}).fetchone()
        if row is None:
            return None
        return _row_to_dict(row, include_password=True)


def upsert_connection(data: dict) -> None:
    """Insert or update a connection. Password blank → preserve existing encrypted value."""
    conn_id = data["conn_id"]
    new_password = data.get("password") or ""

    with get_session() as s:
        existing = s.execute(
            text("SELECT password, is_encrypted FROM connection WHERE conn_id = :id"),
            {"id": conn_id},
        ).fetchone()

        if new_password:
            encrypted_password = encrypt(new_password)
            is_enc = True
        elif existing:
            encrypted_password = existing.password or ""
            is_enc = bool(existing.is_encrypted)
        else:
            encrypted_password = ""
            is_enc = False

        if existing:
            s.execute(
                text(
                    "UPDATE connection SET conn_type=:conn_type, description=:description, "
                    "host=:host, schema=:schema, login=:login, password=:password, "
                    "port=:port, is_encrypted=:is_encrypted WHERE conn_id=:conn_id"
                ),
                {
                    "conn_id": conn_id,
                    "conn_type": data.get("conn_type", ""),
                    "description": data.get("description", ""),
                    "host": data.get("host", ""),
                    "schema": data.get("schema", ""),
                    "login": data.get("login", ""),
                    "password": encrypted_password,
                    "port": data.get("port"),
                    "is_encrypted": is_enc,
                },
            )
        else:
            s.execute(
                text(
                    "INSERT INTO connection (conn_id, conn_type, description, host, schema, "
                    "login, password, port, is_encrypted, is_extra_encrypted) "
                    "VALUES (:conn_id, :conn_type, :description, :host, :schema, "
                    ":login, :password, :port, :is_encrypted, false)"
                ),
                {
                    "conn_id": conn_id,
                    "conn_type": data.get("conn_type", ""),
                    "description": data.get("description", ""),
                    "host": data.get("host", ""),
                    "schema": data.get("schema", ""),
                    "login": data.get("login", ""),
                    "password": encrypted_password,
                    "port": data.get("port"),
                    "is_encrypted": is_enc,
                },
            )


def delete_connection(conn_id: str) -> None:
    with get_session() as s:
        s.execute(text("DELETE FROM connection WHERE conn_id = :id"), {"id": conn_id})
```

Create `apps/airflow_manager/tests/test_connection_manager.py`:

```python
from unittest.mock import patch, MagicMock, call
import pytest
from cryptography.fernet import Fernet

_KEY = Fernet.generate_key().decode()


@pytest.fixture(autouse=True)
def set_env(monkeypatch):
    monkeypatch.setenv("AIRFLOW_FERNET_KEY", _KEY)
    monkeypatch.setenv("AIRFLOW_DB_URL", "postgresql+psycopg2://x:x@localhost/x")


def _mock_session(rows=None, scalar=None):
    """Return a mock context manager that yields a mock session."""
    session = MagicMock()
    session.execute.return_value.fetchall.return_value = rows or []
    session.execute.return_value.fetchone.return_value = None
    session.execute.return_value.scalar.return_value = scalar or 0
    cm = MagicMock()
    cm.__enter__ = MagicMock(return_value=session)
    cm.__exit__ = MagicMock(return_value=False)
    return cm, session


def test_list_connections_returns_list():
    cm, session = _mock_session(rows=[])
    with patch("airflow_manager.airflow_db.connection_manager.get_session", return_value=cm):
        from airflow_manager.airflow_db.connection_manager import list_connections
        result = list_connections()
    assert isinstance(result, list)


def test_upsert_encrypts_password():
    from cryptography.fernet import Fernet as F
    cm, session = _mock_session()
    session.execute.return_value.fetchone.return_value = None  # no existing row
    with patch("airflow_manager.airflow_db.connection_manager.get_session", return_value=cm):
        from airflow_manager.airflow_db.connection_manager import upsert_connection
        upsert_connection({"conn_id": "test_conn", "password": "secret"})
    # Find the INSERT call and verify password is encrypted
    all_calls = session.execute.call_args_list
    insert_call = [c for c in all_calls if "INSERT" in str(c)]
    assert insert_call, "No INSERT call found"
    params = insert_call[0][0][1]
    assert params["password"] != "secret"
    assert params["is_encrypted"] is True
    # Verify it decrypts correctly
    from airflow_manager.airflow_db.fernet import decrypt
    assert decrypt(params["password"]) == "secret"


def test_upsert_preserves_existing_password_when_blank():
    existing = MagicMock()
    existing.password = "existing_encrypted"
    existing.is_encrypted = True
    cm, session = _mock_session()
    session.execute.return_value.fetchone.return_value = existing
    with patch("airflow_manager.airflow_db.connection_manager.get_session", return_value=cm):
        from airflow_manager.airflow_db.connection_manager import upsert_connection
        upsert_connection({"conn_id": "test_conn", "password": ""})
    update_call = [c for c in session.execute.call_args_list if "UPDATE" in str(c)]
    assert update_call
    params = update_call[0][0][1]
    assert params["password"] == "existing_encrypted"


def test_get_connection_decrypts_password():
    from airflow_manager.airflow_db.fernet import encrypt
    encrypted_pwd = encrypt("real-secret")
    row = MagicMock()
    row.conn_id = "test"
    row.conn_type = "generic"
    row.description = ""
    row.host = ""
    row.schema = ""
    row.login = ""
    row.password = encrypted_pwd
    row.port = None
    row.is_encrypted = True
    row.extra = ""
    cm, session = _mock_session()
    session.execute.return_value.fetchone.return_value = row
    with patch("airflow_manager.airflow_db.connection_manager.get_session", return_value=cm):
        from airflow_manager.airflow_db.connection_manager import get_connection
        result = get_connection("test")
    assert result["password"] == "real-secret"
```

Run: `python -m pytest tests/test_connection_manager.py -v`
Commit: `feat: add connection_manager CRUD with Fernet encrypt/decrypt`

---

## Task 5 — Variable Manager

Create `apps/airflow_manager/airflow_manager/airflow_db/variable_manager.py`:

```python
"""CRUD operations on Airflow's `variable` table.

Variables for CLIENT_REGISTRY and client_config_{id} are stored as plain JSON
(is_encrypted=false). We never encrypt Variables — they contain structural config,
not secrets.
"""
from __future__ import annotations

from sqlalchemy import text

from airflow_manager.airflow_db.connection import get_session


def get_variable(key: str) -> str | None:
    """Return variable value string, or None if not found."""
    with get_session() as s:
        row = s.execute(
            text("SELECT val FROM variable WHERE key = :key"),
            {"key": key},
        ).fetchone()
        return row.val if row else None


def set_variable(key: str, value: str, description: str = "") -> None:
    """Insert or update a variable. Value stored as plain text (not encrypted)."""
    with get_session() as s:
        existing = s.execute(
            text("SELECT key FROM variable WHERE key = :key"), {"key": key}
        ).fetchone()
        if existing:
            s.execute(
                text("UPDATE variable SET val=:val, description=:desc WHERE key=:key"),
                {"key": key, "val": value, "desc": description},
            )
        else:
            s.execute(
                text(
                    "INSERT INTO variable (key, val, description, is_encrypted) "
                    "VALUES (:key, :val, :desc, false)"
                ),
                {"key": key, "val": value, "desc": description},
            )


def delete_variable(key: str) -> None:
    with get_session() as s:
        s.execute(text("DELETE FROM variable WHERE key = :key"), {"key": key})


def list_variables(search: str | None = None) -> list[dict]:
    with get_session() as s:
        if search:
            rows = s.execute(
                text("SELECT key, description FROM variable WHERE key ILIKE :s ORDER BY key"),
                {"s": f"%{search}%"},
            ).fetchall()
        else:
            rows = s.execute(
                text("SELECT key, description FROM variable ORDER BY key")
            ).fetchall()
        return [{"key": r.key, "description": r.description or ""} for r in rows]
```

Create `apps/airflow_manager/tests/test_variable_manager.py`:

```python
from unittest.mock import patch, MagicMock
import pytest


@pytest.fixture(autouse=True)
def set_env(monkeypatch):
    monkeypatch.setenv("AIRFLOW_DB_URL", "postgresql+psycopg2://x:x@localhost/x")


def _mock_session(fetchone_return=None, fetchall_return=None):
    session = MagicMock()
    session.execute.return_value.fetchone.return_value = fetchone_return
    session.execute.return_value.fetchall.return_value = fetchall_return or []
    cm = MagicMock()
    cm.__enter__ = MagicMock(return_value=session)
    cm.__exit__ = MagicMock(return_value=False)
    return cm, session


def test_get_variable_returns_value():
    row = MagicMock()
    row.val = '{"key": "value"}'
    cm, _ = _mock_session(fetchone_return=row)
    with patch("airflow_manager.airflow_db.variable_manager.get_session", return_value=cm):
        from airflow_manager.airflow_db.variable_manager import get_variable
        assert get_variable("MY_VAR") == '{"key": "value"}'


def test_get_variable_returns_none_when_missing():
    cm, _ = _mock_session(fetchone_return=None)
    with patch("airflow_manager.airflow_db.variable_manager.get_session", return_value=cm):
        from airflow_manager.airflow_db.variable_manager import get_variable
        assert get_variable("MISSING") is None


def test_set_variable_inserts_when_new():
    cm, session = _mock_session(fetchone_return=None)
    with patch("airflow_manager.airflow_db.variable_manager.get_session", return_value=cm):
        from airflow_manager.airflow_db.variable_manager import set_variable
        set_variable("NEW_VAR", '{"a": 1}')
    calls = [str(c) for c in session.execute.call_args_list]
    assert any("INSERT" in c for c in calls)


def test_set_variable_updates_when_existing():
    existing = MagicMock()
    existing.key = "EXISTING"
    cm, session = _mock_session(fetchone_return=existing)
    with patch("airflow_manager.airflow_db.variable_manager.get_session", return_value=cm):
        from airflow_manager.airflow_db.variable_manager import set_variable
        set_variable("EXISTING", '{"b": 2}')
    calls = [str(c) for c in session.execute.call_args_list]
    assert any("UPDATE" in c for c in calls)
```

Run: `python -m pytest tests/test_variable_manager.py -v`
Commit: `feat: add variable_manager for Airflow variable table`

---

## Task 6 — DAG Reader

Create `apps/airflow_manager/airflow_manager/airflow_db/dag_reader.py`:

```python
"""Read-only access to Airflow's `dag` table."""
from __future__ import annotations

from sqlalchemy import text

from airflow_manager.airflow_db.connection import get_session


def list_dags(paused: bool | None = None) -> list[dict]:
    """Return list of DAGs, optionally filtered by paused state."""
    with get_session() as s:
        if paused is None:
            rows = s.execute(
                text("SELECT dag_id, is_paused, schedule_interval, last_parsed_time "
                     "FROM dag ORDER BY dag_id")
            ).fetchall()
        else:
            rows = s.execute(
                text("SELECT dag_id, is_paused, schedule_interval, last_parsed_time "
                     "FROM dag WHERE is_paused = :paused ORDER BY dag_id"),
                {"paused": paused},
            ).fetchall()
        return [
            {
                "dag_id": r.dag_id,
                "is_paused": bool(r.is_paused),
                "schedule_interval": r.schedule_interval or "",
                "last_parsed_time": str(r.last_parsed_time) if r.last_parsed_time else "",
            }
            for r in rows
        ]


def get_dag(dag_id: str) -> dict | None:
    with get_session() as s:
        row = s.execute(
            text("SELECT dag_id, is_paused, schedule_interval, last_parsed_time "
                 "FROM dag WHERE dag_id = :dag_id"),
            {"dag_id": dag_id},
        ).fetchone()
        if row is None:
            return None
        return {
            "dag_id": row.dag_id,
            "is_paused": bool(row.is_paused),
            "schedule_interval": row.schedule_interval or "",
            "last_parsed_time": str(row.last_parsed_time) if row.last_parsed_time else "",
        }


def count_dags(paused: bool | None = None) -> int:
    with get_session() as s:
        if paused is None:
            return s.execute(text("SELECT COUNT(*) FROM dag")).scalar() or 0
        return s.execute(
            text("SELECT COUNT(*) FROM dag WHERE is_paused = :p"), {"p": paused}
        ).scalar() or 0
```

Run: `python -m pytest tests/ -x -q`
Commit: `feat: add dag_reader for read-only DAG list`

---

## Task 7 — Registry Sync

Create `apps/airflow_manager/airflow_manager/airflow_db/registry_sync.py`:

```python
"""Serialize AM Client + AM Cabinet records into CLIENT_REGISTRY Airflow Variable.

Called after every Client or Cabinet save. Rebuilds the full registry from scratch.
This is intentionally simple: no diffing, just full rebuild.
"""
from __future__ import annotations

import json

from airflow_manager.airflow_db.variable_manager import set_variable

REGISTRY_KEY = "CLIENT_REGISTRY"


def build_registry(clients: list[dict]) -> dict:
    """Build CLIENT_REGISTRY dict from a list of client dicts.

    Each client dict has:
      id, display_name, db, cabinets: list[dict(slug, display_name, platform, active, dags)]
    """
    registry: dict = {}
    for client in clients:
        entry: dict = {
            "display_name": client["display_name"],
            "db": client["db"],
        }
        for platform in ("wb", "oz", "ms", "ym"):
            cabs = [c for c in client.get("cabinets", []) if c["platform"] == platform]
            if cabs:
                entry[platform] = {
                    c["slug"]: {
                        "display_name": c["display_name"],
                        "active": c["active"],
                        "dags": [d.strip() for d in c.get("dags", "").split(",") if d.strip()],
                    }
                    for c in cabs
                }
        registry[client["id"]] = entry
    return registry


def rebuild_client_registry(clients: list[dict]) -> None:
    """Serialize clients to JSON and write to CLIENT_REGISTRY Variable."""
    registry = build_registry(clients)
    set_variable(REGISTRY_KEY, json.dumps(registry, ensure_ascii=False), description="")
```

Create `apps/airflow_manager/tests/test_registry_sync.py`:

```python
import json
from unittest.mock import patch, MagicMock
from airflow_manager.airflow_db.registry_sync import build_registry, rebuild_client_registry


CLIENTS = [
    {
        "id": "efendem",
        "display_name": "Efendem",
        "db": "05efendem_postgres",
        "cabinets": [
            {"slug": "filippov_sv", "display_name": "Филиппов", "platform": "wb",
             "active": True, "dags": "wb_orders_etl_dag, wb_stocks_etl_dag"},
            {"slug": "pharm_legend", "display_name": "Pharm", "platform": "wb",
             "active": True, "dags": ""},
            {"slug": "filippov_oz", "display_name": "Филиппов OZ", "platform": "oz",
             "active": False, "dags": ""},
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
        "wb_orders_etl_dag", "wb_stocks_etl_dag"
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
```

Run: `python -m pytest tests/test_registry_sync.py -v`
Commit: `feat: add registry_sync — serializes Client+Cabinet to CLIENT_REGISTRY Variable`

---

## Task 8 — Config Sync

Create `apps/airflow_manager/airflow_manager/airflow_db/config_sync.py`:

```python
"""Serialize AM Table Config records into client_config_{id} Airflow Variable.

Three-level structure: dag_id → _default|cabinet_slug → table_name → config dict.
"""
from __future__ import annotations

import json

from airflow_manager.airflow_db.variable_manager import set_variable


def build_client_config(configs: list[dict]) -> dict:
    """Build client_config dict from a list of TableConfig records.

    Each config dict has:
      dag_id, scope ("_default" or "cabinet"), cabinet_slug,
      table_name, enabled, load_strategy, incremental_days, auto_alter,
      exclude_fields: list[str], rename_fields: dict[str,str],
      targets: list[dict(db, table)]
    """
    result: dict = {}

    for cfg in configs:
        dag_id = cfg["dag_id"]
        scope_key = "_default" if cfg["scope"] == "_default" else cfg["cabinet_slug"]
        table_name = cfg["table_name"]

        table_cfg: dict = {
            "enabled": cfg.get("enabled", True),
            "load_strategy": cfg.get("load_strategy", "append"),
            "incremental_days": cfg.get("incremental_days"),
            "auto_alter": cfg.get("auto_alter", False),
            "exclude_fields": cfg.get("exclude_fields", []),
            "rename_fields": cfg.get("rename_fields", {}),
            "targets": cfg.get("targets", []),
        }

        result.setdefault(dag_id, {}).setdefault(scope_key, {})[table_name] = table_cfg

    return result


def rebuild_client_config(client_id: str, configs: list[dict]) -> None:
    """Serialize configs to JSON and write to client_config_{client_id} Variable."""
    built = build_client_config(configs)
    key = f"client_config_{client_id}"
    set_variable(key, json.dumps(built, ensure_ascii=False), description="")
```

Create `apps/airflow_manager/tests/test_config_sync.py`:

```python
import json
from unittest.mock import patch
from airflow_manager.airflow_db.config_sync import build_client_config, rebuild_client_config

CONFIGS = [
    {
        "dag_id": "wb_orders_etl_dag",
        "scope": "_default",
        "cabinet_slug": "",
        "table_name": "wb_orders",
        "enabled": True,
        "load_strategy": "append",
        "incremental_days": 15,
        "auto_alter": False,
        "exclude_fields": ["internal_field"],
        "rename_fields": {"old": "new"},
        "targets": [{"db": None, "table": "wb_orders"}],
    },
    {
        "dag_id": "wb_orders_etl_dag",
        "scope": "cabinet",
        "cabinet_slug": "pharm_legend",
        "table_name": "wb_orders",
        "enabled": True,
        "load_strategy": "append",
        "incremental_days": 30,
        "auto_alter": False,
        "exclude_fields": [],
        "rename_fields": {},
        "targets": [],
    },
]


def test_build_config_structure():
    result = build_client_config(CONFIGS)
    assert "wb_orders_etl_dag" in result
    dag = result["wb_orders_etl_dag"]
    assert "_default" in dag
    assert "pharm_legend" in dag


def test_build_config_default_table():
    result = build_client_config(CONFIGS)
    default_cfg = result["wb_orders_etl_dag"]["_default"]["wb_orders"]
    assert default_cfg["incremental_days"] == 15
    assert default_cfg["exclude_fields"] == ["internal_field"]
    assert default_cfg["rename_fields"] == {"old": "new"}


def test_build_config_cabinet_override():
    result = build_client_config(CONFIGS)
    cab_cfg = result["wb_orders_etl_dag"]["pharm_legend"]["wb_orders"]
    assert cab_cfg["incremental_days"] == 30


def test_rebuild_calls_set_variable_with_correct_key():
    with patch("airflow_manager.airflow_db.config_sync.set_variable") as mock_set:
        rebuild_client_config("efendem", CONFIGS)
    mock_set.assert_called_once()
    key = mock_set.call_args[0][0]
    assert key == "client_config_efendem"
    val = json.loads(mock_set.call_args[0][1])
    assert "wb_orders_etl_dag" in val
```

Run: `python -m pytest tests/test_config_sync.py -v`
Commit: `feat: add config_sync — serializes TableConfig to client_config_{id} Variable`

---

## Task 9 — AM Client + AM Cabinet DocTypes

Create `apps/airflow_manager/airflow_manager/airflow_manager/doctype/am_client/am_client.json`:

```json
{
  "name": "AM Client",
  "doctype": "DocType",
  "module": "Airflow Manager",
  "is_submittable": 0,
  "autoname": "field:client_id",
  "title_field": "display_name",
  "fields": [
    {"fieldname": "client_id", "fieldtype": "Data", "label": "Client ID", "reqd": 1, "unique": 1, "in_list_view": 1},
    {"fieldname": "display_name", "fieldtype": "Data", "label": "Display Name", "reqd": 1, "in_list_view": 1},
    {"fieldname": "db_connection", "fieldtype": "Link", "label": "Database Connection", "options": "AM Database Connection", "in_list_view": 1},
    {"fieldname": "cabinets", "fieldtype": "Table", "label": "Cabinets", "options": "AM Cabinet"}
  ],
  "permissions": [{"role": "System Manager", "read": 1, "write": 1, "create": 1, "delete": 1}]
}
```

Create `apps/airflow_manager/airflow_manager/airflow_manager/doctype/am_client/am_client.py`:

```python
import frappe
from frappe.model.document import Document


class AMClient(Document):
    def after_save(self):
        _trigger_registry_sync()

    def on_trash(self):
        _trigger_registry_sync()


def _trigger_registry_sync():
    from airflow_manager.airflow_db.registry_sync import rebuild_client_registry

    clients_raw = frappe.get_all(
        "AM Client",
        fields=["client_id", "display_name", "db_connection"],
    )
    clients = []
    for c in clients_raw:
        cabinets_raw = frappe.get_all(
            "AM Cabinet",
            filters={"client": c["client_id"]},
            fields=["slug", "display_name", "platform", "active", "dags"],
        )
        clients.append({
            "id": c["client_id"],
            "display_name": c["display_name"],
            "db": c.get("db_connection") or "",
            "cabinets": cabinets_raw,
        })
    rebuild_client_registry(clients)
```

Create `apps/airflow_manager/airflow_manager/airflow_manager/doctype/am_cabinet/am_cabinet.json`:

```json
{
  "name": "AM Cabinet",
  "doctype": "DocType",
  "module": "Airflow Manager",
  "is_submittable": 0,
  "autoname": "format:{client}-{platform}-{slug}",
  "title_field": "display_name",
  "fields": [
    {"fieldname": "slug", "fieldtype": "Data", "label": "Slug", "reqd": 1, "in_list_view": 1},
    {"fieldname": "display_name", "fieldtype": "Data", "label": "Display Name", "reqd": 1, "in_list_view": 1},
    {"fieldname": "client", "fieldtype": "Link", "label": "Client", "options": "AM Client", "reqd": 1, "in_list_view": 1},
    {"fieldname": "platform", "fieldtype": "Select", "label": "Platform", "options": "wb\noz\nms\nym", "reqd": 1, "in_list_view": 1},
    {"fieldname": "active", "fieldtype": "Check", "label": "Active", "default": "1", "in_list_view": 1},
    {"fieldname": "dags", "fieldtype": "Small Text", "label": "DAG IDs (comma-separated)"}
  ],
  "permissions": [{"role": "System Manager", "read": 1, "write": 1, "create": 1, "delete": 1}]
}
```

Create `apps/airflow_manager/airflow_manager/airflow_manager/doctype/am_cabinet/am_cabinet.py`:

```python
import frappe
from frappe.model.document import Document


class AMCabinet(Document):
    def after_save(self):
        from airflow_manager.airflow_manager.doctype.am_client.am_client import _trigger_registry_sync
        _trigger_registry_sync()

    def on_trash(self):
        from airflow_manager.airflow_manager.doctype.am_client.am_client import _trigger_registry_sync
        _trigger_registry_sync()
```

Run: `python -m pytest tests/ -x -q`
Commit: `feat: add AM Client and AM Cabinet DocTypes with registry sync`

---

## Task 10 — AM Airflow Connection + AM Database Connection (Virtual)

Create `apps/airflow_manager/airflow_manager/airflow_manager/doctype/am_airflow_connection/am_airflow_connection.json`:

```json
{
  "name": "AM Airflow Connection",
  "doctype": "DocType",
  "module": "Airflow Manager",
  "is_virtual": 1,
  "autoname": "field:conn_id",
  "title_field": "conn_id",
  "fields": [
    {"fieldname": "conn_id", "fieldtype": "Data", "label": "Connection ID", "reqd": 1, "in_list_view": 1},
    {"fieldname": "conn_type", "fieldtype": "Select", "label": "Type",
     "options": "wb_token\noz_seller\noz_performance\nms_token\nother", "in_list_view": 1},
    {"fieldname": "cabinet", "fieldtype": "Link", "label": "Cabinet", "options": "AM Cabinet", "in_list_view": 1},
    {"fieldname": "description", "fieldtype": "Small Text", "label": "Description"},
    {"fieldname": "api_token", "fieldtype": "Password", "label": "API Token"},
    {"fieldname": "client_seller_id", "fieldtype": "Data", "label": "Client Seller ID"},
    {"fieldname": "perf_id", "fieldtype": "Data", "label": "Performance Client ID"},
    {"fieldname": "perf_secret", "fieldtype": "Password", "label": "Performance Secret"},
    {"fieldname": "ms_token", "fieldtype": "Password", "label": "MoySklad Token"}
  ],
  "permissions": [{"role": "System Manager", "read": 1, "write": 1, "create": 1, "delete": 1}]
}
```

Create `apps/airflow_manager/airflow_manager/airflow_manager/doctype/am_airflow_connection/am_airflow_connection.py`:

```python
"""Virtual DocType — reads and writes directly to Airflow's connection table."""
import frappe
from frappe.model.document import Document
from airflow_manager.airflow_db.connection_manager import (
    list_connections, get_connection, upsert_connection, delete_connection, count_connections,
)

_PLATFORM_FIELDS = {
    "wb_token": {"api_token": "password"},
    "oz_seller": {"api_token": "password", "client_seller_id": "login"},
    "oz_performance": {"perf_id": "login", "perf_secret": "password"},
    "ms_token": {"ms_token": "password"},
}


def _to_airflow_payload(doc_data: dict) -> dict:
    """Map form fields to Airflow connection columns."""
    conn_type = doc_data.get("conn_type", "other")
    mapping = _PLATFORM_FIELDS.get(conn_type, {})
    payload = {
        "conn_id": doc_data["conn_id"],
        "conn_type": conn_type,
        "description": doc_data.get("description", ""),
    }
    for form_field, airflow_field in mapping.items():
        val = doc_data.get(form_field) or ""
        if airflow_field == "password":
            payload["password"] = val
        elif airflow_field == "login":
            payload["login"] = val
    return payload


def _from_airflow_row(row: dict, conn_type_hint: str = "") -> dict:
    """Map Airflow connection row to form fields."""
    conn_type = row.get("conn_type") or conn_type_hint
    mapping = _PLATFORM_FIELDS.get(conn_type, {})
    doc = {
        "conn_id": row["conn_id"],
        "conn_type": conn_type,
        "description": row.get("description", ""),
    }
    for form_field, airflow_field in mapping.items():
        if airflow_field == "password":
            doc[form_field] = None  # never expose password in form
        elif airflow_field == "login":
            doc[form_field] = row.get("login", "")
    return doc


class AMAirflowConnection(Document):
    def load_from_db(self):
        row = get_connection(self.name)
        if not row:
            frappe.throw(f"Connection {self.name} not found in Airflow")
        self.update(_from_airflow_row(row))

    def db_insert(self, *args, **kwargs):
        upsert_connection(_to_airflow_payload(self.as_dict()))

    def db_update(self, *args, **kwargs):
        upsert_connection(_to_airflow_payload(self.as_dict()))

    def delete(self):
        delete_connection(self.name)

    @staticmethod
    def get_list(args):
        limit = args.get("page_length") or 20
        offset = ((args.get("start") or 0))
        return list_connections(limit=limit, offset=offset)

    @staticmethod
    def get_count(args):
        return count_connections()

    @staticmethod
    def get_stats(args):
        return {}
```

Create `apps/airflow_manager/airflow_manager/airflow_manager/doctype/am_database_connection/am_database_connection.json`:

```json
{
  "name": "AM Database Connection",
  "doctype": "DocType",
  "module": "Airflow Manager",
  "is_virtual": 1,
  "autoname": "field:conn_id",
  "title_field": "conn_id",
  "fields": [
    {"fieldname": "conn_id", "fieldtype": "Data", "label": "Connection ID", "reqd": 1, "in_list_view": 1},
    {"fieldname": "host", "fieldtype": "Data", "label": "Host", "in_list_view": 1},
    {"fieldname": "port", "fieldtype": "Int", "label": "Port", "default": "5432"},
    {"fieldname": "schema", "fieldtype": "Data", "label": "Database Name"},
    {"fieldname": "login", "fieldtype": "Data", "label": "Login"},
    {"fieldname": "password", "fieldtype": "Password", "label": "Password"},
    {"fieldname": "description", "fieldtype": "Small Text", "label": "Description"}
  ],
  "permissions": [{"role": "System Manager", "read": 1, "write": 1, "create": 1, "delete": 1}]
}
```

Create `apps/airflow_manager/airflow_manager/airflow_manager/doctype/am_database_connection/am_database_connection.py`:

```python
import frappe
from frappe.model.document import Document
from airflow_manager.airflow_db.connection_manager import (
    list_connections, get_connection, upsert_connection, delete_connection, count_connections,
)


class AMDatabaseConnection(Document):
    def load_from_db(self):
        row = get_connection(self.name)
        if not row:
            frappe.throw(f"Connection {self.name} not found in Airflow")
        self.update({
            "conn_id": row["conn_id"],
            "host": row.get("host", ""),
            "port": row.get("port"),
            "schema": row.get("schema", ""),
            "login": row.get("login", ""),
            "password": None,
            "description": row.get("description", ""),
        })

    def db_insert(self, *args, **kwargs):
        upsert_connection({
            "conn_id": self.conn_id,
            "conn_type": "postgres",
            "host": self.host or "",
            "port": self.port,
            "schema": self.schema or "",
            "login": self.login or "",
            "password": self.get_password("password", raise_exception=False) or "",
            "description": self.description or "",
        })

    def db_update(self, *args, **kwargs):
        self.db_insert()

    def delete(self):
        delete_connection(self.name)

    @staticmethod
    def get_list(args):
        return list_connections(conn_type="postgres",
                                limit=args.get("page_length") or 20,
                                offset=args.get("start") or 0)

    @staticmethod
    def get_count(args):
        return count_connections(conn_type="postgres")

    @staticmethod
    def get_stats(args):
        return {}
```

Run: `python -m pytest tests/ -x -q`
Commit: `feat: add AM Airflow Connection and AM Database Connection Virtual DocTypes`

---

## Task 11 — AM Table Config + Child Tables

Create `apps/airflow_manager/airflow_manager/airflow_manager/doctype/am_table_config_exclude_field/am_table_config_exclude_field.json`:

```json
{
  "name": "AM Table Config Exclude Field",
  "doctype": "DocType",
  "module": "Airflow Manager",
  "istable": 1,
  "fields": [
    {"fieldname": "field_name", "fieldtype": "Data", "label": "Field Name", "reqd": 1, "in_list_view": 1}
  ]
}
```

Create `apps/airflow_manager/airflow_manager/airflow_manager/doctype/am_table_config_rename_field/am_table_config_rename_field.json`:

```json
{
  "name": "AM Table Config Rename Field",
  "doctype": "DocType",
  "module": "Airflow Manager",
  "istable": 1,
  "fields": [
    {"fieldname": "source_field", "fieldtype": "Data", "label": "Source Field", "reqd": 1, "in_list_view": 1},
    {"fieldname": "target_field", "fieldtype": "Data", "label": "Target Field", "reqd": 1, "in_list_view": 1}
  ]
}
```

Create `apps/airflow_manager/airflow_manager/airflow_manager/doctype/am_table_config_target/am_table_config_target.json`:

```json
{
  "name": "AM Table Config Target",
  "doctype": "DocType",
  "module": "Airflow Manager",
  "istable": 1,
  "fields": [
    {"fieldname": "db_connection", "fieldtype": "Link", "label": "DB Connection", "options": "AM Database Connection", "in_list_view": 1},
    {"fieldname": "table_name", "fieldtype": "Data", "label": "Table Name", "reqd": 1, "in_list_view": 1}
  ]
}
```

Create `apps/airflow_manager/airflow_manager/airflow_manager/doctype/am_table_config/am_table_config.json`:

```json
{
  "name": "AM Table Config",
  "doctype": "DocType",
  "module": "Airflow Manager",
  "is_submittable": 0,
  "fields": [
    {"fieldname": "client", "fieldtype": "Link", "label": "Client", "options": "AM Client", "reqd": 1, "in_list_view": 1},
    {"fieldname": "dag_id", "fieldtype": "Data", "label": "DAG ID", "reqd": 1, "in_list_view": 1},
    {"fieldname": "scope", "fieldtype": "Select", "label": "Scope", "options": "_default\ncabinet", "reqd": 1, "default": "_default", "in_list_view": 1},
    {"fieldname": "cabinet", "fieldtype": "Link", "label": "Cabinet", "options": "AM Cabinet",
     "depends_on": "eval:doc.scope=='cabinet'"},
    {"fieldname": "table_name", "fieldtype": "Data", "label": "Table Name", "reqd": 1, "in_list_view": 1},
    {"fieldname": "enabled", "fieldtype": "Check", "label": "Enabled", "default": "1"},
    {"fieldname": "load_strategy", "fieldtype": "Select", "label": "Load Strategy",
     "options": "append\ndelete_and_append", "default": "append"},
    {"fieldname": "incremental_days", "fieldtype": "Int", "label": "Incremental Days"},
    {"fieldname": "auto_alter", "fieldtype": "Check", "label": "Auto ALTER"},
    {"fieldname": "exclude_fields", "fieldtype": "Table", "label": "Exclude Fields",
     "options": "AM Table Config Exclude Field"},
    {"fieldname": "rename_fields", "fieldtype": "Table", "label": "Rename Fields",
     "options": "AM Table Config Rename Field"},
    {"fieldname": "targets", "fieldtype": "Table", "label": "Write Targets",
     "options": "AM Table Config Target"}
  ],
  "permissions": [{"role": "System Manager", "read": 1, "write": 1, "create": 1, "delete": 1}]
}
```

Create `apps/airflow_manager/airflow_manager/airflow_manager/doctype/am_table_config/am_table_config.py`:

```python
import frappe
from frappe.model.document import Document


class AMTableConfig(Document):
    def after_save(self):
        _trigger_config_sync(self.client)

    def on_trash(self):
        _trigger_config_sync(self.client)


def _trigger_config_sync(client_id: str):
    from airflow_manager.airflow_db.config_sync import rebuild_client_config

    raw_configs = frappe.get_all(
        "AM Table Config",
        filters={"client": client_id},
        fields=["dag_id", "scope", "cabinet", "table_name", "enabled",
                "load_strategy", "incremental_days", "auto_alter"],
    )

    configs = []
    for cfg in raw_configs:
        doc = frappe.get_doc("AM Table Config", cfg["name"])
        configs.append({
            "dag_id": doc.dag_id,
            "scope": doc.scope,
            "cabinet_slug": doc.cabinet or "",
            "table_name": doc.table_name,
            "enabled": bool(doc.enabled),
            "load_strategy": doc.load_strategy,
            "incremental_days": doc.incremental_days,
            "auto_alter": bool(doc.auto_alter),
            "exclude_fields": [r.field_name for r in doc.exclude_fields],
            "rename_fields": {r.source_field: r.target_field for r in doc.rename_fields},
            "targets": [
                {"db": r.db_connection or None, "table": r.table_name}
                for r in doc.targets
            ],
        })

    rebuild_client_config(client_id, configs)
```

Run: `python -m pytest tests/ -x -q`
Commit: `feat: add AM Table Config DocType with child tables and config sync`

---

## Task 12 — AM Airflow DAG (Virtual, read-only)

Create `apps/airflow_manager/airflow_manager/airflow_manager/doctype/am_airflow_dag/am_airflow_dag.json`:

```json
{
  "name": "AM Airflow DAG",
  "doctype": "DocType",
  "module": "Airflow Manager",
  "is_virtual": 1,
  "autoname": "field:dag_id",
  "title_field": "dag_id",
  "fields": [
    {"fieldname": "dag_id", "fieldtype": "Data", "label": "DAG ID", "in_list_view": 1},
    {"fieldname": "is_paused", "fieldtype": "Check", "label": "Paused", "in_list_view": 1, "read_only": 1},
    {"fieldname": "schedule_interval", "fieldtype": "Data", "label": "Schedule", "in_list_view": 1, "read_only": 1},
    {"fieldname": "last_parsed_time", "fieldtype": "Datetime", "label": "Last Parsed", "in_list_view": 1, "read_only": 1}
  ],
  "permissions": [{"role": "System Manager", "read": 1}]
}
```

Create `apps/airflow_manager/airflow_manager/airflow_manager/doctype/am_airflow_dag/am_airflow_dag.py`:

```python
import frappe
from frappe.model.document import Document
from airflow_manager.airflow_db.dag_reader import list_dags, get_dag, count_dags


class AMAirflowDAG(Document):
    def load_from_db(self):
        row = get_dag(self.name)
        if not row:
            frappe.throw(f"DAG {self.name} not found in Airflow")
        self.update(row)

    def db_insert(self, *args, **kwargs):
        frappe.throw("AM Airflow DAG is read-only")

    def db_update(self, *args, **kwargs):
        frappe.throw("AM Airflow DAG is read-only")

    def delete(self):
        frappe.throw("AM Airflow DAG is read-only")

    @staticmethod
    def get_list(args):
        return list_dags()

    @staticmethod
    def get_count(args):
        return count_dags()

    @staticmethod
    def get_stats(args):
        return {}
```

Run: `python -m pytest tests/ -x -q`
Commit: `feat: add AM Airflow DAG Virtual DocType (read-only)`

---

## Task 13 — Docker + Compose

Create `Dockerfile` at repo root:

```dockerfile
FROM python:3.11-slim

# System deps
RUN apt-get update && apt-get install -y \
    git curl wget mariadb-client \
    && rm -rf /var/lib/apt/lists/*

# Install bench CLI
RUN pip install frappe-bench

# App dependencies
COPY requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt

WORKDIR /home/frappe/frappe-bench

# Copy our app source
COPY apps/airflow_manager /home/frappe/frappe-bench/apps/airflow_manager
RUN pip install -e /home/frappe/frappe-bench/apps/airflow_manager

EXPOSE 8000
CMD ["bench", "start"]
```

Create `docker-compose.yml` at repo root:

```yaml
version: "3.9"

services:
  frappe-mariadb:
    image: mariadb:10.11
    environment:
      MYSQL_ROOT_PASSWORD: ${MARIADB_ROOT_PASSWORD:-frappe}
      MYSQL_DATABASE: ${DB_NAME:-airflow_manager}
      MYSQL_USER: ${DB_USER:-frappe}
      MYSQL_PASSWORD: ${DB_PASSWORD:-frappe}
    volumes:
      - frappe-mariadb-data:/var/lib/mysql
    healthcheck:
      test: ["CMD", "mysqladmin", "ping", "-h", "localhost"]
      interval: 5s
      timeout: 3s
      retries: 10

  frappe-redis:
    image: redis:7-alpine

  frappe:
    build: .
    environment:
      DB_HOST: frappe-mariadb
      DB_PORT: "3306"
      DB_NAME: ${DB_NAME:-airflow_manager}
      DB_USER: ${DB_USER:-frappe}
      DB_PASSWORD: ${DB_PASSWORD:-frappe}
      FRAPPE_SITE_NAME: ${FRAPPE_SITE_NAME:-airflow-manager.localhost}
      ADMIN_PASSWORD: ${ADMIN_PASSWORD:-admin}
      # Airflow integration
      AIRFLOW_DB_URL: ${AIRFLOW_DB_URL}
      AIRFLOW_FERNET_KEY: ${AIRFLOW_FERNET_KEY}
    ports:
      - "${FRAPPE_PORT:-8000}:8000"
    depends_on:
      frappe-mariadb:
        condition: service_healthy
      frappe-redis:
        condition: service_started
    volumes:
      - frappe-sites:/home/frappe/frappe-bench/sites

volumes:
  frappe-mariadb-data:
  frappe-sites:
```

Create `.env.example` at repo root:

```bash
# MariaDB (Frappe's own database)
MARIADB_ROOT_PASSWORD=change_me
DB_NAME=airflow_manager
DB_USER=frappe
DB_PASSWORD=change_me

# Frappe
FRAPPE_SITE_NAME=airflow-manager.localhost
ADMIN_PASSWORD=change_me
FRAPPE_PORT=8000

# Airflow integration — REQUIRED
# Use a read-write user for connection/variable tables, read-only for dag table
AIRFLOW_DB_URL=postgresql+psycopg2://airflow_ui:change_me@airflow-postgres:5432/airflow
AIRFLOW_FERNET_KEY=<copy from AIRFLOW__CORE__FERNET_KEY in your Airflow deployment>
```

Create `scripts/init_airflow_db_user.sql` — run once against Airflow PG to create the restricted user:

```sql
-- Run as postgres superuser against the Airflow database
CREATE USER airflow_ui WITH PASSWORD 'change_me';
GRANT CONNECT ON DATABASE airflow TO airflow_ui;
GRANT USAGE ON SCHEMA public TO airflow_ui;

-- Write access for connection and variable management
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE connection TO airflow_ui;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE variable TO airflow_ui;
GRANT USAGE, SELECT ON SEQUENCE connection_id_seq TO airflow_ui;
GRANT USAGE, SELECT ON SEQUENCE variable_id_seq TO airflow_ui;

-- Read-only for DAG list
GRANT SELECT ON TABLE dag TO airflow_ui;
```

Run: `python -m pytest tests/ -x -q`
Commit: `chore: add Dockerfile, docker-compose.yml, .env.example, DB init SQL`

---

## Done

All 13 tasks complete. Summary:

| File | Purpose |
|------|---------|
| `airflow_manager/airflow_db/fernet.py` | Fernet encrypt/decrypt |
| `airflow_manager/airflow_db/connection.py` | SQLAlchemy engine to Airflow PG |
| `airflow_manager/airflow_db/connection_manager.py` | CRUD on `connection` table |
| `airflow_manager/airflow_db/variable_manager.py` | CRUD on `variable` table |
| `airflow_manager/airflow_db/dag_reader.py` | Read-only `dag` table |
| `airflow_manager/airflow_db/registry_sync.py` | Client+Cabinet → CLIENT_REGISTRY |
| `airflow_manager/airflow_db/config_sync.py` | TableConfig → client_config_{id} |
| `airflow_manager/airflow_manager/doctype/am_client/` | Client DocType + sync trigger |
| `airflow_manager/airflow_manager/doctype/am_cabinet/` | Cabinet DocType + sync trigger |
| `airflow_manager/airflow_manager/doctype/am_airflow_connection/` | Virtual, Airflow PG |
| `airflow_manager/airflow_manager/doctype/am_database_connection/` | Virtual, postgres type |
| `airflow_manager/airflow_manager/doctype/am_table_config/` | Config DocType + child tables |
| `airflow_manager/airflow_manager/doctype/am_airflow_dag/` | Virtual, read-only |
| `Dockerfile` + `docker-compose.yml` | Container setup |
