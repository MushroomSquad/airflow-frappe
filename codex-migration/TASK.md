# frappe-airflow — Base Refactor

You are refactoring the `airflow-frappe` repository into a reusable `frappe-airflow` base.

Work through each task in order. After each task: run the verification command, fix any failures, then commit.

## Project context

- Frappe v15, Python 3.11, Docker (MariaDB + Redis)
- Current Python package: `airflow_manager` → rename to `frappe_airflow`
- The inner Frappe module directory `airflow_manager/` stays named as-is (Frappe module names are separate from Python package names)
- Business doctypes (Client, Cabinet, TableConfig) are extracted to a new repo at `/mnt/Soft/Work/Projects/fldrspro/airflow-manager`
- All paths are relative to the repo root unless stated otherwise
- Tests: `cd apps/frappe_airflow && python -m pytest tests/ -x -q`

## Directory structure after refactor

```
apps/frappe_airflow/              ← was: apps/airflow_manager/
  frappe_airflow/                 ← was: airflow_manager/  (Python package)
    airflow_db/                   ← unchanged (Airflow integration logic)
    airflow_manager/              ← unchanged (Frappe module — keep this name)
      doctype/
        am_airflow_dag/           ← keep
        am_airflow_connection/    ← keep
        am_database_connection/   ← keep
        am_client/                ← REMOVE (goes to airflow-manager repo)
        am_cabinet/               ← REMOVE (goes to airflow-manager repo)
        am_table_config/          ← REMOVE (goes to airflow-manager repo)
        am_table_config_*/        ← REMOVE (goes to airflow-manager repo)
      workspace/
    hooks.py
    setup.py
    __init__.py
  tests/
  setup.py                        ← pip package setup
```

---

## Task 1 — Rename directories

Rename the app directory and Python package using git mv so history is preserved.

```bash
git mv apps/airflow_manager apps/frappe_airflow
git mv apps/frappe_airflow/airflow_manager apps/frappe_airflow/frappe_airflow
```

Verify:
```bash
ls apps/frappe_airflow/frappe_airflow/airflow_db/
```
Expected: `connection.py  connection_manager.py  config_sync.py  dag_reader.py  fernet.py  registry_sync.py  variable_manager.py  __init__.py`

Commit:
```bash
git add -A && git commit -m "refactor: rename Python package airflow_manager → frappe_airflow"
```

---

## Task 2 — Update pip setup.py

Replace `apps/frappe_airflow/setup.py` with:

```python
from setuptools import setup, find_packages

setup(
    name="frappe_airflow",
    version="0.1.0",
    description="Frappe UI for Airflow configuration management",
    author="Fldrspro",
    packages=find_packages(),
    zip_safe=False,
    include_package_data=True,
    python_requires=">=3.11",
)
```

Commit:
```bash
git add apps/frappe_airflow/setup.py && git commit -m "refactor: pip package name → frappe_airflow"
```

---

## Task 3 — Update hooks.py

Replace `apps/frappe_airflow/frappe_airflow/hooks.py` with:

```python
app_name = "frappe_airflow"
app_title = "Airflow Manager"
app_publisher = "Fldrspro"
app_description = "Manage Airflow connections, variables, and configuration"
app_version = "0.1.0"

app_include_js = []
app_include_css = []

after_install = "frappe_airflow.setup.after_install"
on_session_creation = "frappe_airflow.setup.set_default_workspace"

fixtures = [
    {"dt": "Workspace", "filters": [["module", "=", "Airflow Manager"]]}
]

doc_events = {}
```

Commit:
```bash
git add apps/frappe_airflow/frappe_airflow/hooks.py && git commit -m "refactor: hooks.py app_name + on_session_creation hook"
```

---

## Task 4 — Update Python imports

Only replace `airflow_db` import paths. Do NOT replace `airflow_manager.airflow_manager.doctype` paths — those are cross-doctype references that belong to the extension repo and must stay unchanged.

```bash
find apps/frappe_airflow -name "*.py" | xargs sed -i 's/from airflow_manager\.airflow_db\./from frappe_airflow.airflow_db./g'
```

Verify — zero remaining old airflow_db imports:
```bash
grep -r "from airflow_manager\.airflow_db\." apps/frappe_airflow --include="*.py" | grep -v __pycache__
```
Expected: no output.

Verify — cross-doctype import in am_cabinet.py is untouched:
```bash
grep "airflow_manager\.airflow_manager\.doctype" apps/frappe_airflow/frappe_airflow/airflow_manager/doctype/am_cabinet/am_cabinet.py
```
Expected: two lines containing `from airflow_manager.airflow_manager.doctype.am_client.am_client import`

Run tests (may skip without real Airflow DB — no new failures is the goal):
```bash
cd apps/frappe_airflow && python -m pytest tests/ -x -q 2>&1 | tail -10
```

Commit:
```bash
git add apps/frappe_airflow && git commit -m "refactor: update airflow_db imports to frappe_airflow namespace"
```

---

## Task 5 — Update Dockerfile

In `Dockerfile`, replace the two lines that reference `apps/airflow_manager`:

Old:
```dockerfile
COPY --chown=frappe:frappe apps/airflow_manager apps/airflow_manager
RUN ./env/bin/pip install sqlalchemy>=2.0 psycopg2-binary cryptography && \
    ./env/bin/pip install -e apps/airflow_manager
```

New:
```dockerfile
COPY --chown=frappe:frappe apps/frappe_airflow apps/frappe_airflow
RUN ./env/bin/pip install sqlalchemy>=2.0 psycopg2-binary cryptography && \
    ./env/bin/pip install -e apps/frappe_airflow
```

Verify:
```bash
grep "airflow_manager" Dockerfile
```
Expected: no output.

Commit:
```bash
git add Dockerfile && git commit -m "refactor: Dockerfile uses frappe_airflow app path"
```

---

## Task 6 — Update entrypoint.sh

Replace `docker/entrypoint.sh` with:

```bash
#!/bin/bash
set -euo pipefail

BENCH_DIR=/home/frappe/frappe-bench
REDIS_HOST=${REDIS_HOST:-frappe-redis}

if [ ! -f "$BENCH_DIR/sites/$FRAPPE_SITE_NAME/site_config.json" ]; then
    echo "==> First run: creating site $FRAPPE_SITE_NAME ..."

    cd "$BENCH_DIR"

    bench set-config -g db_host "$DB_HOST"
    bench set-config -g db_port "${DB_PORT:-3306}"
    bench set-config -g redis_cache "redis://${REDIS_HOST}:6379/0"
    bench set-config -g redis_queue "redis://${REDIS_HOST}:6379/1"
    bench set-config -g redis_socketio "redis://${REDIS_HOST}:6379/2"

    bench new-site "$FRAPPE_SITE_NAME" \
        --db-host "$DB_HOST" \
        --db-name "$DB_NAME" \
        --db-root-username root \
        --db-root-password "${MARIADB_ROOT_PASSWORD}" \
        --admin-password "$ADMIN_PASSWORD" \
        --mariadb-user-host-login-scope='%'

    if ! grep -qxF "frappe_airflow" sites/apps.txt; then
        printf "\nfrappe_airflow\n" >> sites/apps.txt
    fi

    bench --site "$FRAPPE_SITE_NAME" install-app frappe_airflow

    echo "==> Site ready."
fi

cd "$BENCH_DIR"
bench set-config -g default_site "$FRAPPE_SITE_NAME"
exec bench serve --port 8000
```

Verify:
```bash
grep "airflow_manager" docker/entrypoint.sh
```
Expected: no output.

Commit:
```bash
git add docker/entrypoint.sh && git commit -m "refactor: entrypoint uses REDIS_HOST env var, installs frappe_airflow"
```

---

## Task 7 — Move business doctypes to new repo

Copy the six doctype directories to the new repo path, then remove them from the base.

Step 1 — create destination:
```bash
mkdir -p /mnt/Soft/Work/Projects/fldrspro/airflow-manager/apps/airflow_manager/airflow_manager/airflow_manager/doctype
```

Step 2 — copy:
```bash
for doctype in am_client am_cabinet am_table_config am_table_config_exclude_field am_table_config_rename_field am_table_config_target; do
  cp -r "apps/frappe_airflow/frappe_airflow/airflow_manager/doctype/${doctype}" \
    "/mnt/Soft/Work/Projects/fldrspro/airflow-manager/apps/airflow_manager/airflow_manager/airflow_manager/doctype/"
done
```

Step 3 — verify copy:
```bash
find /mnt/Soft/Work/Projects/fldrspro/airflow-manager -name "*.py" | grep -v __pycache__ | sort
```
Expected: 6 `.py` files (one per doctype).

Step 4 — remove from base:
```bash
git rm -r \
  apps/frappe_airflow/frappe_airflow/airflow_manager/doctype/am_client \
  apps/frappe_airflow/frappe_airflow/airflow_manager/doctype/am_cabinet \
  apps/frappe_airflow/frappe_airflow/airflow_manager/doctype/am_table_config \
  apps/frappe_airflow/frappe_airflow/airflow_manager/doctype/am_table_config_exclude_field \
  apps/frappe_airflow/frappe_airflow/airflow_manager/doctype/am_table_config_rename_field \
  apps/frappe_airflow/frappe_airflow/airflow_manager/doctype/am_table_config_target
```

Verify only 3 base doctypes remain:
```bash
ls apps/frappe_airflow/frappe_airflow/airflow_manager/doctype/
```
Expected: `am_airflow_connection  am_airflow_dag  am_database_connection  __init__.py`

Commit:
```bash
git add -A && git commit -m "refactor: extract Client/Cabinet/TableConfig to airflow-manager repo"
```

---

## Task 8 — Fix setup.py (workspace + homepage)

Replace `apps/frappe_airflow/frappe_airflow/setup.py` with:

```python
"""Post-install setup: workspace and default homepage."""
import json
import frappe


def after_install():
    _setup_workspace()
    _hide_default_workspaces()
    frappe.db.set_value(
        "System Settings", "System Settings", "default_app", "frappe_airflow"
    )
    frappe.db.commit()


def set_default_workspace(login_manager):
    frappe.db.set_value(
        "User",
        frappe.session.user,
        "home_settings",
        '{"route": "Workspaces/Airflow Manager"}',
    )


def _setup_workspace():
    shortcuts_data = [
        ("DAGs", "AM Airflow DAG", "Gray"),
        ("Connections", "AM Airflow Connection", "Orange"),
        ("Databases", "AM Database Connection", "Orange"),
    ]

    if frappe.db.exists("Workspace", "Airflow Manager"):
        frappe.db.delete("Workspace Shortcut", {"parent": "Airflow Manager"})
        ws = frappe.get_doc("Workspace", "Airflow Manager")
    else:
        ws = frappe.new_doc("Workspace")
        ws.name = "Airflow Manager"
        ws.label = "Airflow Manager"
        ws.module = "Airflow Manager"
        ws.is_standard = 1
        ws.public = 1
        ws.icon = "server"
        ws.indicator_color = "blue"

    for label, link_to, color in shortcuts_data:
        ws.append("shortcuts", {
            "type": "DocType",
            "label": label,
            "link_to": link_to,
            "color": color,
            "doc_view": "List",
        })

    content = [
        {"id": f"s{i + 1}", "type": "shortcut", "data": {"shortcut_name": label, "col": 3}}
        for i, (label, _, _) in enumerate(shortcuts_data)
    ]
    ws.content = json.dumps(content)
    ws.flags.ignore_links = True
    ws.flags.ignore_validate = True
    ws.save(ignore_permissions=True)
    frappe.db.commit()


def _hide_default_workspaces():
    for name in ["Integrations", "Build", "Tools", "Users", "Welcome Workspace", "Website", "Home"]:
        if frappe.db.exists("Workspace", name):
            frappe.db.set_value("Workspace", name, "is_hidden", 1)
    frappe.db.commit()
```

Replace `apps/frappe_airflow/frappe_airflow/airflow_manager/workspace/airflow_manager/airflow_manager.json` with:

```json
{
  "doctype": "Workspace",
  "name": "Airflow Manager",
  "label": "Airflow Manager",
  "module": "Airflow Manager",
  "is_standard": 1,
  "public": 1,
  "icon": "server",
  "indicator_color": "blue",
  "title": "Airflow Manager",
  "shortcuts": [
    {"color": "Gray",   "doc_view": "List", "label": "DAGs",       "link_to": "AM Airflow DAG",         "type": "DocType"},
    {"color": "Orange", "doc_view": "List", "label": "Connections", "link_to": "AM Airflow Connection",  "type": "DocType"},
    {"color": "Orange", "doc_view": "List", "label": "Databases",   "link_to": "AM Database Connection", "type": "DocType"}
  ],
  "content": "[{\"id\":\"s1\",\"type\":\"shortcut\",\"data\":{\"shortcut_name\":\"DAGs\",\"col\":3}},{\"id\":\"s2\",\"type\":\"shortcut\",\"data\":{\"shortcut_name\":\"Connections\",\"col\":3}},{\"id\":\"s3\",\"type\":\"shortcut\",\"data\":{\"shortcut_name\":\"Databases\",\"col\":3}}]"
}
```

Commit:
```bash
git add apps/frappe_airflow/frappe_airflow/setup.py \
        apps/frappe_airflow/frappe_airflow/airflow_manager/workspace/ \
  && git commit -m "feat: workspace 3 shortcuts + homepage redirect on login"
```

---

## Task 9 — Update docker-compose.yml and .env.example

Replace `docker-compose.yml` with:

```yaml
version: "3.9"

services:
  frappe-mariadb:
    image: mariadb:10.6
    command: --character-set-server=utf8mb4 --collation-server=utf8mb4_unicode_ci --skip-ssl
    environment:
      MYSQL_ROOT_PASSWORD: ${MARIADB_ROOT_PASSWORD:-frappe}
    volumes:
      - frappe-mariadb-data:/var/lib/mysql
      - ./docker/mariadb.cnf:/etc/mysql/conf.d/local.cnf
    healthcheck:
      test: ["CMD", "mysqladmin", "ping", "-h", "localhost"]
      interval: 5s
      timeout: 3s
      retries: 10

  frappe-redis:
    image: redis:7-alpine

  frappe:
    image: ${FRAPPE_IMAGE:-ghcr.io/fldrspro/frappe-airflow:latest}
    build:
      context: .
    environment:
      DB_HOST: ${DB_HOST:-frappe-mariadb}
      DB_PORT: "3306"
      DB_NAME: ${DB_NAME:-frappe_airflow}
      FRAPPE_SITE_NAME: ${FRAPPE_SITE_NAME:-airflow-manager.localhost}
      ADMIN_PASSWORD: ${ADMIN_PASSWORD:-admin}
      MARIADB_ROOT_PASSWORD: ${MARIADB_ROOT_PASSWORD:-frappe}
      REDIS_HOST: ${REDIS_HOST:-frappe-redis}
      AIRFLOW_DB_URL: ${AIRFLOW_DB_URL:-}
      AIRFLOW_FERNET_KEY: ${AIRFLOW_FERNET_KEY:-}
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

Create `.env.example`:

```
# Required for Airflow integration
AIRFLOW_DB_URL=postgresql+psycopg2://airflow:airflow@airflow-postgres:5432/airflow
AIRFLOW_FERNET_KEY=

# Frappe / MariaDB
MARIADB_ROOT_PASSWORD=frappe
ADMIN_PASSWORD=admin
DB_NAME=frappe_airflow
FRAPPE_SITE_NAME=airflow-manager.localhost
FRAPPE_PORT=8000

# Override to connect to external services (plugin pattern)
# DB_HOST=my-existing-mariadb
# REDIS_HOST=my-existing-redis

# Override to use published image instead of local build
# FRAPPE_IMAGE=ghcr.io/fldrspro/frappe-airflow:latest
```

Verify:
```bash
grep "REDIS_HOST" docker-compose.yml
```
Expected: `REDIS_HOST: ${REDIS_HOST:-frappe-redis}`

Commit:
```bash
git add docker-compose.yml .env.example && git commit -m "feat: docker-compose REDIS_HOST + FRAPPE_IMAGE plugin pattern"
```

---

## Task 10 — Smoke test full rebuild

Stop everything and rebuild from scratch:

```bash
docker-compose down -v
docker-compose up --build -d
```

Wait for site init:
```bash
for i in $(seq 1 60); do
  docker-compose logs frappe 2>&1 | grep -q "Site ready" && echo "Ready!" && break
  echo "Waiting... $i/60"; sleep 5
done
```

Verify workspace has exactly 3 shortcuts:
```bash
docker exec $(docker-compose ps -q frappe) bash -c "
cd /home/frappe/frappe-bench && ./env/bin/python3 << 'PYEOF'
import frappe, os
os.chdir('/home/frappe/frappe-bench/sites')
frappe.init(site='airflow-manager.localhost', sites_path='/home/frappe/frappe-bench/sites')
frappe.connect()
shortcuts = frappe.get_all('Workspace Shortcut', filters={'parent': 'Airflow Manager'}, fields=['label'])
labels = [s.label for s in shortcuts]
print('shortcuts:', labels)
assert labels == ['DAGs', 'Connections', 'Databases'], f'Unexpected: {labels}'
print('OK — 3 shortcuts correct')
frappe.destroy()
PYEOF"
```

Verify HTTP responds:
```bash
curl -s -o /dev/null -w "%{http_code}" http://localhost:8000
```
Expected: `200` or `302`.

Commit:
```bash
git add -A && git commit -m "test: frappe-airflow base builds and serves correctly"
```
