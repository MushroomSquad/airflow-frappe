# frappe-airflow Base Refactor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Refactor current `airflow-frappe` repo into the `frappe-airflow` base — rename Python package, extract business doctypes to new repo path, fix workspace, add homepage redirect, and make Docker pluggable via env vars.

**Architecture:** Python package `airflow_manager` → `frappe_airflow`. Frappe module directory `airflow_manager/` stays unchanged (Frappe module names are separate from Python package names). Business doctypes (am_client, am_cabinet, am_table_config*) are copied to `/mnt/Soft/Work/Projects/fldrspro/airflow-manager` and removed from base. Docker entrypoint uses `REDIS_HOST` and `DB_HOST` env vars instead of hardcoded service names.

**Tech Stack:** Python 3.11, Frappe v15, Docker, MariaDB 10.6, Redis 7, SQLAlchemy

---

### Task 1: Rename app directory and Python package

**Files:**
- Rename: `apps/airflow_manager/` → `apps/frappe_airflow/`
- Rename: `apps/frappe_airflow/airflow_manager/` → `apps/frappe_airflow/frappe_airflow/`
- Inner Frappe module `apps/frappe_airflow/frappe_airflow/airflow_manager/` stays unchanged

- [ ] **Step 1: Rename the outer app directory**

```bash
git mv apps/airflow_manager apps/frappe_airflow
```

- [ ] **Step 2: Rename the Python package directory**

```bash
git mv apps/frappe_airflow/airflow_manager apps/frappe_airflow/frappe_airflow
```

- [ ] **Step 3: Verify structure**

```bash
find apps/frappe_airflow -maxdepth 3 -type d | sort
```

Expected output includes:
```
apps/frappe_airflow
apps/frappe_airflow/frappe_airflow
apps/frappe_airflow/frappe_airflow/airflow_db
apps/frappe_airflow/frappe_airflow/airflow_manager
apps/frappe_airflow/frappe_airflow/airflow_manager/doctype
apps/frappe_airflow/tests
```

- [ ] **Step 4: Commit rename (no logic changes yet)**

```bash
git add -A
git commit -m "refactor: rename Python package airflow_manager → frappe_airflow"
```

---

### Task 2: Update pip setup.py

**Files:**
- Modify: `apps/frappe_airflow/setup.py`

- [ ] **Step 1: Replace setup.py content**

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

- [ ] **Step 2: Commit**

```bash
git add apps/frappe_airflow/setup.py
git commit -m "refactor: update pip package name to frappe_airflow"
```

---

### Task 3: Update hooks.py — app_name and all module paths

**Files:**
- Modify: `apps/frappe_airflow/frappe_airflow/hooks.py`

- [ ] **Step 1: Rewrite hooks.py**

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

- [ ] **Step 2: Commit**

```bash
git add apps/frappe_airflow/frappe_airflow/hooks.py
git commit -m "refactor: update hooks.py for frappe_airflow package name"
```

---

### Task 4: Update all Python imports in base app files

**Files:**
- Modify: `apps/frappe_airflow/frappe_airflow/airflow_db/dag_reader.py`
- Modify: `apps/frappe_airflow/frappe_airflow/airflow_db/connection_manager.py`
- Modify: `apps/frappe_airflow/frappe_airflow/airflow_db/config_sync.py`
- Modify: `apps/frappe_airflow/frappe_airflow/airflow_db/registry_sync.py`
- Modify: `apps/frappe_airflow/frappe_airflow/airflow_db/variable_manager.py`
- Modify: `apps/frappe_airflow/frappe_airflow/airflow_manager/doctype/am_airflow_dag/am_airflow_dag.py`
- Modify: `apps/frappe_airflow/frappe_airflow/airflow_manager/doctype/am_airflow_connection/am_airflow_connection.py`
- Modify: `apps/frappe_airflow/frappe_airflow/airflow_manager/doctype/am_database_connection/am_database_connection.py`
- Modify: `apps/frappe_airflow/tests/test_*.py` (all 6 test files)

- [ ] **Step 1: Replace only airflow_db imports (not cross-doctype paths)**

`am_cabinet.py` imports from `airflow_manager.airflow_manager.doctype.am_client` — that path stays valid in the extension repo, so we must NOT replace it. Only `airflow_db` paths move to `frappe_airflow`.

```bash
find apps/frappe_airflow -name "*.py" | xargs sed -i 's/from airflow_manager\.airflow_db\./from frappe_airflow.airflow_db./g'
```

- [ ] **Step 2: Verify airflow_db imports are updated**

```bash
grep -r "from airflow_manager\.airflow_db\." apps/frappe_airflow --include="*.py" | grep -v __pycache__
```

Expected: no output (zero remaining old-style airflow_db imports).

- [ ] **Step 3: Verify am_cabinet cross-doctype import is untouched**

```bash
grep "airflow_manager\.airflow_manager\.doctype" apps/frappe_airflow/frappe_airflow/airflow_manager/doctype/am_cabinet/am_cabinet.py
```

Expected: two lines with `from airflow_manager.airflow_manager.doctype.am_client.am_client import` (unchanged — correct for extension repo).

- [ ] **Step 3: Run existing unit tests**

```bash
cd apps/frappe_airflow && python -m pytest tests/ -v --tb=short 2>&1 | tail -30
```

Expected: same pass/fail counts as before rename (tests may skip if no DB — that's fine, no new failures).

- [ ] **Step 4: Commit**

```bash
git add apps/frappe_airflow
git commit -m "refactor: update all imports from airflow_manager to frappe_airflow"
```

---

### Task 5: Update Dockerfile — new app path

**Files:**
- Modify: `Dockerfile`

- [ ] **Step 1: Replace old app path references**

In `Dockerfile`, change:
```dockerfile
COPY --chown=frappe:frappe apps/airflow_manager apps/airflow_manager
RUN ./env/bin/pip install sqlalchemy>=2.0 psycopg2-binary cryptography && \
    ./env/bin/pip install -e apps/airflow_manager
```

To:
```dockerfile
COPY --chown=frappe:frappe apps/frappe_airflow apps/frappe_airflow
RUN ./env/bin/pip install sqlalchemy>=2.0 psycopg2-binary cryptography && \
    ./env/bin/pip install -e apps/frappe_airflow
```

- [ ] **Step 2: Commit**

```bash
git add Dockerfile
git commit -m "refactor: update Dockerfile for frappe_airflow app path"
```

---

### Task 6: Update entrypoint.sh — app name + REDIS_HOST

**Files:**
- Modify: `docker/entrypoint.sh`

- [ ] **Step 1: Replace entrypoint.sh content**

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

- [ ] **Step 2: Commit**

```bash
git add docker/entrypoint.sh
git commit -m "refactor: entrypoint uses REDIS_HOST env var, installs frappe_airflow"
```

---

### Task 7: Copy business doctypes to new repo path before removing from base

**Files:**
- Copy to: `/mnt/Soft/Work/Projects/fldrspro/airflow-manager/apps/airflow_manager/airflow_manager/airflow_manager/doctype/`
- Source: `apps/frappe_airflow/frappe_airflow/airflow_manager/doctype/`

- [ ] **Step 1: Create destination structure**

```bash
mkdir -p /mnt/Soft/Work/Projects/fldrspro/airflow-manager/apps/airflow_manager/airflow_manager/airflow_manager/doctype
```

- [ ] **Step 2: Copy all business doctypes**

```bash
cp -r apps/frappe_airflow/frappe_airflow/airflow_manager/doctype/am_client \
    /mnt/Soft/Work/Projects/fldrspro/airflow-manager/apps/airflow_manager/airflow_manager/airflow_manager/doctype/

cp -r apps/frappe_airflow/frappe_airflow/airflow_manager/doctype/am_cabinet \
    /mnt/Soft/Work/Projects/fldrspro/airflow-manager/apps/airflow_manager/airflow_manager/airflow_manager/doctype/

cp -r apps/frappe_airflow/frappe_airflow/airflow_manager/doctype/am_table_config \
    /mnt/Soft/Work/Projects/fldrspro/airflow-manager/apps/airflow_manager/airflow_manager/airflow_manager/doctype/

cp -r apps/frappe_airflow/frappe_airflow/airflow_manager/doctype/am_table_config_exclude_field \
    /mnt/Soft/Work/Projects/fldrspro/airflow-manager/apps/airflow_manager/airflow_manager/airflow_manager/doctype/

cp -r apps/frappe_airflow/frappe_airflow/airflow_manager/doctype/am_table_config_rename_field \
    /mnt/Soft/Work/Projects/fldrspro/airflow-manager/apps/airflow_manager/airflow_manager/airflow_manager/doctype/

cp -r apps/frappe_airflow/frappe_airflow/airflow_manager/doctype/am_table_config_target \
    /mnt/Soft/Work/Projects/fldrspro/airflow-manager/apps/airflow_manager/airflow_manager/airflow_manager/doctype/
```

- [ ] **Step 3: Verify copy**

```bash
find /mnt/Soft/Work/Projects/fldrspro/airflow-manager -name "*.py" | sort
```

Expected: 6 doctype `.py` files present.

- [ ] **Step 4: Now remove business doctypes from base**

```bash
git rm -r apps/frappe_airflow/frappe_airflow/airflow_manager/doctype/am_client
git rm -r apps/frappe_airflow/frappe_airflow/airflow_manager/doctype/am_cabinet
git rm -r apps/frappe_airflow/frappe_airflow/airflow_manager/doctype/am_table_config
git rm -r apps/frappe_airflow/frappe_airflow/airflow_manager/doctype/am_table_config_exclude_field
git rm -r apps/frappe_airflow/frappe_airflow/airflow_manager/doctype/am_table_config_rename_field
git rm -r apps/frappe_airflow/frappe_airflow/airflow_manager/doctype/am_table_config_target
```

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "refactor: extract Client/Cabinet/TableConfig doctypes to airflow-manager repo"
```

---

### Task 8: Fix workspace — 3 shortcuts for base, correct Frappe v15 format

**Files:**
- Modify: `apps/frappe_airflow/frappe_airflow/setup.py`
- Modify: `apps/frappe_airflow/frappe_airflow/airflow_manager/workspace/airflow_manager/airflow_manager.json`

- [ ] **Step 1: Rewrite setup.py**

```python
"""Post-install setup: configure workspace and hide default Frappe modules."""
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

- [ ] **Step 2: Update workspace fixture JSON**

`apps/frappe_airflow/frappe_airflow/airflow_manager/workspace/airflow_manager/airflow_manager.json`:

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
    {"color": "Gray",   "doc_view": "List", "label": "DAGs",        "link_to": "AM Airflow DAG",         "type": "DocType"},
    {"color": "Orange", "doc_view": "List", "label": "Connections",  "link_to": "AM Airflow Connection",  "type": "DocType"},
    {"color": "Orange", "doc_view": "List", "label": "Databases",    "link_to": "AM Database Connection", "type": "DocType"}
  ],
  "content": "[{\"id\":\"s1\",\"type\":\"shortcut\",\"data\":{\"shortcut_name\":\"DAGs\",\"col\":3}},{\"id\":\"s2\",\"type\":\"shortcut\",\"data\":{\"shortcut_name\":\"Connections\",\"col\":3}},{\"id\":\"s3\",\"type\":\"shortcut\",\"data\":{\"shortcut_name\":\"Databases\",\"col\":3}}]"
}
```

- [ ] **Step 3: Commit**

```bash
git add apps/frappe_airflow/frappe_airflow/setup.py \
        apps/frappe_airflow/frappe_airflow/airflow_manager/workspace/
git commit -m "feat: workspace 3 shortcuts + homepage redirect hook"
```

---

### Task 9: Update docker-compose.yml — REDIS_HOST + FRAPPE_IMAGE pattern

**Files:**
- Modify: `docker-compose.yml`

- [ ] **Step 1: Rewrite docker-compose.yml**

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

- [ ] **Step 2: Update .env.example (create if missing)**

```bash
cat > .env.example << 'EOF'
# Required for Airflow integration
AIRFLOW_DB_URL=postgresql+psycopg2://airflow:airflow@airflow-postgres:5432/airflow
AIRFLOW_FERNET_KEY=

# Frappe / MariaDB
MARIADB_ROOT_PASSWORD=frappe
ADMIN_PASSWORD=admin
DB_NAME=frappe_airflow
FRAPPE_SITE_NAME=airflow-manager.localhost
FRAPPE_PORT=8000

# Override to use external services (plugin pattern)
# DB_HOST=my-existing-mariadb
# REDIS_HOST=my-existing-redis

# Override to use published image instead of local build
# FRAPPE_IMAGE=ghcr.io/fldrspro/frappe-airflow:latest
EOF
```

- [ ] **Step 3: Commit**

```bash
git add docker-compose.yml .env.example
git commit -m "feat: docker-compose supports REDIS_HOST and FRAPPE_IMAGE env vars"
```

---

### Task 10: Full rebuild smoke test

- [ ] **Step 1: Stop and remove existing containers + volumes**

```bash
docker-compose down -v
```

- [ ] **Step 2: Build from scratch**

```bash
docker-compose up --build -d
```

- [ ] **Step 3: Wait for site to initialize (watch logs)**

```bash
docker-compose logs -f frappe
```

Wait until you see: `==> Site ready.` then `Ctrl+C`.

- [ ] **Step 4: Verify workspace has 3 shortcuts in DB**

```bash
docker exec $(docker-compose ps -q frappe) bash -c "
cd /home/frappe/frappe-bench && ./env/bin/python3 << 'PYEOF'
import frappe, os
os.chdir('/home/frappe/frappe-bench/sites')
frappe.init(site='airflow-manager.localhost', sites_path='/home/frappe/frappe-bench/sites')
frappe.connect()
shortcuts = frappe.get_all('Workspace Shortcut', filters={'parent': 'Airflow Manager'}, fields=['label'])
print('shortcuts:', [s.label for s in shortcuts])
assert len(shortcuts) == 3, f'Expected 3 shortcuts, got {len(shortcuts)}'
print('OK')
frappe.destroy()
PYEOF"
```

Expected output:
```
shortcuts: ['DAGs', 'Connections', 'Databases']
OK
```

- [ ] **Step 5: Verify app loads without 500 errors**

```bash
curl -s -o /dev/null -w "%{http_code}" http://localhost:8000
```

Expected: `200` or `302` (redirect to login).

- [ ] **Step 6: Commit final state**

```bash
git add -A
git commit -m "test: verify frappe-airflow base builds and runs cleanly"
```

---

## What's next: Plan B — airflow-manager extension repo

Plan B (separate document) will:
1. Initialize git repo at `/mnt/Soft/Work/Projects/fldrspro/airflow-manager`
2. Scaffold Frappe app structure around the copied doctypes
3. Update doctype imports: `airflow_manager.airflow_db.*` → `frappe_airflow.airflow_db.*`
4. Create `hooks.py` with `required_apps = ["frappe_airflow"]`
5. Create `setup.py` that extends workspace with Client + Cabinet + TableConfig shortcuts
6. Create `Dockerfile` (`FROM ghcr.io/fldrspro/frappe-airflow:latest`)
7. Create `docker-compose.yml` for standalone dev
