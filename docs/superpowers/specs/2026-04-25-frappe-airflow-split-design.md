# frappe-airflow split design

**Date:** 2026-04-25  
**Status:** Approved

## Goal

Split the current `airflow-frappe` monolith into two independent repos:

1. **`frappe-airflow`** (this repo) — generic Frappe app that wraps Airflow: DAGs, Connections, Variables. Publishes a Docker base image.
2. **`airflow-manager`** (new repo at `/mnt/Soft/Work/Projects/fldrspro/airflow-manager`) — business-specific extension: Client, Cabinet, Table Config. Built `FROM` the base image.

Frappe is used as an invisible framework/platform. The primary user-facing UI is the Airflow Manager workspace. Frappe admin panel remains accessible for user management.

---

## Section 1: Repository split

### `frappe-airflow` (this repo) — keep + rename

Python-пакет переименовывается: `airflow_manager` → `frappe_airflow`.  
Frappe app name в `hooks.py`: `app_name = "frappe_airflow"`.

```
apps/frappe_airflow/          ← папка приложения (было: apps/airflow_manager)
  frappe_airflow/             ← Python-пакет (было: airflow_manager)
    airflow_db/
      connection.py           ← SQLAlchemy engine, AIRFLOW_DB_URL
      dag_reader.py
      connection_manager.py
      variable_manager.py
      config_sync.py
      fernet.py
      registry_sync.py
    airflow_manager/          ← Frappe-модуль (имя модуля остаётся для обратной совместимости)
      doctype/
        am_airflow_dag/
        am_airflow_connection/
        am_database_connection/
      workspace/              ← shortcuts: DAGs, Connections, Databases
    hooks.py                  ← app_name = "frappe_airflow"
    setup.py                  ← after_install, on_session_creation
    __init__.py
```

Workspace в базе содержит 3 шортката: DAGs, Connections, Databases.

### `airflow-manager` (новый репо) — перенос из текущего

Python-пакет остаётся `airflow_manager`.

```
apps/airflow_manager/         ← папка приложения
  airflow_manager/            ← Python-пакет
    airflow_manager/          ← Frappe-модуль
      doctype/
        am_client/
        am_cabinet/
        am_table_config/
        am_table_config_exclude_field/
        am_table_config_rename_field/
        am_table_config_target/
    hooks.py                  ← required_apps = ["frappe_airflow"]
    setup.py                  ← after_install добавляет шортакаты в workspace
    __init__.py
```

Файлы переносятся (не удаляются), история сохраняется через `git log -- <path>` в исходном репо.

---

## Section 2: Docker images

### Base image — `ghcr.io/fldrspro/frappe-airflow:latest`

Built from `frappe-airflow` repo. Published via GitHub Actions on tag push.

```dockerfile
# Dockerfile (frappe-airflow)
FROM python:3.11-slim
# ... system deps, frappe user, bench init v15
COPY --chown=frappe:frappe apps/frappe_airflow apps/frappe_airflow
RUN ./env/bin/pip install -e apps/frappe_airflow
COPY --chown=frappe:frappe docker/entrypoint.sh /home/frappe/entrypoint.sh
ENTRYPOINT ["/home/frappe/entrypoint.sh"]
```

### Extension image — `ghcr.io/fldrspro/airflow-manager:latest`

Built from `airflow-manager` repo.

```dockerfile
# Dockerfile (airflow-manager)
FROM ghcr.io/fldrspro/frappe-airflow:latest
COPY --chown=frappe:frappe apps/airflow_manager apps/airflow_manager
RUN ./env/bin/pip install -e apps/airflow_manager
```

Local build: `docker build .` — pulls base from GHCR, adds extension layer.

### `docker-compose.yml` pattern (same in both repos)

```yaml
services:
  frappe:
    image: ${FRAPPE_IMAGE:-ghcr.io/fldrspro/airflow-manager:latest}
    build:
      context: .
    environment:
      DB_HOST: ${DB_HOST:-frappe-mariadb}
      REDIS_HOST: ${REDIS_HOST:-frappe-redis}
      AIRFLOW_DB_URL: ${AIRFLOW_DB_URL}
      MARIADB_ROOT_PASSWORD: ${MARIADB_ROOT_PASSWORD:-frappe}
      ADMIN_PASSWORD: ${ADMIN_PASSWORD:-admin}
    ports:
      - "${FRAPPE_PORT:-8000}:8000"
    depends_on:
      frappe-mariadb:
        condition: service_healthy
      frappe-redis:
        condition: service_started
    volumes:
      - frappe-sites:/home/frappe/frappe-bench/sites

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

volumes:
  frappe-mariadb-data:
  frappe-sites:
```

Пользователь в своём существующем compose подключается просто:

```yaml
services:
  frappe-airflow:
    image: ghcr.io/fldrspro/airflow-manager:latest
    environment:
      DB_HOST: my-existing-mariadb
      REDIS_HOST: my-existing-redis
      AIRFLOW_DB_URL: postgresql+psycopg2://user:pass@airflow-postgres:5432/airflow
```

### `entrypoint.sh` — убрать хардкод `frappe-redis`

```bash
REDIS_HOST=${REDIS_HOST:-frappe-redis}
bench set-config -g redis_cache "redis://${REDIS_HOST}:6379/0"
bench set-config -g redis_queue "redis://${REDIS_HOST}:6379/1"
bench set-config -g redis_socketio "redis://${REDIS_HOST}:6379/2"
```

---

## Section 3: Homepage

После логина пользователь попадает напрямую на `/Workspaces/Airflow Manager`.  
Frappe admin panel доступна по прямому URL (`/app`, `/app/user` и т.д.).

### Механизм

**`hooks.py` (frappe_airflow):**
```python
on_session_creation = "frappe_airflow.setup.set_default_workspace"
```

**`setup.py`:**
```python
def set_default_workspace(login_manager):
    frappe.db.set_value(
        "User", frappe.session.user,
        "home_settings",
        '{"route": "Workspaces/Airflow Manager"}'
    )

def after_install():
    _setup_workspace()
    _hide_default_workspaces()
    frappe.db.set_value(
        "System Settings", "System Settings",
        "default_app", "frappe_airflow"
    )
    frappe.db.commit()
```

### Поведение
| Действие | Результат |
|---|---|
| Логин | → `/Workspaces/Airflow Manager` |
| Переход в `/app` | → стандартный Frappe desk |
| `/app/user` | → список пользователей |
| Новый пользователь | `set_default_workspace` задаёт home при первой сессии |

---

## Section 4: Extension pattern

`airflow-manager` расширяет workspace без перезаписи базового.

**`hooks.py`:**
```python
required_apps = ["frappe_airflow"]
after_install = "airflow_manager.setup.after_install"
fixtures = [{"dt": "Workspace", "filters": [["module", "=", "Airflow Manager"]]}]
```

**`setup.py` (airflow_manager):**
```python
def after_install():
    ws = frappe.get_doc("Workspace", "Airflow Manager")
    frappe.db.delete("Workspace Shortcut", {"parent": "Airflow Manager"})

    shortcuts_data = [
        # base shortcuts
        ("DAGs", "AM Airflow DAG", "Gray"),
        ("Connections", "AM Airflow Connection", "Orange"),
        ("Databases", "AM Database Connection", "Orange"),
        # extension shortcuts
        ("Clients", "AM Client", "Blue"),
        ("Cabinets", "AM Cabinet", "Green"),
        ("Table Configs", "AM Table Config", "Purple"),
    ]
    for i, (label, link_to, color) in enumerate(shortcuts_data):
        ws.append("shortcuts", {
            "type": "DocType", "label": label,
            "link_to": link_to, "color": color, "doc_view": "List"
        })

    ws.content = json.dumps([
        {"id": f"s{i+1}", "type": "shortcut", "data": {"shortcut_name": label, "col": 3}}
        for i, (label, _, _) in enumerate(shortcuts_data)
    ])
    ws.save(ignore_permissions=True)
    frappe.db.commit()
```

---

## Stability notes

- Все исправления (`get_count`/`get_list` graceful degradation, workspace v15 format) уже в репо — переживут rebuild.
- `AIRFLOW_DB_URL` не задан → doctypes возвращают `0`/`[]`, workspace не падает.
- Fixture JSON (`workspace/airflow_manager/airflow_manager.json`) синхронизирован с v15 форматом.

---

## Out of scope

- CI/CD pipeline (GitHub Actions для публикации образов) — отдельная задача после разделения репо.
- Аутентификация пользователей через внешний SSO.
- Версионирование пакетов (semver strategy) — решается при создании первого релиза.
