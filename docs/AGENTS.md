# Инструкции для ИИ-агентов — Airflow Manager (frappe_airflow)

Краткий контракт для агентов. Полная документация: **[AIRFLOW_MANAGER.md](./AIRFLOW_MANAGER.md)**.

---

## Роль системы

Frappe-приложение **`frappe_airflow`** — UI для управления метаданными Airflow:

- **PostgreSQL (Airflow):** `connection`, `dag` — через virtual DocTypes.
- **MariaDB (Frappe site):** **`AM DAG Config`** — какие `conn_id` привязаны к какому `dag_id`, плюс `db_connection`.

Не путать с **marketplace-new** (сами DAG на Python) — там runtime пока может читать `CLIENT_REGISTRY` Variable, не Frappe.

---

## Репозитории и деплой

```
~/airflow/              ← docker compose (marketplace-new + frappe overlay)
~/airflow-frappe/       ← git pull ОБЯЗАТЕЛЕН перед build
~/airflow-manager/      ← entrypoint, WSGI
```

Build: `marketplace-new/docker/frappe-manager.Dockerfile`, context = родитель `airflow-frappe` + `airflow-manager`.

**Ошибка агента из чата:** `git pull` из `~/airflow` не обновляет код — репозиторий в `~/airflow-frappe`.

После изменений JS/DocType: в контейнере `migrate` + **`bench build --force`** + `clear-cache`.

Hotfix без rebuild: `docker cp` в `.../apps/frappe_airflow/` + migrate + build + restart.

---

## Актуальный UX (≥ `3adc4f1`)

### Форма **AM Airflow DAG** (virtual)

| Элемент | Реализация |
|---------|------------|
| Pause / Schedule | Airflow `dag` |
| Default Database | Link → AM Database Connection → AM DAG Config |
| **Connections** | Секция `connections_section` + **inline чекбоксы** (`dag_connections.js`) |
| Скрытые поля | `connection_options`, `selected_connections` (JSON) |

**Save на DAG** → `set_dag_paused` + `set_selected_connections` в MariaDB.

**Не делать:** только MultiCheck на virtual DocType; не возвращать UX «Configure Connections» link без чекбоксов, если пользователь просит выбор на той же форме.

### Форма **AM Airflow Connection** (virtual)

- Поля: `platform`, `slug`, `conn_id` (auto), `conn_type`, credentials.
- Запись в Airflow `connection` + `extra` JSON + companion для Ozon.
- **Legacy:** inference из `conn_id` если `extra` пустой (`dag_platform.infer_connection_profile`).

### **AM DAG Config** (MariaDB)

- Дублирует редактирование: MultiCheck + тот же `dag_connections.js`.
- `selected_connections` — JSON array в БД.

---

## Правила фильтра коннекшенов для DAG

Код: `airflow_db/dag_platform.py`, `build_dag_connection_options()`.

1. `infer_dag_platform(dag_id)` — по префиксу `wb_`, `oz_`, `ms_`.
2. `conn_matches_dag(conn_type, platform, dag_id, conn_id=...)` — с legacy normalization.
3. `oz_perf` только на `oz_adv_*`; `oz_seller` не на `oz_adv_*`.
4. Companion (`oz_client_seller_id_*`, …) — исключить.

---

## Критические файлы при задачах

| Задача | Файлы |
|--------|--------|
| Чекбоксы DAG | `am_airflow_dag.py/json`, `public/js/dag_connections.js`, `hooks.py` (`app_include_js`) |
| Список коннекшенов | `am_airflow_connection.py` → `get_list` + `_from_airflow_row` |
| Новый conn_id / Ozon | `dag_platform.py`, `connection_sync.py`, `am_airflow_connection.py` |
| Сохранение привязок | `dag_connection_sync.py` |
| Link field search | `doctype_utils.py` (`as_link_search_rows`) |
| API | `api.py` |
| Деплой | `marketplace-new/docker/frappe-manager.Dockerfile`, `airflow-manager/docker/entrypoint.sh` |

---

## Чего не ломать

1. **Link search** на virtual DocTypes — возвращать tuples для `as_list` searches.
2. **entrypoint.sh** — не убирать `bench build --force` (assets на volume).
3. **Fernet** — пустой password при update = сохранить старый encrypted password.
4. **Companion rows** — не показывать в UI списка/чекбоксов.

---

## Проверка после изменений

```bash
# В контейнере
grep connections_section apps/frappe_airflow/.../am_airflow_dag.json
bench --site airflow-manager.localhost migrate
bench build --force
```

```python
# bench console
doc = frappe.get_doc("AM Airflow DAG", "wb_orders_etl_dag")
assert hasattr(doc, "connection_options")
```

Браузер: под секцией Connections есть `.dag-connections-mount` с input checkbox.

---

## Диагностика для пользователя

Если «не вижу чекбоксы»:

1. `git log -1` на сервере в `airflow-frappe` ≥ `3adc4f1`.
2. Образ пересобран или `docker cp` + `bench build`.
3. Hard refresh браузера.
4. DAG действительно `wb_*` / `oz_*` / `ms_*` — иначе пустой список корректен.
5. `getdoc` содержит `connection_options` — иначе старый код или migrate.

---

## Коммиты

Пушить в **`MushroomSquad/airflow-frappe`**, ветка `master`. Пользователь деплоит pull + build из `~/airflow`.

Не коммитить `.env`, секреты, `.planning/` (если не просят).

---

## Связанные документы

- [AIRFLOW_MANAGER.md](./AIRFLOW_MANAGER.md) — полная документация
- `docs/superpowers/specs/2026-04-25-frappe-airflow-split-design.md` — исторический design (может отставать от кода)
- `codex-migration/TASK.md` — миграционные заметки

При расхождении design doc и `master` — **верить коду и AIRFLOW_MANAGER.md**.
