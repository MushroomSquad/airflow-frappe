# Airflow Manager (Frappe) — полная документация

Актуально для репозитория `airflow-frappe`, ветка `master`, коммит **`3adc4f1`** и новее.

Документ описывает архитектуру, потоки данных, UI, деплой и типичные проблемы. Предназначен для разработчиков и ИИ-агентов, которые продолжают работу над интеграцией Frappe ↔ Apache Airflow.

---

## 1. Контекст и репозитории

### 1.1 Три связанных репозитория

| Репозиторий | Роль |
|-------------|------|
| **airflow-frappe** | Frappe-приложение `frappe_airflow`: virtual DocTypes, чтение/запись Airflow PostgreSQL (`connection`, `dag`, `variable`), UI Airflow Manager |
| **airflow-manager** | Тонкая обёртка: WSGI, entrypoint с `bench migrate` + `bench build`, Docker-образ поверх bench |
| **marketplace-new** | Airflow DAGs, `CLIENT_REGISTRY`, production `docker-compose` + overlay `docker-compose.frappe.yaml` |

### 1.2 Production-стек

Сервис **`airflow-manager`** в compose — это Frappe UI, а не отдельный «голый» `airflow-frappe` compose.

Типичная структура на сервере (`airflow-test`):

```
~/
├── airflow/              # clone marketplace-new (compose запускается отсюда)
├── airflow-frappe/       # clone MushroomSquad/airflow-frappe
└── airflow-manager/      # clone airflow-manager
```

Docker build context для `airflow-manager` — **родительская папка** (`..` от `marketplace-new`), Dockerfile:

`marketplace-new/docker/frappe-manager.Dockerfile`

В образ копируются:

- `airflow-frappe/apps/frappe_airflow` → `/home/frappe/frappe-bench/apps/frappe_airflow`
- `airflow-manager/apps/airflow_manager` → `/home/frappe/frappe-bench/apps/airflow_manager`

**Важно:** `git pull` нужно делать в `~/airflow-frappe`, а compose — из `~/airflow`. Путь `/path/to/fldrspro` из примеров — только для dev-машины разработчика.

### 1.3 Две базы данных

| БД | Назначение |
|----|----------|
| **MariaDB** (Frappe site) | Метаданные Frappe, **`AM DAG Config`** (выбранные коннекшены для DAG), workspace |
| **PostgreSQL** (Airflow) | Таблицы Airflow: `connection`, `dag`, `variable` — источник правды для коннекшенов и статуса DAG |

Переменная окружения: `AIRFLOW_DB_URL` (например `postgresql+psycopg2://airflow:airflow@postgres:5432/airflow`).

Пароли коннекшенов в Airflow хранятся **Fernet-encrypted** (`connection.password`, `is_encrypted`).

---

## 2. Архитектура приложения

### 2.1 Слои

```
┌─────────────────────────────────────────────────────────────┐
│  Frappe UI (Desk)                                           │
│  AM Airflow DAG | AM Airflow Connection | AM Database Conn  │
│  AM DAG Config                                              │
└───────────────────────────┬─────────────────────────────────┘
                            │ virtual DocType controllers
                            │ whitelisted API (api.py)
┌───────────────────────────▼─────────────────────────────────┐
│  frappe_airflow/airflow_db/                                 │
│  connection_manager, dag_reader, dag_connection_sync,       │
│  dag_platform, connection_meta, connection_sync, ...        │
└───────────────────────────┬─────────────────────────────────┘
                            │ SQLAlchemy
┌───────────────────────────▼─────────────────────────────────┐
│  PostgreSQL (Airflow metadata DB)                           │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│  MariaDB — tabAM DAG Config (selected_connections JSON)       │
└─────────────────────────────────────────────────────────────┘
```

### 2.2 DocTypes

#### Virtual (данные в Airflow PG)

| DocType | Таблица Airflow | Операции |
|---------|-----------------|----------|
| **AM Airflow Connection** | `connection` | CRUD маркетплейс-коннекшенов |
| **AM Airflow DAG** | `dag` | Read + pause/unpause + конфиг через AM DAG Config |
| **AM Database Connection** | `connection` (`conn_type=postgres`) | CRUD БД-коннекшенов |
| **AM Airflow Variable** | `variable` | CRUD (если включено в workspace) |

#### Обычный (MariaDB)

| DocType | Назначение |
|---------|------------|
| **AM DAG Config** | `dag_id` = имя документа; `selected_connections` (JSON); `db_connection`; legacy child table `connections` |
| **AM DAG Connection** | Child row: поле `connection` (Link) — синхронизируется из JSON для обратной совместимости |

### 2.3 Удалённая модель (рефакторинг)

Раньше были **AM Client** / **AM Cabinet** и `CLIENT_REGISTRY` в Variable. Сейчас:

- **Первичная сущность** — Airflow `connection` с метаданными в `connection.extra` (JSON).
- **Привязка DAG ↔ connection** — `AM DAG Config.selected_connections` (JSON-массив `conn_id`).
- DAG в Airflow **не получает** автоматически Variable `CLIENT_REGISTRY` при сохранении в Frappe (это возможное будущее расширение).

---

## 3. Маркетплейс-коннекшены

### 3.1 Поля формы AM Airflow Connection

| Поле | Описание |
|------|----------|
| `platform` | `wb` \| `oz` \| `ms` \| `ym` |
| `slug` | Уникальный идентификатор клиента/кабинета |
| `display_name` | Отображаемое имя (в `extra`) |
| `conn_id` | Read-only после создания; генерируется из шаблона |
| `conn_type` | `wb` \| `oz_seller` \| `oz_perf` \| `ms` \| `other` |
| Credentials | Зависят от `conn_type` (см. `depends_on` в JSON) |

### 3.2 Шаблоны `conn_id`

Определены в `airflow_db/dag_platform.py`:

| conn_type | Шаблон conn_id |
|-----------|----------------|
| `wb` | `wb_api_token_{slug}` |
| `oz_seller` | `oz_api_token_{slug}` |
| `oz_perf` | `oz_client_perf_id_{slug}` |
| `ms` | `ms_api_token_{slug}` |

### 3.3 Companion-строки (Ozon)

При сохранении `oz_seller` / `oz_perf` создаются дополнительные строки в `connection` (`connection_sync.py`):

| conn_type | Companion conn_id |
|-----------|-------------------|
| `oz_seller` | `oz_client_seller_id_{slug}` |
| `oz_perf` | `oz_client_perf_secret_{slug}` |

Companion не показываются в списке и не предлагаются в чекбоксах DAG (`is_companion` в `extra` или префикс `conn_id`).

### 3.4 Legacy-коннекшены

Записи без `extra` (например `wb_filippov`, `conn_type=other`) разбираются в `infer_connection_profile()`:

- префиксы `wb_api_token_`, `oz_api_token_`, …;
- укороченные id `wb_{slug}`, `oz_{slug}`, `ms_{slug}`;
- маппинг старых `conn_type`: `wb_token` → `wb`, `oz_token` → `oz_seller`, и т.д.

Это нужно для списка, фильтра DAG и отображения Marketplace/Slug.

---

## 4. Привязка коннекшенов к DAG

### 4.1 Определение платформы DAG

`infer_dag_platform(dag_id)` по префиксу id:

| Префикс | Платформа |
|---------|-----------|
| `wb_` | wb |
| `oz_` | oz |
| `ms_` | ms |
| `amo_` | amo (коннекшены не матчятся) |
| иное | `None` — **нет подходящих маркетплейс-коннекшенов** |

### 4.2 Правила фильтра (`conn_matches_dag`)

- Платформа коннекшена = платформа DAG.
- `oz_perf` — **только** DAG с префиксом `oz_adv_` (`is_perf_dag`).
- `oz_seller` — **не** показывается на `oz_adv_*` DAG.

### 4.3 Где хранится выбор

**Источник правды для UI:** `AM DAG Config.selected_connections` — JSON-массив строк `conn_id`, например:

```json
["wb_filippov", "wb_api_token_filippov_sv"]
```

При сохранении синхронизируется child table `connections` (legacy).

### 4.4 UI на форме AM Airflow DAG (актуально с `3adc4f1`)

1. **Сервер** (`am_airflow_dag.py` → `load_from_db`):
   - читает DAG из Airflow;
   - создаёт `AM DAG Config`, если нет;
   - заполняет скрытые поля `connection_options` (JSON опций) и `selected_connections` (JSON выбранных id);
   - подставляет `db_connection` из AM DAG Config.

2. **Клиент** (`public/js/dag_connections.js`, `app_include_js`):
   - в секции **Connections** вставляет чекбоксы в `.dag-connections-mount`;
   - опции из `connection_options` или API `get_dag_connection_options`;
   - при изменении чекбоксов пишет в `frm.doc.selected_connections` и помечает форму dirty.

3. **Save** (`db_update`):
   - `set_dag_paused` в Airflow;
   - `set_selected_connections(dag_id, conn_ids, db_connection=...)`.

**Не использовать** MultiCheck на virtual DocType как единственный UI — в Frappe он ненадёжен без options.

### 4.5 UI на форме AM DAG Config (запасной путь)

Тот же `dag_connections.js`: MultiCheck + API `prepare_dag_config_form`. Можно править конфиг напрямую по имени `dag_id`.

### 4.6 Автопривязка нового коннекшена

`assign_connection_to_matching_dags(conn_id, conn_type)` при `db_insert` AM Airflow Connection добавляет `conn_id` во все подходящие `AM DAG Config`.

---

## 5. API (whitelisted)

| Метод | Назначение |
|-------|------------|
| `frappe_airflow.api.get_group_by_count` | Пустые группы для virtual list (override) |
| `frappe_airflow.api.get_dag_connection_options` | Список `{label, value}` для чекбоксов |
| `frappe_airflow.api.prepare_dag_config_form` | JSON `connection_options` для AM DAG Config |
| `frappe_airflow.api.preview_conn_id` | Превью conn_id по platform/slug/type |
| `frappe_airflow.api.get_conn_type_options` | Допустимые conn_type для platform |
| `frappe_airflow.api.bulk_delete_connections` | Массовое удаление коннекшенов с каскадом |

---

## 6. Ключевые модули Python

| Файл | Ответственность |
|------|-----------------|
| `airflow_db/connection_manager.py` | CRUD `connection`, Fernet, list без postgres для marketplace |
| `airflow_db/connection_delete.py` | Каскадное удаление коннекшена (DAG config, table config, registry) |
| `airflow_db/connection_meta.py` | `pack_extra` / `unpack_extra` (platform, slug, display_name, is_companion) |
| `airflow_db/dag_platform.py` | Платформы, шаблоны id, legacy inference, `conn_matches_dag` |
| `airflow_db/dag_connection_options.py` | `build_dag_connection_options(dag_id)` |
| `airflow_db/dag_connection_sync.py` | Чтение/запись `selected_connections`, автопривязка |
| `airflow_db/connection_sync.py` | Ozon companion rows |
| `airflow_db/dag_reader.py` | Список DAG, pause |
| `doctype_utils.py` | Virtual row apply, **link search tuple rows** (`as_link_search_rows`) |
| `api.py` | Whitelisted endpoints |
| `public/js/dag_connections.js` | Inline чекбоксы DAG + MultiCheck AM DAG Config |
| `public/js/am_airflow_connection.js` | Фильтр conn_type, preview conn_id |

### 6.1 Virtual DocType и Link search

Frappe `search_link` с `as_list=True` ожидает **кортежи**, не dict. Иначе `KeyError: 0`. Решение: `is_link_search()` + `as_link_search_rows()` в `get_list` для virtual connection doctypes.

---

## 7. Деплой и обновление

### 7.1 Стандартный деплой (сервер)

```bash
cd ~/airflow-frappe && git pull origin master && git log -1 --oneline

cd ~/airflow
docker compose -f docker-compose.yaml -f docker-compose.frappe.yaml \
  --env-file .env --env-file .env.frappe \
  build airflow-manager

docker compose -f docker-compose.yaml -f docker-compose.frappe.yaml \
  --env-file .env --env-file .env.frappe \
  up -d airflow-manager
```

- **`--no-cache`** — только при необходимости полной пересборки; требует доступ к `deb.debian.org` при сборке базового слоя.
- Entrypoint при старте: `migrate` → **`bench build --force`** → `clear-cache` (иначе старый CSS/JS на volume `frappe-sites`).

### 7.2 Hotfix без rebuild (сеть/apt сломаны)

```bash
cd ~/airflow-frappe && git pull origin master

docker cp ~/airflow-frappe/apps/frappe_airflow/. \
  airflow-airflow-manager-1:/home/frappe/frappe-bench/apps/frappe_airflow/

cd ~/airflow
docker compose -f docker-compose.yaml -f docker-compose.frappe.yaml \
  --env-file .env --env-file .env.frappe \
  exec airflow-manager bench --site airflow-manager.localhost migrate

docker compose -f docker-compose.yaml -f docker-compose.frappe.yaml \
  --env-file .env --env-file .env.frappe \
  exec airflow-manager bench build --force

docker compose -f docker-compose.yaml -f docker-compose.frappe.yaml \
  --env-file .env --env-file .env.frappe \
  exec airflow-manager bench --site airflow-manager.localhost clear-cache

docker compose -f docker-compose.yaml -f docker-compose.frappe.yaml \
  --env-file .env --env-file .env.frappe \
  restart airflow-manager
```

Имя контейнера: `docker ps | grep airflow-manager`.

### 7.3 Проверка версии в контейнере

```bash
docker compose ... exec airflow-manager \
  grep connections_section apps/frappe_airflow/frappe_airflow/airflow_manager/doctype/am_airflow_dag/am_airflow_dag.json
```

Должна быть строка `"connections_section"`.

### 7.4 Проверка в bench console

```python
import frappe
frappe.get_meta("AM Airflow DAG").get_field("connections_section")
doc = frappe.get_doc("AM Airflow DAG", "wb_orders_etl_dag")
print(doc.connection_options[:200])
print(doc.selected_connections)
```

---

## 8. Диагностика UI

### 8.1 Браузер

1. Network → запрос `getdoc` для `AM Airflow DAG` → в `docs[0]` должны быть `connection_options`, `selected_connections`, `connections_section`.
2. Network → загрузка `dag_connections.js` (или bundle с этим кодом).
3. Console:
   ```javascript
   cur_frm.doc.connection_options
   cur_frm.doc.selected_connections
   document.querySelector('.dag-connections-mount')
   ```

### 8.2 Типичные симптомы

| Симптом | Причина |
|---------|---------|
| Пусто под «Connections», только заголовок | Старый образ / не выполнен `bench build` / нет `connections_section` в meta |
| Текст «Open Configure Connections» | Версия `ad8587d`, не `3adc4f1` |
| «No marketplace connections match…» | DAG без префикса `wb_`/`oz_`/`ms_` (например `return_etl_dag`) |
| `KeyError: 0` в Link | Старый код list без tuple rows |
| CSS сломан | Нет `bench build` после migrate (volume `sites/assets`) |
| Build Docker падает на apt | Нет сети до Debian; использовать hotfix `docker cp` или build без `--no-cache` |

---

## 9. Связь с marketplace-new (Airflow DAGs)

DAGs в `marketplace-new` historically используют:

- `CLIENT_REGISTRY` (Airflow Variable);
- `conn_id` вида `wb_api_token_{slug}`.

Frappe UI **пишет в `AM DAG Config`**, не в Variable. Пока DAG-код читает Variable, смена привязок только в Frappe **не изменит** runtime DAG без отдельной синхронизации Variable (не реализовано в текущей версии).

При добавлении синхронизации: писать JSON в Variable из `get_selected_connections(dag_id)` по согласованному формату с командой marketplace.

---

## 10. Удаление коннекшенов

Удаление **AM Airflow Connection** (форма или list Actions → Delete) выполняет каскад:

| Что очищается | Где |
|---------------|-----|
| Строка коннекшена + Ozon companions | Airflow PostgreSQL `connection` |
| `CONNECTION_REGISTRY` | Airflow Variable |
| `conn_id` в привязках DAG | `AM DAG Config.selected_connections` → `DAG_REGISTRY` |
| Child rows | `AM DAG Connection` |
| Per-connection table config | `AM Table Config` → `dag_table_config_{dag_id}` |

Код: `airflow_db/connection_delete.py`, API массового удаления: `frappe_airflow.api.bulk_delete_connections`.

List view переопределяет стандартный Frappe bulk delete (порог 10 записей + фоновая очередь без ответа UI) на синхронный вызов `bulk_delete_connections` — см. `public/js/am_airflow_connection_list.js`.

---

## 11. История коммитов (ориентир)

| Коммит | Содержание |
|--------|------------|
| `2b8966a` | Рефакторинг: connections primary, DAG pause, AM DAG Config |
| `b739a3d` | Fix link search tuples для virtual DocTypes |
| `1f934d9` | MultiCheck, platform filter, auto-assign, slug в extra |
| `ad8587d` | Legacy inference; AM DAG Config + summary/link (промежуточный UX) |
| **`3adc4f1`** | **Inline чекбоксы на форме DAG** (актуальный UX) |

---

## 12. Тесты

```bash
cd apps/frappe_airflow
PYTHONPATH=. python -m pytest tests/test_dag_platform.py tests/test_connection_delete.py -q
# или без pytest:
PYTHONPATH=. python3 tests/test_connection_delete.py
```

Тесты inference не требуют PostgreSQL. Интеграционные тесты connection_manager — с SQLAlchemy и тестовой БД.

---

## 13. Ограничения и backlog

- Синхронизация `selected_connections` → Airflow Variable `CLIENT_REGISTRY` — **нет**.
- DAG `amo_*`, `return_*` без префикса маркетплейса — пустой список коннекшенов (by design).
- Virtual DocType + MultiCheck без JS — **не поддерживать** как основной UI.
- `airflow-manager` образ из `ghcr.io/fldrspro/airflow-manager` без пересборки frappe-frappe **не подтянет** изменения `frappe_airflow` — нужен build с context, включающим `airflow-frappe/`.

---

## 14. Контакты и workspace

Frappe workspace: **Airflow Manager** (`setup.py` / fixtures). Shortcuts: Connections, DAGs, Database Connections, Variables.

Модуль Frappe: **Airflow Manager** (`module` в DocType JSON).
