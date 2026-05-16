# Connection-centric migration

## Deploy order

1. Deploy `airflow-frappe` and `airflow-manager` images; run `bench migrate`.
2. Run migration (dry-run first):

```bash
DRY_RUN=1 bench --site SITE execute frappe_airflow.migrate_connections.run
bench --site SITE execute frappe_airflow.migrate_connections.run
```

If `CLIENT_REGISTRY` is missing but connections already exist in Frappe:

```bash
REBUILD_ONLY=1 bench --site SITE execute frappe_airflow.migrate_connections.run
```

If migration fails with JSON error, check the variable (scheduler container):

```bash
docker compose exec airflow-scheduler airflow variables get CLIENT_REGISTRY
```

Encrypted variables need `AIRFLOW_FERNET_KEY` on `airflow-manager` to match `AIRFLOW__CORE__FERNET_KEY`.

After updating `airflow-frappe`, rebuild without cache (otherwise Docker reuses old `COPY` layer):

```bash
docker compose -f docker-compose.yaml -f docker-compose.frappe.yaml --env-file .env --env-file .env.frappe \
  build --no-cache airflow-manager && docker compose ... up -d airflow-manager
```

Diagnose DB vs decrypted read:

```bash
bench --site SITE execute frappe_airflow.migrate_connections.diagnose_client_registry
```

3. Deploy `airflow` DAG/helpers.
4. Set Airflow Variable `USE_CONNECTION_REGISTRY=true` (default).
5. Verify `CONNECTION_REGISTRY`, `DAG_REGISTRY`, `dag_table_config_*` in Admin → Variables.
6. After successful DAG runs, remove `CLIENT_REGISTRY`.

## Model

1. `AM Airflow Connection` — marketplace credentials + `target_db_connection`
2. `AM DAG Config` — which connections run in each DAG
3. `AM Table Config` — per-DAG table settings (`dag_table_config_{dag_id}`)
