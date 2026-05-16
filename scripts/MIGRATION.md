# Connection-centric migration

## Deploy order

1. Deploy `airflow-frappe` and `airflow-manager` images; run `bench migrate`.
2. Run migration (dry-run first):

```bash
DRY_RUN=1 bench --site SITE execute frappe_airflow.migrate_connections.run
bench --site SITE execute frappe_airflow.migrate_connections.run
```

3. Deploy `airflow` DAG/helpers.
4. Set Airflow Variable `USE_CONNECTION_REGISTRY=true` (default).
5. Verify `CONNECTION_REGISTRY`, `DAG_REGISTRY`, `dag_table_config_*` in Admin → Variables.
6. After successful DAG runs, remove `CLIENT_REGISTRY`.

## Model

1. `AM Airflow Connection` — marketplace credentials + `target_db_connection`
2. `AM DAG Config` — which connections run in each DAG
3. `AM Table Config` — per-DAG table settings (`dag_table_config_{dag_id}`)
