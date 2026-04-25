#!/bin/bash
set -euo pipefail

BENCH_DIR=/home/frappe/frappe-bench

# First-run: create site and install app
if [ ! -f "$BENCH_DIR/sites/$FRAPPE_SITE_NAME/site_config.json" ]; then
    echo "==> First run: creating site $FRAPPE_SITE_NAME ..."

    cd "$BENCH_DIR"

    bench set-config -g db_host "$DB_HOST"
    bench set-config -g db_port "${DB_PORT:-3306}"
    bench set-config -g redis_cache "redis://frappe-redis:6379/0"
    bench set-config -g redis_queue "redis://frappe-redis:6379/1"
    bench set-config -g redis_socketio "redis://frappe-redis:6379/2"

    bench new-site "$FRAPPE_SITE_NAME" \
        --db-host "$DB_HOST" \
        --db-name "$DB_NAME" \
        --db-user "$DB_USER" \
        --db-password "$DB_PASSWORD" \
        --admin-password "$ADMIN_PASSWORD" \
        --no-mariadb-socket

    bench --site "$FRAPPE_SITE_NAME" install-app airflow_manager

    echo "==> Site ready."
fi

cd "$BENCH_DIR"
exec bench serve --port 8000 --site "$FRAPPE_SITE_NAME"
