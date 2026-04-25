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
