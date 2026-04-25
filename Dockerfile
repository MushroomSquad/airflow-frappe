FROM python:3.11-slim

# System deps: Node.js + build tools for Frappe, MariaDB client for bench
RUN apt-get update && apt-get install -y --no-install-recommends \
        git curl wget \
        nodejs npm \
        libmariadb-dev \
        build-essential \
        mariadb-client \
    && npm install -g yarn \
    && rm -rf /var/lib/apt/lists/*

# Frappe runs as non-root
RUN useradd -ms /bin/bash frappe

USER frappe
WORKDIR /home/frappe

ENV PATH="/home/frappe/.local/bin:$PATH"

# Install bench + our runtime deps into user site-packages
RUN pip install --user \
        frappe-bench \
        sqlalchemy>=2.0 \
        psycopg2-binary \
        cryptography

# bench init downloads Frappe v15 (~200 MB) and sets up the venv.
# This layer is cached after first build — subsequent builds are fast.
RUN bench init \
        --skip-redis-config-generation \
        --frappe-branch version-15 \
        frappe-bench

WORKDIR /home/frappe/frappe-bench

# Copy our app and install it into the bench venv
COPY --chown=frappe:frappe apps/airflow_manager apps/airflow_manager
RUN ./env/bin/pip install -e apps/airflow_manager

COPY --chown=frappe:frappe docker/entrypoint.sh /home/frappe/entrypoint.sh
RUN chmod +x /home/frappe/entrypoint.sh

EXPOSE 8000
ENTRYPOINT ["/home/frappe/entrypoint.sh"]
