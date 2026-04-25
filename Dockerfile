FROM python:3.11-slim

# System deps
RUN apt-get update && apt-get install -y \
    git curl wget mariadb-client \
    && rm -rf /var/lib/apt/lists/*

# Install bench CLI
RUN pip install frappe-bench

# App dependencies
COPY requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt

WORKDIR /home/frappe/frappe-bench

# Copy our app source
COPY apps/airflow_manager /home/frappe/frappe-bench/apps/airflow_manager
RUN pip install -e /home/frappe/frappe-bench/apps/airflow_manager

EXPOSE 8000
CMD ["bench", "start"]
