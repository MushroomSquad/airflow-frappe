#!/usr/bin/env python3
"""CLI wrapper — prefer: bench --site SITE execute frappe_airflow.migrate_connections.run"""
from frappe_airflow.migrate_connections import run

if __name__ == "__main__":
    run()
