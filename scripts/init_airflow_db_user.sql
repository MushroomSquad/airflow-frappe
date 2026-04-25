-- Run once against Airflow PostgreSQL as superuser.
-- Creates a restricted user for the Frappe app.
--
-- Usage:
--   psql -h <airflow-pg-host> -U postgres -d airflow -f scripts/init_airflow_db_user.sql

CREATE USER airflow_ui WITH PASSWORD 'change_me';
GRANT CONNECT ON DATABASE airflow TO airflow_ui;
GRANT USAGE ON SCHEMA public TO airflow_ui;

-- Write access for connection and variable management
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE connection TO airflow_ui;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE variable TO airflow_ui;
GRANT USAGE, SELECT ON SEQUENCE connection_id_seq TO airflow_ui;
GRANT USAGE, SELECT ON SEQUENCE variable_id_seq TO airflow_ui;

-- Read-only for DAG list
GRANT SELECT ON TABLE dag TO airflow_ui;
