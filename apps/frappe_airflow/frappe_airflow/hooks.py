app_name = "frappe_airflow"
app_title = "Airflow Manager"
app_publisher = "Fldrspro"
app_description = "Manage Airflow connections, variables, and configuration"
app_version = "0.1.0"

app_include_js = [
    "public/js/dag_connections.js",
    "public/js/dag_table_configs.js",
    "public/js/am_airflow_connection_list.js",
]
app_include_css = []

after_install = "frappe_airflow.setup.after_install"
after_migrate = "frappe_airflow.setup.after_migrate"
on_session_creation = "frappe_airflow.setup.set_default_workspace"

fixtures = [
    {"dt": "Workspace", "filters": [["module", "=", "Airflow Manager"]]}
]

doc_events = {}

doctype_js = {
    "AM Airflow Connection": "public/js/am_airflow_connection.js",
    "AM Airflow DAG": [
        "public/js/dag_connections.js",
        "public/js/dag_table_configs.js",
    ],
    "AM DAG Config": "public/js/dag_connections.js",
}

override_whitelisted_methods = {
    "frappe.desk.listview.get_group_by_count": "frappe_airflow.api.get_group_by_count"
}
