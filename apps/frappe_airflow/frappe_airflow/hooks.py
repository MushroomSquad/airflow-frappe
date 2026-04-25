app_name = "frappe_airflow"
app_title = "Airflow Manager"
app_publisher = "Fldrspro"
app_description = "Manage Airflow connections, variables, and configuration"
app_version = "0.1.0"

app_include_js = []
app_include_css = []

after_install = "frappe_airflow.setup.after_install"
on_session_creation = "frappe_airflow.setup.set_default_workspace"

fixtures = [
    {"dt": "Workspace", "filters": [["module", "=", "Airflow Manager"]]}
]

doc_events = {}
