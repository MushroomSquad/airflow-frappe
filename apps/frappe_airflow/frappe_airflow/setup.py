"""Post-install setup: workspace and default homepage."""
import json

import frappe


def after_install():
    _setup_workspace()
    _hide_default_workspaces()
    frappe.db.set_value(
        "System Settings", "System Settings", "default_app", "frappe_airflow"
    )
    frappe.db.commit()


def set_default_workspace(login_manager):
    frappe.db.set_value(
        "User",
        frappe.session.user,
        "home_settings",
        '{"route": "Workspaces/Airflow Manager"}',
    )


def _setup_workspace():
    shortcuts_data = [
        ("DAGs", "AM Airflow DAG", "Gray"),
        ("Connections", "AM Airflow Connection", "Orange"),
        ("Databases", "AM Database Connection", "Orange"),
    ]

    if frappe.db.exists("Workspace", "Airflow Manager"):
        frappe.db.delete("Workspace Shortcut", {"parent": "Airflow Manager"})
        ws = frappe.get_doc("Workspace", "Airflow Manager")
    else:
        ws = frappe.new_doc("Workspace")
        ws.name = "Airflow Manager"
        ws.label = "Airflow Manager"
        ws.module = "Airflow Manager"
        ws.is_standard = 1
        ws.public = 1
        ws.icon = "server"
        ws.indicator_color = "blue"

    for label, link_to, color in shortcuts_data:
        ws.append(
            "shortcuts",
            {
                "type": "DocType",
                "label": label,
                "link_to": link_to,
                "color": color,
                "doc_view": "List",
            },
        )

    content = [
        {
            "id": f"s{i + 1}",
            "type": "shortcut",
            "data": {"shortcut_name": label, "col": 3},
        }
        for i, (label, _, _) in enumerate(shortcuts_data)
    ]
    ws.content = json.dumps(content)
    ws.flags.ignore_links = True
    ws.flags.ignore_validate = True
    ws.save(ignore_permissions=True)
    frappe.db.commit()


def _hide_default_workspaces():
    for name in [
        "Integrations",
        "Build",
        "Tools",
        "Users",
        "Welcome Workspace",
        "Website",
        "Home",
    ]:
        if frappe.db.exists("Workspace", name):
            frappe.db.set_value("Workspace", name, "is_hidden", 1)
    frappe.db.commit()
