(function () {
  if (window.__frappe_airflow_dag_connections) {
    return;
  }
  window.__frappe_airflow_dag_connections = true;

  function parse_selected_connections(raw) {
    if (!raw) {
      return [];
    }
    if (Array.isArray(raw)) {
      return raw.filter(Boolean);
    }
    const text = String(raw).trim();
    if (!text) {
      return [];
    }
    if (text.startsWith("[")) {
      try {
        const data = JSON.parse(text);
        if (Array.isArray(data)) {
          return data.filter(Boolean);
        }
      } catch (e) {
        // fall through
      }
    }
    return text.split("\n").map((line) => line.trim()).filter(Boolean);
  }

  function parse_connection_options(raw) {
    if (!raw) {
      return [];
    }
    try {
      const data = JSON.parse(raw);
      return Array.isArray(data) ? data : [];
    } catch (e) {
      return [];
    }
  }

  function options_to_multicheck_string(options) {
    return options.map((row) => `${row.label}\n${row.value}`).join("\n");
  }

  function setup_connections_multicheck(frm, dag_id) {
    const field = frm.fields_dict.selected_connections;
    if (!field || !dag_id) {
      return;
    }

    const apply = (options) => {
      frm.set_df_property(
        "selected_connections",
        "options",
        options_to_multicheck_string(options)
      );
      if (!options.length) {
        frm.set_df_property(
          "selected_connections",
          "description",
          __("No marketplace connections match this DAG platform.")
        );
      } else {
        frm.set_df_property("selected_connections", "description", "");
      }
      const selected = parse_selected_connections(frm.doc.selected_connections);
      frm.set_value("selected_connections", selected);
      frm.refresh_field("selected_connections");
    };

    const cached = parse_connection_options(frm.doc.connection_options);
    if (cached.length) {
      apply(cached);
      return;
    }

    frappe.call({
      method: "frappe_airflow.api.get_dag_connection_options",
      args: { dag_id: dag_id },
      callback(r) {
        apply(r.message || []);
      },
      error(r) {
        const msg = (r && r.message) || __("Failed to load connections");
        frm.set_df_property("selected_connections", "options", "");
        frm.set_df_property("selected_connections", "description", msg);
        frm.refresh_field("selected_connections");
      },
    });
  }

  frappe.ui.form.on("AM DAG Config", {
    refresh(frm) {
      if (!frm.doc.dag_id) {
        return;
      }
      frappe.call({
        method: "frappe_airflow.api.prepare_dag_config_form",
        args: { dag_id: frm.doc.dag_id },
        callback(r) {
          if (r.message && r.message.connection_options) {
            frm.set_value("connection_options", r.message.connection_options);
          }
          setup_connections_multicheck(frm, frm.doc.dag_id);
        },
      });
    },
    before_save(frm) {
      const value = frm.doc.selected_connections;
      if (Array.isArray(value)) {
        frm.set_value("selected_connections", JSON.stringify(value));
      }
    },
  });

  frappe.ui.form.on("AM Airflow DAG", {
    refresh(frm) {
      if (frm.doc.dag_config) {
        frm.add_custom_button(__("Configure Connections"), () => {
          frappe.set_route("Form", "AM DAG Config", frm.doc.dag_config);
        });
      }
    },
  });
})();
