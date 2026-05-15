frappe.ui.form.on("AM Airflow DAG", {
  refresh(frm) {
    if (!frm.doc.dag_id) {
      return;
    }
    frappe.call({
      method: "frappe_airflow.api.get_dag_connection_options",
      args: { dag_id: frm.doc.dag_id },
      callback(r) {
        const options = (r.message || [])
          .map((row) => `${row.label}\n${row.value}`)
          .join("\n");
        frm.set_df_property("selected_connections", "options", options);
        frm.refresh_field("selected_connections");
      },
    });
  },
});
