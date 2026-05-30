frappe.listview_settings["AM Airflow Connection"] = {
  onload(listview) {
    listview.bulk_operations.delete = function (docnames, done) {
      if (!docnames || !docnames.length) {
        frappe.msgprint(__("Select at least one connection"));
        return;
      }

      const count = docnames.length;
      const message =
        count === 1
          ? __("Delete 1 connection permanently?")
          : __("Delete {0} connections permanently?", [count]);

      frappe.confirm(message, () => {
        listview.disable_list_update = true;
        frappe.call({
          method: "frappe_airflow.api.bulk_delete_connections",
          args: { conn_ids: docnames },
          freeze: true,
          freeze_message: __("Deleting {0} connection(s)...", [count]),
          callback(r) {
            listview.disable_list_update = false;
            const result = r.message || {};
            const deleted = result.deleted || [];
            const failed = result.failed || [];

            if (failed.length) {
              const details = failed
                .map((row) => `${frappe.utils.escape_html(row.conn_id)}: ${frappe.utils.escape_html(row.error)}`)
                .join("<br>");
              frappe.msgprint({
                title: __("Some deletions failed"),
                message: __("Deleted: {0}<br><br>Failed:<br>{1}", [
                  deleted.length,
                  details,
                ]),
                indicator: deleted.length ? "orange" : "red",
              });
            } else if (deleted.length) {
              frappe.show_alert({
                message: __("Deleted {0} connection(s)", [deleted.length]),
                indicator: "green",
              });
            }

            if (deleted.length) {
              frappe.utils.play_sound("delete");
              listview.clear_checked_items();
              listview.refresh();
              if (done) done();
            }
          },
          error() {
            listview.disable_list_update = false;
          },
        });
      });
    };
  },
};
