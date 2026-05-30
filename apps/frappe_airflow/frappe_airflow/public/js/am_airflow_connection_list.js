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
          method: "frappe.desk.reportview.delete_items",
          type: "POST",
          args: {
            doctype: listview.doctype,
            items: JSON.stringify(docnames),
          },
          freeze: true,
          freeze_message: __("Deleting {0} connection(s)...", [count]),
            callback(r) {
            listview.disable_list_update = false;
            const failed = r.message || [];
            const deletedCount = count - (Array.isArray(failed) ? failed.length : 0);

            if (Array.isArray(failed) && failed.length) {
              frappe.msgprint({
                title: __("Some deletions failed"),
                message: __("Failed: {0}", [failed.join(", ")]),
                indicator: deletedCount ? "orange" : "red",
              });
            } else if (deletedCount > 0) {
              frappe.show_alert({
                message: __("Deleted {0} connection(s)", [deletedCount]),
                indicator: "green",
              });
            }

            if (deletedCount > 0) {
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
