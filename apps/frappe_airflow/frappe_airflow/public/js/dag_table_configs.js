(function () {
  if (window.__frappe_airflow_dag_table_configs) {
    return;
  }
  window.__frappe_airflow_dag_table_configs = true;

  function tables_mount(frm) {
    const html_field = frm.fields_dict.tables_ui;
    if (html_field && html_field.$wrapper) {
      let $mount = html_field.$wrapper.find("> .dag-table-configs-mount");
      if (!$mount.length) {
        $mount = $('<div class="dag-table-configs-mount">').appendTo(html_field.$wrapper);
      }
      return $mount;
    }

    const section = frm.fields_dict.tables_section;
    if (section && section.$wrapper) {
      const $section = $(section.wrapper).closest(".form-section");
      if ($section.length) {
        let $mount = $section.find(".dag-table-configs-mount").first();
        if (!$mount.length) {
          const $body = $section.find(".section-body").first();
          $mount = $('<div class="dag-table-configs-mount">').appendTo($body.length ? $body : $section);
        }
        return $mount;
      }
    }
    return null;
  }

  function render_table_configs(frm) {
    const dag_id = frm.doc.dag_id;
    if (!dag_id) {
      return;
    }
    const $mount = tables_mount(frm);
    if (!$mount) {
      return;
    }

    $mount.html(
      '<p class="text-muted" style="margin:0 0 10px">' + __("Loading table settings...") + "</p>"
    );

    frappe.call({
      method: "frappe_airflow.api.get_dag_table_configs",
      args: { dag_id: dag_id },
      callback(r) {
        paint_table_configs(frm, $mount, r.message || []);
      },
      error() {
        $mount.html(
          '<p class="text-danger" style="margin:0">' +
            __("Failed to load table settings") +
            "</p>"
        );
      },
    });
  }

  function paint_table_configs(frm, $mount, rows) {
    const $toolbar = $(`
      <div class="dag-table-configs-toolbar" style="display:flex;align-items:center;gap:8px;margin-bottom:10px">
        <button type="button" class="btn btn-primary btn-sm btn-add-table-config">
          ${__("Add table config")}
        </button>
        <button type="button" class="btn btn-default btn-sm btn-open-table-list">
          ${__("Open full list")}
        </button>
        <span class="text-muted table-config-count"></span>
      </div>
    `);

    $toolbar.find(".btn-add-table-config").on("click", () => {
      frappe.new_doc("AM Table Config", {
        dag_id: frm.doc.dag_id,
        scope: "_default",
        enabled: 1,
        load_strategy: "append",
      });
    });

    $toolbar.find(".btn-open-table-list").on("click", () => {
      frappe.set_route("List", "AM Table Config", {
        dag_id: frm.doc.dag_id,
      });
    });

    $toolbar.find(".table-config-count").text(
      rows.length ? __("{0} config(s)", [rows.length]) : __("No table configs yet")
    );

    if (!rows.length) {
      $mount.empty().append($toolbar).append(
        '<p class="text-muted" style="margin:0">' +
          __("Add rules for which tables this DAG loads and how.") +
          "</p>"
      );
      return;
    }

    const $table = $(`
      <table class="table table-bordered table-sm dag-table-configs-table" style="margin:0">
        <thead>
          <tr>
            <th>${__("Table")}</th>
            <th>${__("Scope")}</th>
            <th>${__("Connection")}</th>
            <th>${__("Strategy")}</th>
            <th>${__("Enabled")}</th>
            <th></th>
          </tr>
        </thead>
        <tbody></tbody>
      </table>
    `);

    const $tbody = $table.find("tbody");
    rows.forEach((row) => {
      const enabled = row.enabled
        ? '<span class="indicator-pill green">' + __("Yes") + "</span>"
        : '<span class="indicator-pill red">' + __("No") + "</span>";
      const conn =
        row.scope === "connection" && row.connection
          ? frappe.utils.escape_html(row.connection)
          : "—";
      $tbody.append(`
        <tr data-name="${frappe.utils.escape_html(row.name)}">
          <td><a href="#" class="edit-table-config">${frappe.utils.escape_html(row.table_name)}</a></td>
          <td>${frappe.utils.escape_html(row.scope || "")}</td>
          <td>${conn}</td>
          <td>${frappe.utils.escape_html(row.load_strategy || "")}</td>
          <td>${enabled}</td>
          <td class="text-right">
            <button type="button" class="btn btn-xs btn-default btn-edit-table-config">${__("Edit")}</button>
          </td>
        </tr>
      `);
    });

    $tbody.find(".edit-table-config, .btn-edit-table-config").on("click", function (e) {
      e.preventDefault();
      const name = $(this).closest("tr").attr("data-name");
      if (name) {
        frappe.set_route("Form", "AM Table Config", name);
      }
    });

    $mount.empty().append($toolbar).append($table);
  }

  function schedule_table_configs(frm) {
    setTimeout(() => render_table_configs(frm), 0);
    setTimeout(() => render_table_configs(frm), 400);
  }

  frappe.ui.form.on("AM Airflow DAG", {
    onload(frm) {
      schedule_table_configs(frm);
    },
    refresh(frm) {
      schedule_table_configs(frm);
    },
  });

  $(document).on("am_table_config_updated", (_e, dag_id) => {
    const cur_frm = frappe.ui.form.get_open_form();
    if (cur_frm && cur_frm.doctype === "AM Airflow DAG" && cur_frm.doc.dag_id === dag_id) {
      render_table_configs(cur_frm);
    }
  });

  frappe.ui.form.on("AM Table Config", {
    after_save(frm) {
      $(document).trigger("am_table_config_updated", [frm.doc.dag_id]);
    },
    on_trash(frm) {
      $(document).trigger("am_table_config_updated", [frm.doc.dag_id]);
    },
  });
})();
