(function () {
  if (window.__frappe_airflow_dag_connections_handlers) {
    return;
  }
  window.__frappe_airflow_dag_connections_handlers = true;

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

  function connections_mount(frm) {
    const html_field = frm.fields_dict.connections_ui;
    if (html_field && html_field.$wrapper) {
      let $mount = html_field.$wrapper.find("> .dag-connections-mount");
      if (!$mount.length) {
        $mount = $('<div class="dag-connections-mount">').appendTo(html_field.$wrapper);
      }
      return $mount;
    }

    const section = frm.fields_dict.connections_section;
    if (!section) {
      const anchor = frm.fields_dict.last_parsed_time || frm.fields_dict.schedule_interval;
      if (!anchor || !anchor.$wrapper) {
        return null;
      }
      let $fallback = $(anchor.$wrapper).closest(".form-page").find(".dag-connections-fallback");
      if (!$fallback.length) {
        $fallback = $(
          '<div class="form-section card-section dag-connections-fallback">' +
            '<motion.div class="section-head section-head collapsible">'.replace("motion.", "") +
            __("Connections") +
            '</div><div class="section-body"></div></div>'
        );
        $(anchor.$wrapper).closest(".form-layout").append($fallback);
      }
      let $mount = $fallback.find(".section-body > .dag-connections-mount");
      if (!$mount.length) {
        $mount = $('<div class="dag-connections-mount">').appendTo($fallback.find(".section-body"));
      }
      return $mount;
    }

    const $section = $(section.wrapper).closest(".form-section");
    const $body = $section.find(".section-body").first();
    const $target = $body.length ? $body : $(section.wrapper);
    let $mount = $target.find("> .dag-connections-mount");
    if (!$mount.length) {
      $mount = $('<div class="dag-connections-mount">').appendTo($target);
    }
    return $mount;
  }

  function paint_inline_checkboxes(frm, $mount, options) {
    const selected = new Set(parse_selected_connections(frm.doc.selected_connections));

    if (!options.length) {
      $mount.html(
        '<p class="text-muted" style="margin:0">' +
          __("No marketplace connections match this DAG platform.") +
          "</p>"
      );
      return;
    }

    const $grid = $('<div class="dag-conn-checkboxes">').css({
      display: "grid",
      gridTemplateColumns: "repeat(auto-fill, minmax(260px, 1fr))",
      gap: "10px",
      padding: "4px 0 8px",
    });

    options.forEach((row) => {
      const id = frappe.utils.escape_html(row.value);
      const label = frappe.utils.escape_html(row.label);
      const checked = selected.has(row.value) ? " checked" : "";
      $grid.append(`
        <label class="checkbox" style="display:flex;align-items:flex-start;gap:8px;margin:0;font-weight:normal">
          <input type="checkbox" data-conn-id="${id}"${checked}>
          <span>${label}</span>
        </label>
      `);
    });

    $mount.empty().append($grid);

    $mount.find('input[type="checkbox"]').off("change").on("change", () => {
      const conn_ids = [];
      $mount.find('input[type="checkbox"]:checked').each((_, el) => {
        conn_ids.push($(el).attr("data-conn-id"));
      });
      frm.doc.selected_connections = JSON.stringify(conn_ids);
      frm.dirty();
    });
  }

  function render_dag_inline_connections(frm) {
    if (!frm.doc.dag_id) {
      return;
    }
    const $mount = connections_mount(frm);
    if (!$mount) {
      return;
    }

    $mount.html(
      '<p class="text-muted" style="margin:0">' + __("Loading connections...") + "</p>"
    );

    const apply = (options) => {
      if (frm.doc.connection_options !== JSON.stringify(options)) {
        frm.doc.connection_options = JSON.stringify(options);
      }
      paint_inline_checkboxes(frm, $mount, options);
    };

    const cached = parse_connection_options(frm.doc.connection_options);
    if (cached.length) {
      apply(cached);
      return;
    }

    frappe.call({
      method: "frappe_airflow.api.get_dag_connection_options",
      args: { dag_id: frm.doc.dag_id },
      callback(r) {
        apply(r.message || []);
      },
      error(r) {
        const msg = (r && r.message) || __("Failed to load connections");
        $mount.html(
          '<p class="text-danger" style="margin:0">' + frappe.utils.escape_html(msg) + "</p>"
        );
      },
    });
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
      setTimeout(() => render_dag_inline_connections(frm), 0);
    },
    before_save(frm) {
      const value = frm.doc.selected_connections;
      if (Array.isArray(value)) {
        frm.doc.selected_connections = JSON.stringify(value);
      }
    },
  });
})();
