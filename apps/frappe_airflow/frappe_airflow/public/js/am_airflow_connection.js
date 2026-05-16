const CONN_TYPES_BY_PLATFORM = {
  wb: ["wb"],
  oz: ["oz_seller", "oz_perf"],
  ms: ["ms"],
  ym: ["ym"],
  amo: ["amo"],
  bitrix: ["bitrix"],
  iiko: ["iiko"],
};

function update_conn_type_options(frm) {
  const platform = frm.doc.platform;
  const allowed = CONN_TYPES_BY_PLATFORM[platform] || ["ym"];
  const options = allowed.join("\n");
  frm.set_df_property("conn_type", "options", options);
  if (frm.doc.conn_type && !allowed.includes(frm.doc.conn_type)) {
    frm.set_value("conn_type", allowed[0]);
  }
  frm.refresh_field("conn_type");
}

function preview_conn_id(frm) {
  if (!frm.doc.slug) {
    return;
  }
  frappe.call({
    method: "frappe_airflow.api.preview_conn_id",
    args: {
      platform: frm.doc.platform,
      conn_type: frm.doc.conn_type,
      slug: frm.doc.slug,
    },
    callback(r) {
      if (r.message) {
        frm.set_value("conn_id", r.message);
      }
    },
  });
}

frappe.ui.form.on("AM Airflow Connection", {
  refresh(frm) {
    update_conn_type_options(frm);
    if (!frm.is_new()) {
      frm.set_df_property("conn_id", "read_only", 1);
    }
  },
  platform(frm) {
    update_conn_type_options(frm);
    preview_conn_id(frm);
  },
  conn_type(frm) {
    preview_conn_id(frm);
  },
  slug(frm) {
    preview_conn_id(frm);
  },
});
