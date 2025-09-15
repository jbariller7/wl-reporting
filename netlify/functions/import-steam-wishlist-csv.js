import { ok, bad } from "../../lib/util.js";
import { upsert } from "../../lib/db.js";
import { parse } from "csv-parse/sync";

export const handler = async (event) => {
  if (event.httpMethod !== "POST") return bad("Use POST");

  const body = event.body ? JSON.parse(event.body) : {};
  const appId = body.appId || process.env.STEAM_APP_ID;
  const filename = body.filename || "upload.csv";
  const csv = body.csv;

  if (!csv || !appId) return bad("Missing csv or appId");

  // Expect columns: date, adds, deletes, purchases_from_wishlist
  const records = parse(csv, {
    columns: true,
    skip_empty_lines: true,
    trim: true
  });

  const rows = records.map((r) => ({
    date: r.date,
    app_id: String(appId),
    adds: Number(r.adds || 0),
    deletes: Number(r.deletes || 0),
    purchases_from_wishlist: Number(r.purchases_from_wishlist || 0),
    source_file: filename
  }));

  await upsert(
    "steam_wishlist_events",
    ["date","app_id"],
    ["date","app_id","adds","deletes","purchases_from_wishlist","source_file","ingested_at"],
    rows
  );

  return ok({ ok: true, rows: rows.length });
};
