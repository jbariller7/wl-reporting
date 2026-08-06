import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import {
  isDashboardEligibleSubscriber,
  latestRowDate,
  mergeDashboardSubscribers,
  recentSubscriberRepairWindow
} from "../lib/dashboard-data.js";

test("dashboard loader avoids GViz for mixed-type purchase dates", () => {
  const html = fs.readFileSync(new URL("../public/index.html", import.meta.url), "utf8");
  assert.match(html, /fetch\('\/.netlify\/functions\/dashboard-data'/);
  assert.match(html, /fetchExportSheet\('1605998113'\)/);
  assert.match(html, /fetchExportSheet\('270734933'\)/);
  assert.doesNotMatch(html, /fetchSheet\('Steam_Sales'/);
  assert.doesNotMatch(html, /fetchSheet\('Google_Play'/);
});

test("recent raw MailerLite rows repair a stale derived subscriber tab", () => {
  const derived = [{ subscriber_id: "1", created_at: "2026-08-02 10:00:00", country: "US" }];
  const raw = [
    { subscriber_id: "2", created_at: "2026-08-04 10:00:00", country: "DE", raw: "{}" },
    { subscriber_id: "3", created_at: "2026-08-04 11:00:00", country: "US", raw: JSON.stringify({ fields: { utm_source: "reddit" } }) },
    { subscriber_id: "4", created_at: "2026-08-04 12:00:00", country: "MY", raw: JSON.stringify({ groups: [{ name: "PDF Content only" }] }) }
  ];

  const merged = mergeDashboardSubscribers(derived, raw);
  assert.deepEqual(merged, [
    { subscriber_id: "1", created_at: "2026-08-02 10:00:00", country: "US" },
    { subscriber_id: "2", created_at: "2026-08-04 10:00:00", country: "DE" }
  ]);
  assert.equal(latestRowDate(merged, "created_at"), "2026-08-04");
  assert.equal(isDashboardEligibleSubscriber(raw[1]), false);
  assert.equal(isDashboardEligibleSubscriber(raw[2]), false);
});

test("recent MailerLite repair uses one bounded row window and includes partially derived dates", () => {
  const rawDates = [
    { __rowNumber: 10, created_at: "2026-08-01 10:00:00", country: "DE" },
    { __rowNumber: 11, created_at: "2026-08-01 11:00:00", country: "DE" },
    { __rowNumber: 12, created_at: "2026-08-02 10:00:00", country: "DE" },
    { __rowNumber: 20, created_at: "2026-07-10 10:00:00", country: "US" },
    { __rowNumber: 21, created_at: "2026-08-06 10:00:00", country: "US" },
    { __rowNumber: 22, created_at: "2026-08-07 10:00:00", country: "US" },
    { __rowNumber: 23, created_at: "2026-07-17 10:00:00", country: "FR" }
  ];

  assert.deepEqual(recentSubscriberRepairWindow(rawDates, "2026-08-06"), {
    cutoffDate: "2026-07-17",
    latestDate: "2026-08-06",
    startRow: 10,
    endRow: 23,
    rowNumbers: [10, 11, 12, 21, 23]
  });

  const handler = fs.readFileSync(new URL("../netlify/functions/dashboard-data.js", import.meta.url), "utf8");
  assert.match(handler, /startRow: repairWindow\.startRow, endRow: repairWindow\.endRow/);
  assert.doesNotMatch(handler, /Promise\.all\(rowRanges/);
});

test("sync writes dashboard-critical date and amount columns as typed cells", () => {
  const etl = fs.readFileSync(new URL("../lib/etl.js", import.meta.url), "utf8");
  const db = fs.readFileSync(new URL("../lib/db.js", import.meta.url), "utf8");
  assert.match(etl, /userEnteredColumns: \["date"\]/);
  assert.match(etl, /"Order Charged Date",[\s\S]*?"Item Price",[\s\S]*?"Charged Amount"/);
  assert.match(etl, /userEnteredColumns: \["created_at"\]/);
  assert.match(db, /const addedRows = await sheet\.addRows\(newRows, \{ raw: true \}\)/);
  assert.match(db, /userEnteredCellUpdates\.push/);
});
