import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import {
  findSubscriberRepairPartitions,
  groupContiguousRowNumbers,
  isDashboardEligibleSubscriber,
  latestRowDate,
  mergeDashboardSubscribers,
  subscriberPartitionKey
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

test("internal MailerLite date-country gaps are repaired even when newer derived dates exist", () => {
  const derived = [
    { subscriber_id: "1", created_at: "2026-07-31 10:00:00", country: "US" },
    { subscriber_id: "5", created_at: "2026-08-04 10:00:00", country: "US" }
  ];
  const rawDates = [
    { __rowNumber: 10, created_at: "2026-08-01 10:00:00", country: "DE" },
    { __rowNumber: 11, created_at: "2026-08-01 11:00:00", country: "DE" },
    { __rowNumber: 12, created_at: "2026-08-02 10:00:00", country: "DE" },
    { __rowNumber: 20, created_at: "2026-08-03 10:00:00", country: "DE" },
    { __rowNumber: 21, created_at: "2026-08-04 10:00:00", country: "US" }
  ];

  assert.deepEqual(findSubscriberRepairPartitions(derived, rawDates), [
    "2026-08-01|DE",
    "2026-08-02|DE",
    "2026-08-03|DE"
  ]);
  assert.equal(subscriberPartitionKey(rawDates[0]), "2026-08-01|DE");
  assert.deepEqual(groupContiguousRowNumbers([10, 11, 12, 20]), [
    { startRow: 10, endRow: 12 },
    { startRow: 20, endRow: 20 }
  ]);
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
