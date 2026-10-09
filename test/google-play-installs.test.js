import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import { installReportPaths, parseGooglePlayInstallsBytes, parseGooglePlayInstallsCsv } from "../lib/google-play-installs.js";

const csv = [
  "Date,Package Name,Country,Daily User Installs,Daily Device Installs",
  "2026-10-01,app.wonderlang,FR,12,14",
  "2026-10-02,app.wonderlang,FR,3,4",
  "2026-10-02,other.app,US,100,100"
].join("\r\n");

test("country install report paths cover every selected month", () => {
  assert.deepEqual(installReportPaths("app.wonderlang", "2026-09-29", "2026-10-02"), [
    "stats/installs/installs_app.wonderlang_202609_country.csv",
    "stats/installs/installs_app.wonderlang_202610_country.csv"
  ]);
});

test("UTF-16 Play reports parse into date and country user and device counts", () => {
  const bytes = Buffer.from(`\uFEFF${csv}`, "utf16le");
  const rows = parseGooglePlayInstallsBytes(bytes, {
    packageId: "app.wonderlang", sinceUtc: "2026-10-02", untilUtc: "2026-10-04"
  });
  assert.deepEqual(rows, [{
    Date: "2026-10-02", "Package Name": "app.wonderlang", Country: "FR",
    "Daily User Installs": 3, "Daily Device Installs": 4
  }]);
});

test("malformed install counts fail instead of becoming false zeros", () => {
  assert.throws(() => parseGooglePlayInstallsCsv(csv.replace(",3,4", ",N/A,4"), {
    packageId: "app.wonderlang", sinceUtc: "2026-10-01", untilUtc: "2026-10-04"
  }), /Invalid Daily User Installs/);
});

test("install rows without a country fail instead of being stored", () => {
  assert.throws(() => parseGooglePlayInstallsCsv(csv.replace(",FR,3,4", ",,3,4"), {
    packageId: "app.wonderlang", sinceUtc: "2026-10-01", untilUtc: "2026-10-04"
  }), /Missing Country.*2026-10-02/);
});

test("installs flow reaches the Sheet, daily sync, dashboard API and UI", () => {
  const read = path => fs.readFileSync(new URL(path, import.meta.url), "utf8");
  assert.match(read("../lib/etl.js"), /upsert\(\s*"google_play_installs"/);
  assert.match(read("../lib/etl.js"), /\["Date", "Package Name", "Country"\],[\s\S]*?userEnteredColumns: \["Daily User Installs", "Daily Device Installs"\]/);
  assert.match(read("../netlify/functions/cron-play-installs.mjs"), /etlGooglePlayInstalls\(\{/);
  assert.match(read("../netlify/functions/dashboard-data.js"), /readSheetColumns\("google_play_installs"/);
  const html = read("../public/index.html");
  assert.match(html, /id="kpi-installs"/);
  assert.match(html, /addMetric\(r.Date, 'installs'/);
  assert.match(html, /fetch-google-play-installs/);
});
