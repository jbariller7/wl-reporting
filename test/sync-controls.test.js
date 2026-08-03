import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";

test("Update ALL forwards force refresh and covers every active source except TikTok", () => {
  const html = fs.readFileSync(new URL("../public/index.html", import.meta.url), "utf8");
  const refresh = fs.readFileSync(new URL("../netlify/functions/refresh.js", import.meta.url), "utf8");

  assert.match(html, /triggerUpdate\('refresh'\)/);
  assert.match(html, /forceRefresh\s*=\s*document\.getElementById\('force-refresh'\)\.checked/);
  assert.match(html, /body:\s*JSON\.stringify\([\s\S]*?forceRefresh/);
  assert.match(refresh, /range\.forceRefresh\s*=\s*body\.forceRefresh\s*===\s*true/);
  assert.match(refresh, /\["stripe","meta","mailerlite","steam","google_play","telemetry"\]/);
  assert.doesNotMatch(refresh, /body\.sources\s*\|\|\s*\[[^\]]*"tiktok"/);
  assert.match(refresh, /results\.telemetry\s*=\s*await etlTelemetry\(range\)/);
  assert.match(html, /\['fetch-meta', 'META'\]/);
  assert.match(html, /runMetaSync\(since, until, forceRefresh\)/);
  assert.match(html, /WLDataSync\.buildDateChunks\(since, until, 1\)/);
  assert.match(html, /one-day part/);
  assert.match(html, /\['insights', 'country insights'\]/);
  assert.match(html, /\['creatives', 'creative insights'\]/);
  assert.match(html, /syncPart:\s*'repair'/);
  assert.match(html, /syncPart:\s*'configuration'/);
});

test("MailerLite force refresh fetches and filters before deleting sheet rows", () => {
  const etl = fs.readFileSync(new URL("../lib/etl.js", import.meta.url), "utf8");
  const start = etl.indexOf("export async function etlMailerLite");
  const end = etl.indexOf("export async function etlSteamSalesApi");
  const mailerLite = etl.slice(start, end);

  const fetchIndex = mailerLite.indexOf("while (hasMore)");
  const deleteIndex = mailerLite.indexOf('clearDateRange("mailerlite_subscribers"');
  const upsertIndex = mailerLite.indexOf('"mailerlite_subscribers",', deleteIndex);

  assert.match(mailerLite, /isTimestampInRange\(created, sinceUtc, untilUtc\)/);
  assert.ok(fetchIndex >= 0 && deleteIndex > fetchIndex, "force refresh must not delete before the MailerLite API fetch succeeds");
  assert.ok(upsertIndex > deleteIndex, "replacement rows must be written after the selected range is cleared");
});

test("MailerLite sync maintains a lightweight dashboard-only subscriber tab", () => {
  const etl = fs.readFileSync(new URL("../lib/etl.js", import.meta.url), "utf8");
  const html = fs.readFileSync(new URL("../public/index.html", import.meta.url), "utf8");
  const db = fs.readFileSync(new URL("../lib/db.js", import.meta.url), "utf8");

  assert.match(etl, /subscribers\?limit=100&include=groups/);
  assert.match(etl, /String\(group\?\.name \|\| ""\)\.trim\(\)\.toLowerCase\(\) === "pdf content only"/);
  assert.match(etl, /!utmSource\.includes\("reddit"\)/);
  assert.match(etl, /clearDateRange\("mailerlite_dashboard_subscribers"/);
  assert.match(etl, /"mailerlite_dashboard_subscribers",[\s\S]*?\{ createSheet: true, updateExisting: true \}/);
  assert.match(db, /'mailerlite_dashboard_subscribers': 'MailerLite_Dashboard'/);
  assert.match(html, /fetchSheet\('MailerLite_Dashboard', \['B', 'C'\]\)/);
  assert.doesNotMatch(html, /fetchSheet\('MailerLite',/);
  assert.match(db, /addRows\(newRows,\s*\{\s*raw:\s*true\s*\}\)/);
});

test("hourly MailerLite sync starts at midnight before replacing dashboard dates", () => {
  const cron = fs.readFileSync(new URL("../netlify/functions/cron-hourly.mjs", import.meta.url), "utf8");

  assert.match(cron, /mailerLiteSince\.setUTCDate\(mailerLiteSince\.getUTCDate\(\) - 2\)/);
  assert.match(cron, /mailerLiteSince\.setUTCHours\(0, 0, 0, 0\)/);
  assert.match(cron, /etlMailerLite\(mailerLiteRange\)/);
  assert.doesNotMatch(cron, /etlMailerLite\(range\)/);
});
