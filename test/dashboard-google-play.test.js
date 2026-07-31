import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";

test("every revenue dashboard consumes Google Play data", () => {
  const html = fs.readFileSync(new URL("../public/index.html", import.meta.url), "utf8");

  assert.match(html, /fetchSheet\('Google_Play', \['B', 'D', 'J', 'K', 'Q'\]\)/);
  assert.match(html, /<script src="\/google-play\.js"><\/script>/);
  assert.match(html, /const netAfterUrssaf = toEur\(sale\.netRevenue, sale\.currency\) \* \(1 - TAX_RATE\)/);
  assert.match(html, /triggerUpdate\('fetch-google-play'\)/);
  assert.match(html, /Google Play: €\$\{totalGooglePlayNet\.toFixed\(2\)\}/);

  const aggregationCalls = html.match(/forEachGooglePlayRevenue\(raw, \(/g) || [];
  assert.equal(aggregationCalls.length, 8, "main, funnel, ROAS, day-of-week, evolution, the retired optimizer helper, and both diagnosis aggregations must include Google Play");
});

test("dashboard derives revenue from the native Google Play schema", async () => {
  await import("../public/google-play.js");
  const sale = globalThis.WLGooglePlay.normalizeRow({
    "Order Charged Date": "05/07/2026",
    "Financial Status": "Charged",
    "Currency of Sale": "GBP",
    "Item Price": "6.66",
    "Country of Buyer": "GB"
  }, 0.15);

  assert.equal(sale.date, "05/07/2026");
  assert.equal(sale.currency, "GBP");
  assert.equal(sale.country, "GB");
  assert.ok(Math.abs(sale.netRevenue - 5.661) < 1e-9);
  const eurAfterUrssaf = sale.netRevenue * 1.15 * (1 - 0.125);
  assert.ok(Math.abs(eurAfterUrssaf - 5.69638125) < 1e-9);

  const refund = globalThis.WLGooglePlay.normalizeRow({
    "Order Charged Date": "19/07/2026",
    "Financial Status": "Refund",
    "Currency of Sale": "EUR",
    "Item Price": "22,31",
    "Country of Buyer": "ES"
  }, 0.15);
  assert.ok(refund.netRevenue < 0);
});
