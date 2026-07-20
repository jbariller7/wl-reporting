import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";

test("every revenue dashboard consumes Google Play data", () => {
  const html = fs.readFileSync(new URL("../public/index.html", import.meta.url), "utf8");

  assert.match(html, /fetchSheet\('Google_Play'\)/);
  assert.match(html, /triggerUpdate\('fetch-google-play'\)/);
  assert.match(html, /Google Play: €\$\{totalGooglePlayNet\.toFixed\(2\)\}/);

  const aggregationCalls = html.match(/forEachGooglePlayRevenue\(raw, \(/g) || [];
  assert.equal(aggregationCalls.length, 7, "main, funnel, ROAS, day-of-week, evolution, and both diagnosis aggregations must include Google Play");
});
