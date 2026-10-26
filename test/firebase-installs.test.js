import test from "node:test";
import assert from "node:assert/strict";
import { firstOpenHeaders, firstOpenRequest, firstOpenRows } from "../lib/firebase-installs.js";

test("Firebase API calls use the accessible WonderLang quota project rather than the credential's legacy project", () => {
  assert.equal(firstOpenHeaders("test-token")["x-goog-user-project"], "wonderlang-2209c");
  assert.equal(firstOpenHeaders("test-token", "other-project")["x-goog-user-project"], "other-project");
  assert.equal(firstOpenHeaders("test-token").Authorization, "Bearer test-token");
});

const row = (day, country, stream, count) => ({
  dimensionValues: [day, country, stream, "com.wonderlang.app"].map(value => ({ value })),
  metricValues: [{ value: count }]
});
test("daily Firebase report requests Android first_open events with country and stream", () => {
  const request = firstOpenRequest({ sinceUtc: "2026-10-24", untilUtc: "2026-10-25", streamId: "123" });
  assert.deepEqual(request.dimensions.map(value => value.name), ["date", "countryId", "streamId", "streamName"]);
  assert.deepEqual(request.dimensionFilter.andGroup.expressions.map(value => value.filter.stringFilter.value), ["first_open", "Android", "123"]);
});
test("first opens keep date, country and unknown-country counts without inventing locations", () => {
  const result = firstOpenRows([{ rows: [row("20261025", "FR", "123", "8"), row("20261025", "(not set)", "123", "2")] }], { syncedAt: "2026-10-26T08:00:00Z" });
  assert.equal(result[0].Date, "2026-10-25");
  assert.equal(result[0]["First Opens"], 8);
  assert.equal(result[0].Country, "FR");
  assert.equal(result[1].Country, "ZZ");
  assert.equal(result[1]["First Opens"], 2);
});
test("multiple Android streams require explicit selection; malformed counts fail", () => {
  assert.throws(() => firstOpenRows([{ rows: [row("20261025", "FR", "123", "8"), row("20261025", "US", "456", "2")] }], {}), /Multiple Android streams/);
  assert.throws(() => firstOpenRows([{ rows: [row("20261025", "FR", "123", "N\/A")] }], {}), /Invalid Firebase/);
});
