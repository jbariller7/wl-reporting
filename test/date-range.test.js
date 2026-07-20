import test from "node:test";
import assert from "node:assert/strict";
import { isTimestampInRange, parseRange, parseUtcTimestamp } from "../lib/util.js";
import { coalesceVerticalUpdates, normalizeSheetDateValue } from "../lib/db.js";

test("date-only manual ranges include both complete boundary days", () => {
  const range = parseRange({ since: "2026-07-01", until: "2026-07-05" });
  assert.equal(range.sinceUtc, "2026-07-01T00:00:00.000Z");
  assert.equal(range.untilUtc, "2026-07-05T23:59:59.999Z");
});

test("MailerLite UTC timestamps with a space include the selected first day", () => {
  const since = "2026-07-01T00:00:00.000Z";
  const until = "2026-07-05T23:59:59.999Z";

  assert.equal(parseUtcTimestamp("2026-07-01 00:00:00"), Date.parse(since));
  assert.equal(isTimestampInRange("2026-07-01 14:30:00", since, until), true);
  assert.equal(isTimestampInRange("2026-07-05 23:59:59", since, until), true);
  assert.equal(isTimestampInRange("2026-06-30 23:59:59", since, until), false);
});

test("reversed manual ranges are normalized before reaching an API", () => {
  const range = parseRange({ since: "2026-07-20", until: "2026-06-21" });
  assert.equal(range.sinceUtc, "2026-06-21T00:00:00.000Z");
  assert.equal(range.untilUtc, "2026-07-20T23:59:59.999Z");
  assert.equal(range.rangeWasReversed, true);
});

test("Google Play sheet dates support the native day-first format", () => {
  assert.equal(normalizeSheetDateValue("05/07/2026"), "2026-07-05");
  assert.equal(normalizeSheetDateValue("2026-07-19"), "2026-07-19");
});

test("adjacent sheet updates with the same columns are vertically coalesced", () => {
  assert.deepEqual(coalesceVerticalUpdates([
    { rowNumber: 11, startColumn: 15, endColumn: 15, values: ["raw-2"] },
    { rowNumber: 10, startColumn: 15, endColumn: 15, values: ["raw-1"] },
    { rowNumber: 13, startColumn: 15, endColumn: 15, values: ["raw-4"] },
    { rowNumber: 10, startColumn: 0, endColumn: 1, values: ["2026-07-01", "879"] }
  ]), [
    { startColumn: 0, endColumn: 1, startRow: 10, endRow: 10, values: [["2026-07-01", "879"]] },
    { startColumn: 15, endColumn: 15, startRow: 10, endRow: 11, values: [["raw-1"], ["raw-2"]] },
    { startColumn: 15, endColumn: 15, startRow: 13, endRow: 13, values: [["raw-4"]] }
  ]);
});

