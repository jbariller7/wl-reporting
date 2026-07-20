import test from "node:test";
import assert from "node:assert/strict";
import { isTimestampInRange, parseRange, parseUtcTimestamp } from "../lib/util.js";
import { normalizeSheetDateValue } from "../lib/db.js";

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

test("Google Play sheet dates support the native day-first format", () => {
  assert.equal(normalizeSheetDateValue("05/07/2026"), "2026-07-05");
  assert.equal(normalizeSheetDateValue("2026-07-19"), "2026-07-19");
});

