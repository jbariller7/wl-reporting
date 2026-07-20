import test from "node:test";
import assert from "node:assert/strict";

await import("../public/sync-utils.js");
const { buildDateChunks, readJsonResponse } = globalThis.WLDataSync;

test("Meta date ranges are split into inclusive seven-day chunks", () => {
  assert.deepEqual(buildDateChunks("2026-06-21", "2026-07-20", 7), [
    { since: "2026-06-21", until: "2026-06-27" },
    { since: "2026-06-28", until: "2026-07-04" },
    { since: "2026-07-05", until: "2026-07-11" },
    { since: "2026-07-12", until: "2026-07-18" },
    { since: "2026-07-19", until: "2026-07-20" }
  ]);
});

test("HTML platform errors become a useful timeout message", async () => {
  const response = new Response("<HTML><HEAD><TITLE>Gateway Timeout</TITLE></HEAD></HTML>", {
    status: 504,
    headers: { "content-type": "text/html" }
  });
  await assert.rejects(readJsonResponse(response), /server timed out.*HTTP 504/i);
});

test("valid JSON sync responses are returned unchanged", async () => {
  const response = new Response(JSON.stringify({ ok: true, rows: 12 }), {
    status: 200,
    headers: { "content-type": "application/json" }
  });
  assert.deepEqual(await readJsonResponse(response), { ok: true, rows: 12 });
});
