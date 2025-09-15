import dayjs from "dayjs";
import tz from "dayjs-plugin-timezone";
import utc from "dayjs-plugin-utc";
import { q, upsert } from "./db.js";

// Extend dayjs with timezone and UTC plugins
dayjs.extend(utc);
dayjs.extend(tz);

// Constant timezone for display (Paris)
const TZ = "Europe/Paris";

/**
 * Parse a since/until range from an object with optional ISO strings.
 * Defaults to the last 30 days ending now.
 *
 * @param {Object} query Object with optional since/until ISO strings
 */
export function parseRange(query = {}) {
  const { since, until } = query;
  const end = until ? dayjs.utc(until) : dayjs.utc();
  const start = since ? dayjs.utc(since) : end.subtract(30, "day");
  return {
    sinceUtc: start.toISOString(),
    untilUtc: end.toISOString(),
    display: {
      sinceLocal: start.tz(TZ).format(),
      untilLocal: end.tz(TZ).format()
    }
  };
}

/**
 * Helper to create a successful JSON response.
 * @param {any} body Body data
 */
export function ok(body) {
  return {
    statusCode: 200,
    headers: {
      "content-type": "application/json",
      "access-control-allow-origin": "*"
    },
    body: JSON.stringify(body)
  };
}

/**
 * Helper to create an error JSON response.
 * @param {string} msg Error message
 * @param {number} code HTTP status code
 */
export function bad(msg, code = 400) {
  return {
    statusCode: code,
    headers: {
      "content-type": "application/json",
      "access-control-allow-origin": "*"
    },
    body: JSON.stringify({ error: msg })
  };
}

// Cursor helpers for incremental ETL

/**
 * Get the current cursor (ISO string) for a given source. Returns null if none.
 * @param {string} source Source key
 */
export async function getCursor(source) {
  const res = await q("SELECT since FROM etl_cursors WHERE source=$1", [source]);
  return res.rowCount ? res.rows[0].since : null;
}

/**
 * Persist a cursor (ISO string) for a given source.
 * @param {string} source Source key
 * @param {string} sinceIso ISO timestamp
 */
export async function setCursor(source, sinceIso) {
  await upsert(
    "etl_cursors",
    ["source"],
    ["source", "since"],
    [{ source, since: sinceIso }]
  );
}
