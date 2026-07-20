import dayjs from "dayjs";
import utc from "dayjs/plugin/utc.js";
import timezone from "dayjs/plugin/timezone.js";

// Extend dayjs with the CORRECT native timezone and UTC plugins
dayjs.extend(utc);
dayjs.extend(timezone);

// Constant timezone for display (Paris)
const TZ = "Europe/Paris";

const DATE_ONLY_RE = /^\d{4}-\d{2}-\d{2}$/;

export function parseUtcTimestamp(value) {
  if (!value) return NaN;
  let normalized = String(value).trim();
  if (/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(?:\.\d+)?$/.test(normalized)) {
    normalized = `${normalized.replace(" ", "T")}Z`;
  } else if (/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?$/.test(normalized)) {
    normalized = `${normalized}Z`;
  }
  return Date.parse(normalized);
}

export function isTimestampInRange(value, sinceUtc, untilUtc) {
  const timestamp = parseUtcTimestamp(value);
  const since = parseUtcTimestamp(sinceUtc);
  const until = parseUtcTimestamp(untilUtc);
  return Number.isFinite(timestamp) && Number.isFinite(since) && Number.isFinite(until)
    && timestamp >= since && timestamp <= until;
}

export function parseRange(query = {}) {
  const { since, until } = query;
  let end = until ? dayjs.utc(until) : dayjs.utc();
  let start = since ? dayjs.utc(since) : end.subtract(30, "day");
  if (since && DATE_ONLY_RE.test(since)) start = start.startOf("day");
  if (until && DATE_ONLY_RE.test(until)) end = end.endOf("day");
  return {
    sinceUtc: start.toISOString(),
    untilUtc: end.toISOString(),
    display: {
      sinceLocal: start.tz(TZ).format(),
      untilLocal: end.tz(TZ).format()
    }
  };
}

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

// Cursor helpers disabled for Google Sheets
export async function getCursor(source) {
  return null;
}

export async function setCursor(source, sinceIso) {
  return null;
}
