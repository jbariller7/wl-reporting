import dayjs from "dayjs";
import utc from "dayjs/plugin/utc.js";
import timezone from "dayjs/plugin/timezone.js";

// Extend dayjs with the CORRECT native timezone and UTC plugins
dayjs.extend(utc);
dayjs.extend(timezone);

// Constant timezone for display (Paris)
const TZ = "Europe/Paris";

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
