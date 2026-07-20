(function attachDataSync(global) {
  "use strict";

  const DATE_RE = /^(\d{4})-(\d{2})-(\d{2})$/;

  function parseDate(value) {
    const match = String(value || "").match(DATE_RE);
    if (!match) return null;
    const date = new Date(Date.UTC(Number(match[1]), Number(match[2]) - 1, Number(match[3])));
    return formatDate(date) === value ? date : null;
  }

  function formatDate(date) {
    return date.toISOString().slice(0, 10);
  }

  function addDays(date, days) {
    return new Date(date.getTime() + days * 86400000);
  }

  function buildDateChunks(since, until, chunkDays = 7) {
    const start = parseDate(since);
    const end = parseDate(until);
    if (!start || !end) throw new Error("Choose a valid From and To date before syncing.");
    if (start > end) throw new Error("The From date must be before or equal to the To date.");
    if (!Number.isInteger(chunkDays) || chunkDays < 1) throw new Error("chunkDays must be a positive integer.");

    const chunks = [];
    let cursor = start;
    while (cursor <= end) {
      const candidateEnd = addDays(cursor, chunkDays - 1);
      const chunkEnd = candidateEnd < end ? candidateEnd : end;
      chunks.push({ since: formatDate(cursor), until: formatDate(chunkEnd) });
      cursor = addDays(chunkEnd, 1);
    }
    return chunks;
  }

  async function readJsonResponse(response) {
    const text = await response.text();
    let payload;
    try {
      payload = text ? JSON.parse(text) : {};
    } catch (_error) {
      const looksHtml = /^\s*</.test(text);
      const timedOut = response.status === 502 || response.status === 504 || /timed?\s*out|gateway/i.test(text);
      const detail = timedOut
        ? "The server timed out before the sync finished."
        : looksHtml
          ? "The server returned an HTML error page instead of sync data."
          : "The server returned an invalid response.";
      throw new Error(`${detail} HTTP ${response.status || "unknown"}.`);
    }

    if (!response.ok) {
      throw new Error(payload.error || payload.msg || `HTTP ${response.status} ${response.statusText || "error"}`);
    }
    return payload;
  }

  global.WLDataSync = Object.freeze({ buildDateChunks, readJsonResponse });
})(typeof globalThis !== "undefined" ? globalThis : window);
