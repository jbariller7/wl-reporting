import { normalizeSheetDateValue } from "./db.js";

function parseRaw(value) {
  if (value && typeof value === "object") return value;
  try {
    return JSON.parse(String(value || "{}"));
  } catch {
    return {};
  }
}

export function isDashboardEligibleSubscriber(row) {
  const raw = parseRaw(row?.raw);
  const utmSource = String(raw.fields?.utm_source || "").toLowerCase();
  const groups = Array.isArray(raw.groups) ? raw.groups : [];
  const isPdfOnly = groups.some(group =>
    String(group?.name || "").trim().toLowerCase() === "pdf content only"
  );
  return !isPdfOnly && !utmSource.includes("reddit");
}

export function latestRowDate(rows, dateField) {
  return (rows || []).reduce((latest, row) => {
    const date = normalizeSheetDateValue(row?.[dateField]);
    return /^\d{4}-\d{2}-\d{2}$/.test(date) && date > latest ? date : latest;
  }, "");
}

export function recentSubscriberRepairWindow(rawDateRows, latestDate, days = 21) {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(latestDate || "")) return null;
  const latestMs = Date.parse(`${latestDate}T00:00:00Z`);
  const dayCount = Math.max(1, Number(days) || 1);
  const cutoffDate = new Date(latestMs - (dayCount - 1) * 86400000).toISOString().slice(0, 10);
  const selectedRows = (rawDateRows || []).filter(row => {
    const date = normalizeSheetDateValue(row?.created_at);
    return date >= cutoffDate
      && date <= latestDate
      && Number.isFinite(Number(row?.__rowNumber));
  });
  if (selectedRows.length === 0) return null;

  const rowNumbers = selectedRows.map(row => Number(row.__rowNumber));
  return {
    cutoffDate,
    latestDate,
    startRow: Math.min(...rowNumbers),
    endRow: Math.max(...rowNumbers),
    rowNumbers
  };
}

export function mergeDashboardSubscribers(derivedRows, rawRows) {
  const merged = new Map();
  for (const row of derivedRows || []) {
    const createdAt = row.created_at || "";
    const key = String(row.subscriber_id || `${createdAt}|${row.country || ""}`);
    if (!createdAt) continue;
    merged.set(key, {
      subscriber_id: String(row.subscriber_id || ""),
      created_at: createdAt,
      country: row.country || ""
    });
  }
  for (const row of rawRows || []) {
    if (!row.created_at || !isDashboardEligibleSubscriber(row)) continue;
    const key = String(row.subscriber_id || `${row.created_at}|${row.country || ""}`);
    merged.set(key, {
      subscriber_id: String(row.subscriber_id || ""),
      created_at: row.created_at,
      country: row.country || ""
    });
  }
  return [...merged.values()];
}
