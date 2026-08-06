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

export function subscriberPartitionKey(row) {
  const date = normalizeSheetDateValue(row?.created_at);
  if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) return "";
  return `${date}|${String(row?.country || "UNKNOWN").trim().toUpperCase() || "UNKNOWN"}`;
}

export function findSubscriberRepairPartitions(derivedRows, rawDateRows) {
  const derivedPartitions = new Set(
    (derivedRows || [])
      .map(subscriberPartitionKey)
      .filter(Boolean)
  );

  return [...new Set(
    (rawDateRows || [])
      .map(subscriberPartitionKey)
      .filter(partition => partition && !derivedPartitions.has(partition))
  )].sort();
}

export function groupContiguousRowNumbers(rowNumbers) {
  const sorted = [...new Set(
    (rowNumbers || []).map(Number).filter(Number.isFinite)
  )].sort((a, b) => a - b);
  const ranges = [];

  for (const rowNumber of sorted) {
    const previous = ranges[ranges.length - 1];
    if (previous && rowNumber === previous.endRow + 1) {
      previous.endRow = rowNumber;
    } else {
      ranges.push({ startRow: rowNumber, endRow: rowNumber });
    }
  }

  return ranges;
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
