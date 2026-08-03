const WISHLIST_FIELDS = [
  "wishlist_adds",
  "wishlist_deletes",
  "wishlist_purchases",
  "wishlist_gifts",
  "wishlist_adds_windows",
  "wishlist_adds_mac",
  "wishlist_adds_linux"
];

function normalizeDate(value) {
  return String(value || "").trim().replace(/\//g, "-").slice(0, 10);
}

function numericSummary(summary = {}) {
  return Object.fromEntries(WISHLIST_FIELDS.map(field => [field, Number(summary[field] || 0)]));
}

export function wishlistDatesInRange(sinceUtc, untilUtc) {
  const start = new Date(`${String(sinceUtc).slice(0, 10)}T00:00:00.000Z`);
  const end = new Date(`${String(untilUtc).slice(0, 10)}T00:00:00.000Z`);
  if (!Number.isFinite(start.getTime()) || !Number.isFinite(end.getTime())) return [];

  const dates = [];
  for (const cursor = new Date(start); cursor <= end; cursor.setUTCDate(cursor.getUTCDate() + 1)) {
    dates.push(cursor.toISOString().slice(0, 10));
  }
  return dates;
}

export function wishlistRowsFromResponse(payload, requestedAppId, requestedDate, ingestedAt = new Date().toISOString()) {
  const response = payload?.response || payload || {};
  const date = normalizeDate(response.date || requestedDate);
  const appId = String(response.appid || requestedAppId || "");
  const countries = Array.isArray(response.country_summary) ? response.country_summary : [];

  const rows = countries.map(country => ({
    date,
    app_id: appId,
    country: String(country.country_code || "UNKNOWN").trim().toUpperCase(),
    country_name: String(country.country_name || ""),
    region: String(country.region || ""),
    ...numericSummary(country.summary_actions),
    source: "api",
    raw: country,
    ingested_at: ingestedAt
  }));

  // Preserve the API's app-wide total if Valve cannot assign every action to a
  // country. This keeps the global dashboard total exact without duplicating
  // the country rows.
  if (response.wishlist_summary) {
    const total = numericSummary(response.wishlist_summary);
    const assigned = Object.fromEntries(WISHLIST_FIELDS.map(field => [
      field,
      rows.reduce((sum, row) => sum + Number(row[field] || 0), 0)
    ]));
    const remainder = Object.fromEntries(WISHLIST_FIELDS.map(field => [field, Math.max(0, total[field] - assigned[field])]));
    if (countries.length === 0 || WISHLIST_FIELDS.some(field => remainder[field] > 0)) {
      rows.push({
        date,
        app_id: appId,
        country: "UNKNOWN",
        country_name: "Unallocated",
        region: "Unallocated",
        ...remainder,
        source: "api",
        raw: { summary_actions: remainder, reconciliation: true },
        ingested_at: ingestedAt
      });
    }
  }

  return rows;
}

export const STEAM_WISHLIST_FIELDS = WISHLIST_FIELDS;
