// Financial API dates are Pacific calendar dates. Never shift their labels.
export const STEAM_SALES_COLUMNS = [
  'date', 'app_id', 'country', 'currency', 'units', 'gross_revenue',
  'refunds', 'net_units', 'net_revenue', 'source', 'raw', 'ingested_at'
];

export async function fetchSteamSalesRange({ key, appIds, sinceUtc, untilUtc,
  baseUrl = 'https://partner.steam-api.com', fetchImpl = fetch,
  ingestedAt = new Date().toISOString() }) {
  const allowed = new Set(appIds.map(String));
  const warnings = [], missing = new Map(), aggregate = new Map();
  const request = async (method, params) => {
    const url = new URL(`${baseUrl.replace(/\/$/, '')}/IPartnerFinancialsService/${method}/v1`);
    url.search = new URLSearchParams({ key, ...params }).toString();
    const res = await fetchImpl(url, { signal: AbortSignal.timeout(45000) });
    if (!res.ok) throw new Error(`Steam ${method}: HTTP ${res.status}`);
    let payload;
    try { payload = await res.json(); } catch { throw new Error(`Steam ${method}: invalid JSON`); }
    if (!payload?.response || typeof payload.response !== 'object') throw new Error(`Steam ${method}: missing response`);
    return payload.response;
  };
  const changed = await request('GetChangedDatesForPartner', { highwatermark: '0' });
  if (!Array.isArray(changed.dates)) throw new Error('Steam changed-dates response is incomplete');
  const dates = [...new Set(changed.dates.map(d => String(d).replaceAll('/', '-')))]
    .filter(d => d >= sinceUtc.slice(0, 10) && d <= untilUtc.slice(0, 10)).sort();
  if (dates.some(d => !/^\d{4}-\d{2}-\d{2}$/.test(d))) throw new Error('Steam returned an invalid financial date');
  const number = (v, field) => {
    const n = Number(v ?? 0);
    if (!Number.isFinite(n)) throw new Error(`Steam returned invalid ${field}`);
    return n;
  };
  const noteApp = (id, name = '') => {
    if (!allowed.has(id) && id !== '0' && id) {
      if (!missing.has(id)) missing.set(id, { appId: id, name, units: 0, netUSD: 0 });
      if (name) missing.get(id).name = name;
    }
  };
  for (const date of dates) {
    let cursor = '0';
    for (let page = 0; ; page++) {
      if (page >= 1000) throw new Error(`Steam pagination exceeded limit for ${date}`);
      const data = await request('GetDetailedSales', { date, highwatermark_id: cursor });
      if (!/^\d+$/.test(String(data.max_id ?? ''))) throw new Error(`Steam sales response is incomplete for ${date}`);
      const next = String(data.max_id);
      // Valve omits the repeated results field on the terminal empty page.
      const records = data.results ?? data.data ?? (next === cursor ? [] : undefined);
      if (!Array.isArray(records)) throw new Error(`Steam sales response is incomplete for ${date}`);
      if (BigInt(next) < BigInt(cursor)) throw new Error(`Steam pagination moved backwards for ${date}`);
      if (next === cursor && records.length) throw new Error(`Steam returned records without advancing pagination for ${date}`);
      for (const app of data.app_info || []) noteApp(String(app.appid), String(app.app_name || ''));
      for (const r of records) {
        const id = String(r.primary_appid || r.appid || '0');
        const units = number(r.gross_units_sold, 'units'), net = number(r.net_sales_usd, 'net sales');
        if (!allowed.has(id)) {
          if (id === '0' && (units || net || Number(r.gross_units_returned))) {
            if (!missing.has(id)) missing.set(id, { appId: id, name: 'Unassigned primary app', units: 0, netUSD: 0 });
          } else noteApp(id);
          const entry = missing.get(id);
          if (entry) { entry.units += units; entry.netUSD += net; }
          continue;
        }
        const country = String(r.country_code || 'UNKNOWN').toUpperCase();
        const currency = r.currency || 'USD'; // Transaction currency; revenue fields below are always USD.
        const rowKey = [date, id, country, currency].join('|');
        if (!aggregate.has(rowKey)) aggregate.set(rowKey, {
          date, app_id: id, country, currency, units: 0, gross_revenue: 0,
          refunds: 0, net_units: 0, net_revenue: 0, source: 'api',
          raw: 'aggregated across all API pages', ingested_at: ingestedAt
        });
        const row = aggregate.get(rowKey);
        row.units += units;
        row.refunds += Math.abs(number(r.gross_units_returned, 'refunds'));
        row.net_units += number(r.net_units_sold, 'net units');
        row.gross_revenue += Math.round(number(r.gross_sales_usd, 'gross sales') * 10000);
        row.net_revenue += Math.round(net * 10000);
      }
      if (next === cursor) break;
      cursor = next;
    }
  }
  const rows = [...aggregate.values()].map(r => ({ ...r, gross_revenue: r.gross_revenue / 10000, net_revenue: r.net_revenue / 10000 }));
  for (const app of missing.values()) warnings.push(
    `Steam app ${app.name || app.appId} (${app.appId}) is missing from STEAM_APP_ID: ${app.units} gross units and $${app.netUSD.toFixed(4)} net sales are excluded. Add the app and backfill sales AND wishlists. Known apps have still been synced.`
  );
  return { rows, dates, warnings, missingApps: [...missing.values()], complete: missing.size === 0 };
}
