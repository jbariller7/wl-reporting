import test from 'node:test';
import assert from 'node:assert/strict';
import { fetchSteamSalesRange } from '../lib/steam-sales.js';

const record = (app = 4205110, extra = {}) => ({ primary_appid: app, country_code: 'FR', currency: 'EUR', gross_units_sold: 1, gross_units_returned: 0, net_units_sold: 1, gross_sales_usd: '24.9900', net_sales_usd: '20.8250', ...extra });
function run(pages, appIds = ['4205110', '4786520', '5185460']) {
  let i = 0;
  return fetchSteamSalesRange({ key: 'test', appIds, sinceUtc: '2026-10-16', untilUtc: '2026-11-01', fetchImpl: async url => {
    if (url.pathname.includes('GetChangedDates')) return { ok: true, json: async () => ({ response: { dates: ['2026/10/30'] } }) };
    const page = pages[i++];
    assert.ok(page, 'unexpected extra page');
    if (page.status) return { ok: false, status: page.status };
    return { ok: true, json: async () => ({ response: page }) };
  }});
}
test('combines records across pages and preserves English, Arabic and Russian', async () => {
  const result = await run([
    { max_id: '10', results: [record(), record(4786520)] },
    { max_id: '20', results: [record(), record(5185460), record(4205110, { gross_units_sold: 0, gross_units_returned: -1, net_units_sold: -1, gross_sales_usd: '0', net_sales_usd: '-20.8250' })] },
    { max_id: '20', results: [] }
  ]);
  assert.equal(result.complete, true);
  assert.equal(result.rows.length, 3);
  assert.deepEqual(result.rows.map(r => r.app_id), ['4205110', '4786520', '5185460']);
  assert.equal(result.rows[0].units, 2);
  assert.equal(result.rows[0].refunds, 1);
  assert.equal(result.rows[0].net_units, 1);
  assert.equal(result.rows[0].net_revenue, 20.825);
  assert.equal(result.rows[0].gross_revenue, 49.98);
});
test('unknown apps produce explicit warnings while keeping known-app data', async () => {
  const result = await run([
    { max_id: '10', app_info: [{ appid: 999, app_name: 'Future release' }], results: [record(), record(999)] },
    { max_id: '10', results: [] }
  ]);
  assert.equal(result.complete, false);
  assert.equal(result.rows.length, 1);
  assert.equal(result.missingApps[0].appId, '999');
  assert.match(result.warnings[0], /Future release.*missing from STEAM_APP_ID.*sales AND wishlists/);
});
test('empty advancing pages and 64-bit cursors do not truncate sales', async () => {
  const result = await run([
    { max_id: '9007199254740993', results: [] },
    { max_id: '9007199254740994', results: [record()] },
    { max_id: '9007199254740994', results: [] }
  ]);
  assert.equal(result.rows[0].units, 1);
});
test('incomplete or failed later pages reject the entire fetched snapshot', async () => {
  await assert.rejects(run([{ max_id: '10', results: [record()] }, { status: 503 }]), /HTTP 503/);
  await assert.rejects(run([{ max_id: '10' }]), /incomplete/);
  await assert.rejects(run([{ max_id: '0', results: [record()] }]), /without advancing/);
});
test('missing country is retained rather than failing sync', async () => {
  const result = await run([{ max_id: '10', results: [record(4205110, { country_code: '' })] }, { max_id: '10', results: [] }]);
  assert.equal(result.rows[0].country, 'UNKNOWN');
});
