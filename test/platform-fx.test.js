import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';
import '../public/steam-fx.js';
import '../public/google-play.js';

const fx = globalThis.WLSteamFx;
const observations = JSON.parse(fs.readFileSync(new URL('../public/steam-fx-snapshot.json', import.meta.url), 'utf8')).observations;
const rates = fx.buildPlatformRates(observations, '2026-11-02');
test('PayPal uses the sale month while Google Play retains a one-month offset', () => {
  assert.equal(rates.paypalMonths['2026-09'].rateDate, '2026-09-29');
  assert.equal(rates.months['2026-09'].rateDate, '2026-10-29');
  assert.equal(fx.convertCurrency(100, 'USD', '2026-09-15', rates, 'paypal'), 100 * 0.995 / 1.1355);
  assert.ok(Math.abs(fx.convertCurrency(100, 'USD', '2026-09-15', rates, 'google_play') - 100 * 0.995 / 1.113) < 1e-10);
  assert.equal(rates.paypalMonths['2026-02'].rateDate, '2026-02-27');
});
test('current PayPal month uses latest available rate even after the 29th', () => {
  const r = fx.buildPlatformRates(observations, '2026-10-30');
  assert.equal(r.paypalMonths['2026-10'].rateDate, '2026-10-30');
  assert.equal(r.paypalMonths['2026-10'].provisional, true);
  assert.equal(rates.paypalMonths['2026-10'].rateDate, '2026-10-29');
  assert.equal(rates.paypalMonths['2026-10'].provisional, false);
});
test('non-USD currencies use their own monthly rate, EUR has no FX fee, refunds keep their sign', () => {
  assert.equal(fx.convertCurrency(100, 'EUR', '2026-10-01', rates, 'google_play'), 100);
  assert.ok(Math.abs(fx.convertCurrency(100, 'GBP', '2026-10-01', rates, 'google_play') - 100 / 0.8338 * 0.995) < 1e-10);
  const twd = fx.convertCurrency(100, 'TWD', '2026-10-01', rates, 'google_play');
  assert.ok(Math.abs(twd - 100 / (1.1172 * 32.478) * 0.995) < 1e-10);
  assert.equal(fx.convertCurrency(-100, 'TWD', '2026-10-01', rates, 'google_play'), -twd);
  assert.throws(() => fx.convertCurrency(100, 'XYZ', '2026-10-01', rates, 'paypal'), /Missing monthly XYZ/);
});
test('TWD cross-rate respects independent source holidays and does not use future quotes', () => {
  const ecb = fx.parseEcbRates(`<Cube time="2026-10-29"><Cube currency="USD" rate="1.1"/><Cube currency="GBP" rate="0.8"/></Cube>`);
  const cbc = fx.parseCbcTwd([{ '日期': '20261028', NTD_USD: '32' }, { '日期': '20261030', NTD_USD: '34' }]);
  const combined = fx.addTwdRates(ecb, cbc)[0];
  assert.equal(combined.currencyPerEur.TWD, 1.1 * 32);
  assert.equal(combined.currencyRateDates.TWD, '2026-10-28');
  assert.equal(combined.currencyPerEur.GBP, 0.8);
});
test('dashboard PayPal and Google Play apply platform fee, FX fee and URSSAF once, including refunds', () => {
  const html = fs.readFileSync(new URL('../public/index.html', import.meta.url), 'utf8');
  const helpers = html.slice(html.indexOf('        const FX_FALLBACK_WARNINGS'), html.indexOf('        async function fetchAllSheets('));
  const normalize = html.slice(html.indexOf('        function normalizeDate('), html.indexOf('        // --- Region Management ---'));
  const google = html.slice(html.indexOf('        function forEachGooglePlayRevenue('), html.indexOf('        const regionNames'));
  const ctx = vm.createContext({ WLSteamFx: fx, WLGooglePlay: globalThis.WLGooglePlay, STEAM_FX_RATES: rates, STEAM_CUT: 0.3, TAX_RATE: 0.125, toEur: () => { throw new Error('Unexpected fixed-rate fallback'); } });
  vm.runInContext(normalize + helpers + google, ctx);
  const paypal = vm.runInContext("paypalNetEur({date: '2026-09-15', amount: 100, currency: 'USD'})", ctx);
  assert.ok(Math.abs(paypal - 100 / 1.1355 * 0.995 * 0.875) < 1e-10);
  const rows = [
    { 'Order Number': 'one', 'Order Charged Date': '2026-09-15', 'Financial Status': 'Charged', 'Item Price': 100, 'Charged Amount': 120, 'Currency of Sale': 'USD' },
    { 'Order Number': 'two', 'Order Charged Date': '2026-09-15', 'Financial Status': 'Refund', 'Item Price': 100, 'Charged Amount': 120, 'Currency of Sale': 'USD' },
    { 'Order Number': 'three', 'Order Charged Date': '2026-09-15', 'Financial Status': 'Charged', 'Item Price': 100, 'Currency of Sale': 'EUR' }
  ];
  ctx.raw = { googlePlay: [...rows, rows[0]] };
  const values = [];
  ctx.capture = (date, net) => values.push(net);
  vm.runInContext('forEachGooglePlayRevenue(raw, capture)', ctx);
  assert.equal(values.length, 3);
  assert.ok(Math.abs(values[0] - 100 * 0.85 / 1.113 * 0.995 * 0.875) < 1e-10);
  assert.equal(values[1], -values[0]);
  assert.equal(values[2], 100 * 0.85 * 0.875);
  assert.equal((html.match(/paypalNetEur\((?:r|row)\)/g) || []).length, 9);
});
