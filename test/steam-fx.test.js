import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';
import '../public/steam-fx.js';

const fx = globalThis.WLSteamFx;
const observations = JSON.parse(fs.readFileSync(new URL('../public/steam-fx-snapshot.json', import.meta.url), 'utf8')).observations;
const rates = fx.buildMonthlyRates(observations, '2026-11-02');
test('matches the payment month, February month-end and preceding business days', () => {
  assert.equal(rates.months['2026-01'].paymentDate, '2026-02-28');
  assert.equal(rates.months['2026-01'].rateDate, '2026-02-27');
  assert.equal(rates.months['2026-02'].rateDate, '2026-03-27');
  assert.equal(rates.months['2026-07'].rateDate, '2026-08-28');
  assert.equal(rates.months['2026-09'].rateDate, '2026-10-29');
  assert.equal(rates.months['2026-12'].paymentDate, '2027-01-29');
});
test('inverts ECB USD-per-EUR and applies the 0.4 percent fee exactly once', () => {
  const expected = 100 / 1.113 * 0.996;
  assert.ok(Math.abs(fx.toEur(100, '2026-09-10', rates) - expected) < 1e-10);
  assert.ok(Math.abs(fx.toEur(-100, '2026-09-10', rates) + expected) < 1e-10);
  assert.equal(fx.toEur(100, '2025-12-10', rates), 86);
  assert.throws(() => fx.toEur(100, '2026-09-10', {}), /Missing Steam exchange rate/);
});
test('unpaid months are provisional and become historical after the payout reference is available', () => {
  assert.equal(rates.months['2026-09'].provisional, false);
  assert.equal(rates.months['2026-10'].provisional, true);
  assert.equal(rates.months['2026-10'].rateDate, '2026-10-30');
  assert.equal(rates.months['2026-11'].rateDate, '2026-10-30');
  assert.equal(rates.months['2026-11'].effectiveEurPerUsd, 0.996 / 1.1172);
  const later = fx.buildMonthlyRates([...observations, { date: '2026-11-27', usdPerEur: 1.2 }], '2026-11-30');
  assert.equal(later.months['2026-10'].rateDate, '2026-11-27');
  assert.equal(later.months['2026-10'].provisional, false);
  assert.equal(later.months['2026-10'].effectiveEurPerUsd, 0.996 / 1.2);
});
test('ECB XML parsing handles quote styles and rejects malformed responses', () => {
  assert.deepEqual(fx.parseEcbUsd(`<Cube><Cube time='2026-10-29'><Cube currency='USD' rate='1.1130'/></Cube><Cube time="2026-10-30"><Cube currency="USD" rate="1.1172"/></Cube></Cube>`), [
    { date: '2026-10-29', usdPerEur: 1.113 }, { date: '2026-10-30', usdPerEur: 1.1172 }
  ]);
  assert.throws(() => fx.parseEcbUsd('<html>Unavailable</html>'), /No valid ECB/);
});
test('dashboard applies fee, Steam share and URSSAF once, including typed Sheet dates', () => {
  const html = fs.readFileSync(new URL('../public/index.html', import.meta.url), 'utf8');
  const helper = html.slice(html.indexOf('        function steamNetEur('), html.indexOf('        async function fetchAllSheets('));
  const normalize = html.slice(html.indexOf('        function normalizeDate('), html.indexOf('        // --- Region Management ---'));
  const ctx = vm.createContext({ WLSteamFx: fx, STEAM_FX_RATES: rates, STEAM_CUT: 0.3, TAX_RATE: 0.125 });
  vm.runInContext(normalize + helper, ctx);
  const amount = vm.runInContext("steamNetEur({date: '2026-09-10', net_revenue: '100'})", ctx);
  assert.ok(Math.abs(amount - 100 / 1.113 * 0.996 * 0.7 * 0.875) < 1e-10);
  const serial = Date.parse('2026-09-10') / 86400000 + 25569;
  assert.equal(vm.runInContext(`steamNetEur({date: ${serial}, net_revenue: '100'})`, ctx), amount);
  assert.equal((html.match(/steamNetEur\((?:r|row)\)/g) || []).length, 9); // definition plus eight revenue views/exports
  assert.doesNotMatch(html, /(?<!\.)toEur\(parseFloat\((?:r|row)\.net_revenue/);
});
