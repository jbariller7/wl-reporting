import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';

const html = fs.readFileSync(new URL('../public/index.html', import.meta.url), 'utf8');
const helpers = html.slice(html.indexOf('        function normalizeDate('), html.indexOf('        // --- Region Management ---'));
const evolution = html.slice(html.indexOf('        let evoSpendChart = null;'), html.indexOf('        function sortDowTable('));

function explorer(timeframe = 'daily') {
  const charts = new Map();
  const elements = new Map();
  const element = id => {
    if (!elements.has(id)) elements.set(id, { id, value: '', checked: false, getContext() { return id; } });
    return elements.get(id);
  };
  element('evo-timeframe').value = timeframe;
  const context = vm.createContext({
    document: { getElementById: element, querySelectorAll: () => [{ checked: true, value: 'firstOpens' }] },
    Chart: function (id, config) { charts.set(id, config); this.destroy = () => {}; },
    globalDashboardState: {
      startDate: '2026-09-01', endDate: '2026-10-23', filterSelection: ['REGION:Europe'],
      excludeAdsets: [], excludedRanges: []
    },
    REGIONS: { FR: ['Europe'], DE: ['Europe'] },
    isDateExcluded: (date, excluded) => excluded.includes(date), forEachGooglePlayRevenue() {},
    toEur: value => value, STEAM_CUT: 0.3, TAX_RATE: 0.125,
    raw: {
      meta: [], mailerlite: [], steamWishlist: [], steam: [], stripe: [], paypal: [], telemetry: [],
      firebase: [
        { Date: '2026-08-31', Country: 'FR', 'First Opens': 999 },
        { Date: '2026-09-01', Country: 'FR', 'First Opens': 3 },
        { Date: '2026-09-01', Country: 'DE', 'First Opens': 1 },
        { Date: '2026-09-01', Country: 'US', 'First Opens': 100 },
        { Date: '2026-09-02', Country: 'FR', 'First Opens': 2 },
        { Date: '2026-09-06', Country: 'US', 'First Opens': 50 },
        { Date: '2026-10-23', Country: 'FR', 'First Opens': 8 },
        { Date: '2026-10-24', Country: 'FR', 'First Opens': 999 }
      ],
      // A separate Play count must not inflate the Firebase series.
      installs: [{ Date: '2026-09-01', Country: 'FR', 'Daily User Installs': 200 }]
    }
  });
  vm.runInContext(helpers + evolution, context);
  const run = () => {
    vm.runInContext('loadEvolutionData(raw)', context);
    return charts.get('evoCustomChart');
  };
  return { context, run };
}

test('Explorer plots daily Android first opens, respects filters and shows raw count tooltips', () => {
  assert.match(html, /value="firstOpens"[^>]*checked[^>]*> Android First Opens/);
  const { context, run } = explorer();
  context.globalDashboardState.excludedRanges = ['2026-09-02'];
  const chart = run();
  assert.deepEqual(Array.from(chart.data.labels), ['2026-09-01', '2026-09-06', '2026-10-23']);
  assert.equal(chart.data.datasets[0].label, 'Android First Opens');
  assert.deepEqual(Array.from(chart.data.datasets[0].data), [50, 0, 100]);
  assert.equal(chart.options.plugins.tooltip.callbacks.label({
    datasetIndex: 0, dataIndex: 0, dataset: chart.data.datasets[0], parsed: { y: 50 }
  }), 'Android First Opens: 4 (Index: 50%)');
  assert.deepEqual(Array.from(vm.runInContext('customChartDataCache.totalLeads', context)), [0, 0, 0]);

  context.globalDashboardState.filterSelection = ['FR'];
  assert.deepEqual(Array.from(run().data.datasets[0].data), [37.5, 0, 100]);
});

test('Explorer sums first opens by week, including Firebase-only dates and empty legacy feeds', () => {
  const { context, run } = explorer('weekly');
  const chart = run();
  assert.deepEqual(Array.from(chart.data.labels), ['Wk 2026-08-31 to 2026-09-06', 'Wk 2026-10-19 to 2026-10-25']);
  assert.deepEqual(Array.from(chart.data.datasets[0].data), [75, 100]);
  assert.deepEqual(Array.from(vm.runInContext('customChartDataCache.firstOpens', context)), [6, 8]);
  context.globalDashboardState.filterSelection = ['ALL'];
  run();
  assert.deepEqual(Array.from(vm.runInContext('customChartDataCache.firstOpens', context)), [156, 8]);
  delete context.raw.firebase;
  assert.deepEqual(Array.from(run().data.datasets[0].data), []);
});
