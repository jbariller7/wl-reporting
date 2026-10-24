import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import vm from "node:vm";
import { normalizeSheetDateValue } from "../lib/db.js";
import "../public/google-play.js";

const { installCoverage, formatInstalls } = globalThis.WLGooglePlay;
const rows = [
  { Date: "2026-10-12", Country: "FR", "Daily User Installs": 50 },
  { Date: "2026-10-13", Country: "FR", "Daily User Installs": 2 },
  { Date: "2026-10-18", Country: "FR", "Daily User Installs": 3 },
  { Date: "2026-10-19", Country: "GB", "Daily User Installs": 4 },
  { Date: "2026-10-19", Country: "US", "Daily User Installs": 10 }
];
const options = { startDate: "2026-10-21", endDate: "2026-10-23", normalizeDate: normalizeSheetDateValue };

test("unreported recent dates display Pending and retain the latest reported week's count", () => {
  const coverage = installCoverage(rows, options);
  assert.deepEqual(coverage, { status: "pending", latestDate: "2026-10-19", weekStart: "2026-10-13", weekInstalls: 19 });
  assert.equal(formatInstalls(0, coverage), "Pending");
});

test("reported zero stays zero; partial totals are not replaced by a different date window", () => {
  assert.equal(formatInstalls(0, installCoverage(rows, { ...options, startDate: "2026-10-13", endDate: "2026-10-19" })), "0");
  const coverage = installCoverage(rows, { ...options, startDate: "2026-10-18" });
  assert.equal(coverage.status, "partial");
  assert.equal(formatInstalls(17, coverage), "17");
});

test("the latest reported week respects selected countries and excluded dates", () => {
  const coverage = installCoverage(rows, {
    ...options, includeCountry: country => country === "FR", isExcluded: date => date === "2026-10-13"
  });
  assert.equal(coverage.weekInstalls, 3);
});

test("empty reports are unavailable and numeric Sheets dates still identify coverage", () => {
  assert.equal(formatInstalls(0, installCoverage([], options)), "N/A");
  const serial = Date.parse("2026-10-19T00:00:00Z") / 86400000 + 25569;
  const coverage = installCoverage([{ Date: serial, Country: "UK", "Daily User Installs": 4 }], {
    ...options, includeCountry: country => country === "GB"
  });
  assert.equal(coverage.latestDate, "2026-10-19");
  assert.equal(coverage.weekInstalls, 4);
});

test("the real dashboard renders pending installs, country rows and a working reported-week shortcut", () => {
  const html = fs.readFileSync(new URL("../public/index.html", import.meta.url), "utf8");
  const elements = new Map();
  const element = id => {
    if (!elements.has(id)) elements.set(id, {
      innerText: "", innerHTML: "", value: "", classList: { replace() {}, toggle() {} }, getContext() { return {}; }
    });
    return elements.get(id);
  };
  const state = { ...options, filterSelection: ["REGION:Europe"], excludeAdsets: [], excludedRanges: [] };
  let refreshes = 0;
  const context = vm.createContext({
    document: { getElementById: element }, window: {}, WLGooglePlay: globalThis.WLGooglePlay,
    globalDashboardState: state, normalizeDate: normalizeSheetDateValue, REGIONS: { FR: ["Europe"], GB: ["Europe"] },
    isDateExcluded: () => false, forEachGooglePlayRevenue() {}, getCountryName: country => country,
    getRankColor: () => "transparent", toEur: value => value, TAX_RATE: 0.125, STEAM_CUT: 0.3,
    dashCountryData: [], dashRegionData: [], dashSort: { country: { col: "spend", desc: true }, region: { col: "spend", desc: true } },
    myChart: null, Chart: function () { this.destroy = () => {}; }, refreshAllViews: () => { refreshes += 1; }
  });
  const source = html.slice(html.indexOf("        function loadDashboardData(raw)"), html.indexOf("        // --- FUNNEL HEALTH & OPTIMIZER TAB LOGIC ---"));
  vm.runInContext(source, context);
  context.raw = {
    meta: [{ date: "2026-10-22", country: "FR", spend: 5, impressions: 10, clicks: 1 }],
    mailerlite: [], steamWishlist: [], installs: rows, steam: [], stripe: [], paypal: [], telemetry: []
  };
  vm.runInContext("loadDashboardData(raw)", context);
  assert.equal(element("kpi-installs").innerText, "Pending");
  assert.match(element("kpi-installs-through").innerText, /not reported yet.*2026-10-19/);
  assert.match(element("kpi-installs-latest").innerText, /^9 installs.*2026-10-13 to 2026-10-19/);
  assert.match(element("dash-country-body").innerHTML, />Pending<\/td>/);
  assert.match(element("dash-region-body").innerHTML, />Pending<\/td>/);
  vm.runInContext("showLatestInstallWeek()", context);
  assert.equal(element("dash-start").value, "2026-10-13");
  assert.equal(element("dash-end").value, "2026-10-19");
  assert.equal(refreshes, 1);
  state.startDate = "2026-10-13";
  state.endDate = "2026-10-19";
  vm.runInContext("loadDashboardData(raw)", context);
  assert.equal(element("kpi-installs").innerText, "9");
  assert.equal(state.totalInstalls, 9);
});
