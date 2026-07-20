import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import { shouldRetryMetaStableFields } from "../lib/meta.js";

await import("../public/meta-optimizer.js");
const { buildOptimizerReport, toMarkdown } = globalThis.WLMetaOptimizer;

function metaRow({ date, id, name, country, spend = 10, impressions = 1000, links = 20, lpv = 12 }) {
  return {
    date,
    adset_id: id,
    adset_name: name,
    country,
    spend,
    impressions,
    clicks: links * 2,
    raw: JSON.stringify({
      actions: [
        { action_type: "link_click", value: String(links) },
        { action_type: "landing_page_view", value: String(lpv) }
      ]
    })
  };
}

test("optimizer keeps country revenue separate from exact ad-set metrics", () => {
  const metaRows = [];
  const marketRows = [];
  for (let day = 13; day <= 19; day++) {
    const date = `2026-07-${day}`;
    metaRows.push(metaRow({ date, id: "a", name: "US Scale", country: "US" }));
    metaRows.push(metaRow({ date, id: "b", name: "US Test", country: "US", spend: 5, links: 8, lpv: 4 }));
    marketRows.push({ date, country: "US", netRevenue: 50, orders: 1 });
  }
  // Current-day data must be ignored because Steam normally closes a day later.
  metaRows.push(metaRow({ date: "2026-07-20", id: "a", name: "US Scale", country: "US", spend: 999 }));
  marketRows.push({ date: "2026-07-18", country: "FR", netRevenue: 100, orders: 5, subscribers: 12 });

  const report = buildOptimizerReport({
    now: new Date("2026-07-20T12:00:00Z"),
    startDate: "2026-06-01",
    endDate: "2026-07-20",
    metaRows,
    marketRows,
    adsetConfigRows: [
      { snapshot_date: "2026-07-20", adset_id: "a", account_currency: "EUR", daily_budget: "10000", bid_amount: "200" },
      { snapshot_date: "2026-07-20", adset_id: "b", account_currency: "EUR", daily_budget: "5000", bid_amount: "100" }
    ],
    options: { targetRoas: 2, targetLinkCtr: 1, minLandingPageRate: 45, minSpend: 20, maxBudgetChangePct: 15 }
  });

  assert.equal(report.reportEnd, "2026-07-19");
  const scale = report.adsets.find((row) => row.id === "a");
  assert.equal(scale.metrics7.spend, 70, "current-day spend is excluded");
  assert.equal(scale.metrics7.netRevenue, 0, "store revenue is never copied into an ad-set metric");
  assert.ok(scale.marketRoas7 > 3, "country revenue is exposed only as a shared market signal");
  assert.equal(scale.recommendation.action, "INCREASE CPA");
  assert.equal(scale.recommendation.confidence, "Medium", "overlapping US ad sets reduce confidence");

  const usMarket = report.markets.find((row) => row.country === "US");
  assert.equal(usMarket.metrics7.netRevenue, 350);
  assert.equal(usMarket.overlap, 2);
  assert.equal(report.opportunities.find((row) => row.country === "FR")?.action, "START COUNTRY TEST");

  const markdown = toMarkdown(report);
  assert.match(markdown, /country-level shared market evidence/i);
  assert.match(markdown, /INCREASE CPA/);
});

test("dashboard wires the optimizer to the new read-only Meta sheets", () => {
  const html = fs.readFileSync(new URL("../public/index.html", import.meta.url), "utf8");
  const db = fs.readFileSync(new URL("../lib/db.js", import.meta.url), "utf8");
  const meta = fs.readFileSync(new URL("../lib/meta.js", import.meta.url), "utf8");

  assert.match(html, /fetchSheet\('Meta_AdSets'\)/);
  assert.match(html, /fetchSheet\('Meta_Ads'\)/);
  assert.match(html, /Store revenue remains shared country-market evidence/);
  assert.match(db, /'meta_adsets': 'Meta_AdSets'/);
  assert.match(db, /options\.updateExisting/);
  assert.match(meta, /level:\s*"ad"/);
  assert.match(meta, /daily_budget/);
  assert.match(meta, /\{ createSheet: true, updateExisting: true \}/);
});

test("Meta stable-field fallback does not retry invalid date ranges", () => {
  assert.equal(shouldRetryMetaStableFields(new Error("(#100) For field 'insights': since must be less than or equal to until in time_range")), false);
  assert.equal(shouldRetryMetaStableFields(new Error("(#100) Tried accessing nonexisting field reach")), true);
});
