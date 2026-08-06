import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";

const html = fs.readFileSync(new URL("../public/index.html", import.meta.url), "utf8");

test("macro overview presents subscribers, wishlists, and total leads as paired count-and-cost cards", () => {
  assert.match(html, /id="kpi-subs"/);
  assert.match(html, /id="kpi-cps">Cost \/ subscriber: -/);
  assert.match(html, /id="kpi-wishlist-adds"/);
  assert.match(html, /id="kpi-cost-per-wishlist">Cost \/ wishlist: -/);
  assert.match(html, /id="kpi-total-leads"/);
  assert.match(html, /id="kpi-cost-per-lead">Cost \/ lead: -/);
  assert.match(html, /const totalLeads = totalSubs \+ totalWishlistAdds/);
  assert.match(html, /totalSpend \/ totalLeads/);
});

test("correlation explorer exposes total leads and cost per lead", () => {
  assert.match(html, /value="totalLeads"[^>]*> Total Leads/);
  assert.match(html, /value="costPerLead"[^>]*> Cost\/Lead/);
  assert.match(html, /totalLeads: mapData\('totalLeads', false\)/);
  assert.match(html, /costPerLead: mapData\('costPerLead', false\)/);
  assert.doesNotMatch(html, />Spend\/Wishlist</);
});

test("daily funnel diagnosis and AI export include lead totals and costs", () => {
  assert.match(html, /d\.totalLeads = d\.subs \+ d\.wishlists/);
  assert.match(html, /d\.costPerLead = d\.totalLeads > 0/);
  assert.match(html, />Total Leads<\/th>/);
  assert.match(html, />Cost\/Lead<br>/);
  assert.match(html, /\| Total Leads \| Cost\/Lead \(4D Rolling\) \|/);
  assert.match(html, /d\.totalLeads} \| \$\{costLeadStr}/);
});
