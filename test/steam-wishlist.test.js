import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import { wishlistDatesInRange, wishlistRowsFromResponse } from "../lib/steam-wishlist.js";

test("Steam wishlist sync expands an inclusive GMT date range", () => {
  assert.deepEqual(
    wishlistDatesInRange("2026-07-30T14:00:00.000Z", "2026-08-02T23:59:59.999Z"),
    ["2026-07-30", "2026-07-31", "2026-08-01", "2026-08-02"]
  );
});

test("Steam wishlist API response becomes one row per country and day", () => {
  const rows = wishlistRowsFromResponse({ response: {
    appid: 440,
    date: "2026-02-15",
    wishlist_summary: { wishlist_adds: 30, wishlist_deletes: 15, wishlist_purchases: 10, wishlist_gifts: 5, wishlist_adds_windows: 22, wishlist_adds_mac: 3, wishlist_adds_linux: 5 },
    country_summary: [
      { country_code: "CA", country_name: "Canada", region: "North America", summary_actions: { wishlist_adds: 20, wishlist_deletes: 6, wishlist_purchases: 9, wishlist_gifts: 2, wishlist_adds_windows: 15, wishlist_adds_mac: 1, wishlist_adds_linux: 4 } },
      { country_code: "FR", country_name: "France", region: "Western Europe", summary_actions: { wishlist_adds: 10, wishlist_deletes: 9, wishlist_purchases: 1, wishlist_gifts: 3, wishlist_adds_windows: 7, wishlist_adds_mac: 2, wishlist_adds_linux: 1 } }
    ]
  } }, "440", "2026-02-15", "2026-02-16T01:00:00.000Z");

  assert.equal(rows.length, 2);
  assert.deepEqual(rows.map(row => [row.date, row.app_id, row.country, row.wishlist_adds]), [
    ["2026-02-15", "440", "CA", 20],
    ["2026-02-15", "440", "FR", 10]
  ]);
  assert.equal(rows[1].wishlist_purchases, 1);
  assert.equal(rows[1].wishlist_adds_mac, 2);
});

test("Steam wishlist sync preserves unallocated totals and zero-activity dates", () => {
  const partial = wishlistRowsFromResponse({ response: {
    wishlist_summary: { wishlist_adds: 5, wishlist_deletes: 1 },
    country_summary: [{ country_code: "US", summary_actions: { wishlist_adds: 4, wishlist_deletes: 1 } }]
  } }, "999", "2026-08-01");
  assert.equal(partial.at(-1).country, "UNKNOWN");
  assert.equal(partial.at(-1).wishlist_adds, 1);

  const zero = wishlistRowsFromResponse({ response: { wishlist_summary: {} } }, "999", "2026-08-02");
  assert.equal(zero.length, 1);
  assert.equal(zero[0].wishlist_adds, 0);
});

test("Steam button, scheduled sync, and dashboards all consume wishlist API data", () => {
  const etl = fs.readFileSync(new URL("../lib/etl.js", import.meta.url), "utf8");
  const db = fs.readFileSync(new URL("../lib/db.js", import.meta.url), "utf8");
  const handler = fs.readFileSync(new URL("../netlify/functions/fetch-steam-sales.js", import.meta.url), "utf8");
  const cron = fs.readFileSync(new URL("../netlify/functions/cron-daily.mjs", import.meta.url), "utf8");
  const html = fs.readFileSync(new URL("../public/index.html", import.meta.url), "utf8");

  assert.match(etl, /IPartnerFinancialsService\/GetAppWishlistReporting\/v001\//);
  assert.match(etl, /clearDateRange\("steam_wishlist", "date"/);
  assert.match(etl, /ensureSheetColumns\("steam_wishlist", STEAM_WISHLIST_COLUMNS/);
  assert.match(etl, /\["date", "app_id", "country"\]/);
  assert.match(db, /'steam_wishlist': 'Steam_Wishlist'/);
  assert.match(handler, /etlSteamData\(range\)/);
  assert.match(cron, /etlSteamData\(range\)/);
  assert.match(html, /fetchSheet\('Steam_Wishlist', \['A', 'C', 'F', 'G', 'H'\]\)/);
  assert.match(html, /id="kpi-wishlist-adds"/);
  assert.match(html, /Steam Wishlist Adds \| Cost\/Wishlist \(4D Rolling\)/);
  assert.match(html, /value="wishlistAdds"/);
  assert.match(html, /country-level commercial-intent signal/);
});
