import test from "node:test";
import assert from "node:assert/strict";
import { zipSync } from "fflate";
import {
  extractGooglePlaySalesRowsFromZip,
  GOOGLE_PLAY_SHEET_COLUMNS,
  monthsInRange,
  parseGooglePlaySalesCsv,
  toGooglePlaySheetRows
} from "../lib/google-play.js";

const HEADER = "Order Number,Order Charged Date,Order Charged Timestamp,Financial Status,Product Title,Package ID,Product Type,SKU ID,Currency of Sale,Item Price,Taxes Collected,Charged Amount,Country of Buyer";

test("monthsInRange includes every intersecting report month", () => {
  assert.deepEqual(
    monthsInRange("2026-01-30T00:00:00.000Z", "2026-03-01T00:00:00.000Z"),
    ["202601", "202602", "202603"]
  );
});

test("sales rows are filtered by package and refunds reduce estimated net revenue", () => {
  const csv = [
    HEADER,
    "GPA.1,2026-07-10,1783641600,Charged,WonderLang,com.wonderlang.game,Paid app,wonderlang,EUR,10.00,2.00,12.00,FR",
    "GPA.1,2026-07-12,1783814400,Refund,WonderLang,com.wonderlang.game,Paid app,wonderlang,EUR,10.00,2.00,12.00,FR",
    "GPA.2,2026-07-13,1783900800,Charged,Other App,com.example.other,Paid app,other,EUR,5.00,1.00,6.00,DE"
  ].join("\n");

  const rows = parseGooglePlaySalesCsv(csv, {
    sinceUtc: "2026-07-01T00:00:00.000Z",
    untilUtc: "2026-07-31T23:59:59.999Z",
    packageId: "com.wonderlang.game",
    feeRate: 0.15,
    sourceFile: "salesreport_202607.zip"
  });

  assert.equal(rows.length, 2);
  assert.equal(rows[0].net_revenue, 8.5);
  assert.equal(rows[1].net_revenue, -8.5);
  assert.equal(rows[1].units, -1);
  assert.equal(rows[0].country, "FR");
});

test("UTF-16 Google Play ZIP reports are decoded", () => {
  const csv = `${HEADER}\nGPA.3,2026-07-15,1784073600,Charged,WonderLang,com.wonderlang.game,Paid app,wonderlang,USD,4.00,0.80,4.80,US`;
  const utf16 = Buffer.from(`\uFEFF${csv}`, "utf16le");
  const zipped = zipSync({ "salesreport_202607.csv": utf16 });
  const rows = extractGooglePlaySalesRowsFromZip(zipped, {
    sinceUtc: "2026-07-01T00:00:00.000Z",
    untilUtc: "2026-07-31T23:59:59.999Z"
  });

  assert.equal(rows.length, 1);
  assert.equal(rows[0].currency, "USD");
  assert.equal(rows[0].net_revenue, 3.4);
});

test("automatic sync writes the native Google Play report schema", () => {
  const csv = `${HEADER}\nGPA.3321-9652-4732-87159,2026-07-05,1783288058,Charged,WonderLang,com.wonderlang.app,One-time product,wonderlangch1,GBP,6.66,1.33,7.99,GB`;
  const parsed = parseGooglePlaySalesCsv(csv, {
    sinceUtc: "2026-07-01T00:00:00.000Z",
    untilUtc: "2026-07-31T23:59:59.999Z",
    packageId: "com.wonderlang.app"
  });
  const [sheetRow] = toGooglePlaySheetRows(parsed);

  assert.deepEqual(Object.keys(sheetRow), GOOGLE_PLAY_SHEET_COLUMNS);
  assert.equal(sheetRow["Order Number"], "GPA.3321-9652-4732-87159");
  assert.equal(sheetRow["Order Charged Date"], "2026-07-05");
  assert.equal(sheetRow["Currency of Sale"], "GBP");
  assert.equal(sheetRow["Country of Buyer"], "GB");
  assert.equal(sheetRow["Sales Channel"], "");
});

