import { getDoc, normalizeSheetDateValue, readSheetColumns } from "../../lib/db.js";
import { latestRowDate, mergeDashboardSubscribers } from "../../lib/dashboard-data.js";
import { bad } from "../../lib/util.js";

function cleanRows(rows) {
  return rows.map(({ __rowNumber, ...row }) => row);
}

export const handler = async (event) => {
  if (event.httpMethod !== "GET") return bad("Use GET");

  try {
    const [steam, googlePlay, derivedSubscribers, rawSubscriberDates] = await Promise.all([
      readSheetColumns("steam_sales", ["date", "country", "net_revenue"]),
      readSheetColumns("google_play_sales", [
        "Order Number",
        "Order Charged Date",
        "Order Charged Timestamp",
        "Financial Status",
        "SKU ID",
        "Currency of Sale",
        "Item Price",
        "Charged Amount",
        "Country of Buyer"
      ]),
      readSheetColumns("mailerlite_dashboard_subscribers", ["subscriber_id", "created_at", "country"]),
      readSheetColumns("mailerlite_subscribers", ["created_at"])
    ]);

    const derivedLatest = latestRowDate(derivedSubscribers, "created_at");
    const freshRawRows = rawSubscriberDates.filter(row => {
      const date = normalizeSheetDateValue(row.created_at);
      return /^\d{4}-\d{2}-\d{2}$/.test(date) && (!derivedLatest || date > derivedLatest);
    });

    let rawSubscribers = [];
    if (freshRawRows.length > 0) {
      const startRow = Math.min(...freshRawRows.map(row => row.__rowNumber));
      const endRow = Math.max(...freshRawRows.map(row => row.__rowNumber));
      rawSubscribers = await readSheetColumns(
        "mailerlite_subscribers",
        ["subscriber_id", "created_at", "country", "raw"],
        { startRow, endRow }
      );
      rawSubscribers = rawSubscribers.filter(row => {
        const date = normalizeSheetDateValue(row.created_at);
        return !derivedLatest || date > derivedLatest;
      });
    }

    const mailerlite = mergeDashboardSubscribers(derivedSubscribers, rawSubscribers);
    const rawLatest = latestRowDate(rawSubscriberDates, "created_at");
    const body = JSON.stringify({
      steam: cleanRows(steam),
      googlePlay: cleanRows(googlePlay),
      mailerlite,
      freshness: {
        steam: latestRowDate(steam, "date"),
        googlePlay: latestRowDate(googlePlay, "Order Charged Date"),
        mailerlite: latestRowDate(mailerlite, "created_at"),
        mailerliteDerived: derivedLatest,
        mailerliteRaw: rawLatest
      }
    });

    return {
      statusCode: 200,
      headers: {
        "content-type": "application/json",
        "access-control-allow-origin": "*",
        "cache-control": "public, max-age=60, stale-while-revalidate=240"
      },
      body
    };
  } catch (error) {
    return bad(`Dashboard data error: ${error.message}`, 500);
  }
};
