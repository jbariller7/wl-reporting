import { getDoc, readSheetColumns } from "../../lib/db.js";
import {
  latestRowDate,
  mergeDashboardSubscribers,
  recentSubscriberRepairWindow
} from "../../lib/dashboard-data.js";
import { bad } from "../../lib/util.js";

function cleanRows(rows) {
  return rows.map(({ __rowNumber, ...row }) => row);
}

export const handler = async (event) => {
  if (event.httpMethod !== "GET") return bad("Use GET");

  try {
    const [steam, googlePlay, installs, derivedSubscribers, rawSubscriberDates] = await Promise.all([
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
      readSheetColumns("google_play_installs", ["Date", "Country", "Daily User Installs"], { unbounded: true })
        .catch(error => {
          if (error.message.includes('Tab named "Google_Play_Installs" not found')) return [];
          throw error;
        }),
      readSheetColumns("mailerlite_dashboard_subscribers", ["subscriber_id", "created_at", "country"]),
      readSheetColumns("mailerlite_subscribers", ["subscriber_id", "created_at", "country"])
    ]);

    const derivedLatest = latestRowDate(derivedSubscribers, "created_at");
    const rawLatest = latestRowDate(rawSubscriberDates, "created_at");
    const repairWindow = recentSubscriberRepairWindow(rawSubscriberDates, rawLatest);

    let rawSubscribers = [];
    if (repairWindow) {
      const repairRowNumbers = new Set(repairWindow.rowNumbers);
      rawSubscribers = await readSheetColumns(
        "mailerlite_subscribers",
        ["subscriber_id", "created_at", "country", "raw"],
        { startRow: repairWindow.startRow, endRow: repairWindow.endRow }
      );
      rawSubscribers = rawSubscribers.filter(row => repairRowNumbers.has(row.__rowNumber));
    }

    const mailerlite = mergeDashboardSubscribers(derivedSubscribers, rawSubscribers);
    const repairSummary = repairWindow ? {
      cutoffDate: repairWindow.cutoffDate,
      latestDate: repairWindow.latestDate,
      startRow: repairWindow.startRow,
      endRow: repairWindow.endRow,
      selectedRows: repairWindow.rowNumbers.length
    } : null;
    const body = JSON.stringify({
      steam: cleanRows(steam),
      googlePlay: cleanRows(googlePlay),
      installs: cleanRows(installs),
      mailerlite,
      freshness: {
        steam: latestRowDate(steam, "date"),
        googlePlay: latestRowDate(googlePlay, "Order Charged Date"),
        installs: latestRowDate(installs, "Date"),
        mailerlite: latestRowDate(mailerlite, "created_at"),
        mailerliteDerived: derivedLatest,
        mailerliteRaw: rawLatest,
        mailerliteRepairWindow: repairSummary,
        mailerliteRepairedRows: rawSubscribers.length
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
