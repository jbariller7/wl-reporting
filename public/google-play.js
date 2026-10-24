(function attachGooglePlayHelpers(global) {
  const DEFAULT_FEE_RATE = 0.15;

  function parseNumber(value) {
    let text = String(value ?? "").trim().replace(/\s/g, "");
    if (!text) return 0;
    if (text.includes(",") && text.includes(".")) {
      if (text.lastIndexOf(",") > text.lastIndexOf(".")) {
        text = text.replace(/\./g, "").replace(",", ".");
      } else {
        text = text.replace(/,/g, "");
      }
    } else if (text.includes(",")) {
      text = text.replace(",", ".");
    }
    const parsed = Number(text);
    return Number.isFinite(parsed) ? parsed : 0;
  }

  function getDirection(status) {
    const normalized = String(status || "").toLowerCase();
    if (normalized.includes("refund") || normalized.includes("chargeback") || normalized.includes("reversal")) return -1;
    if (normalized.includes("charg") || normalized.includes("paid") || normalized.includes("rebill")) return 1;
    return 0;
  }

  function normalizeRow(row, feeRate = DEFAULT_FEE_RATE) {
    const direction = getDirection(row?.["Financial Status"]);
    if (direction === 0) return null;
    const itemPrice = Math.abs(parseNumber(row?.["Item Price"]));
    return {
      date: row?.["Order Charged Date"] || "",
      netRevenue: direction * itemPrice * (1 - feeRate),
      currency: row?.["Currency of Sale"] || "EUR",
      country: row?.["Country of Buyer"] || "UNKNOWN",
      units: direction
    };
  }

  function installCoverage(rows, { startDate, endDate, normalizeDate, includeCountry = () => true, isExcluded = () => false }) {
    const datedRows = (rows || []).map(row => ({ row, date: normalizeDate(row.Date) }))
      .filter(item => /^\d{4}-\d{2}-\d{2}$/.test(item.date || ""));
    const latestDate = datedRows.reduce((latest, item) => item.date > latest ? item.date : latest, "");
    if (!latestDate) return { status: "unavailable", latestDate: null, weekStart: null, weekInstalls: null };

    const weekStart = new Date(Date.parse(`${latestDate}T00:00:00Z`) - 6 * 86400000).toISOString().slice(0, 10);
    const weekInstalls = datedRows.reduce((total, { row, date }) => {
      const country = String(row.Country || "UNKNOWN").trim().toUpperCase().replace(/^UK$/, "GB");
      return date >= weekStart && date <= latestDate && includeCountry(country) && !isExcluded(date)
        ? total + parseNumber(row["Daily User Installs"])
        : total;
    }, 0);
    const status = startDate > latestDate ? "pending" : endDate > latestDate ? "partial" : "complete";
    return { status, latestDate, weekStart, weekInstalls };
  }

  function formatInstalls(value, coverage) {
    if (coverage?.status === "unavailable") return "N/A";
    if (coverage?.status === "pending") return "Pending";
    return String(value || 0);
  }

  global.WLGooglePlay = Object.freeze({ normalizeRow, parseNumber, installCoverage, formatInstalls });
})(typeof window === "undefined" ? globalThis : window);
