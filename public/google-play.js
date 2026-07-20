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

  global.WLGooglePlay = Object.freeze({ normalizeRow, parseNumber });
})(typeof window === "undefined" ? globalThis : window);
