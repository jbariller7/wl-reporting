import crypto from "node:crypto";
import { parse } from "csv-parse/sync";
import { unzipSync } from "fflate";

const DEFAULT_FEE_RATE = 0.15;

function parseMoney(value) {
  if (value === null || value === undefined || value === "") return 0;
  const parsed = Number(String(value).replace(/,/g, "").trim());
  return Number.isFinite(parsed) ? parsed : 0;
}

function getField(row, name) {
  if (Object.prototype.hasOwnProperty.call(row, name)) return row[name];
  const normalizedName = name.toLowerCase();
  const matchingKey = Object.keys(row).find((key) => key.trim().toLowerCase() === normalizedName);
  return matchingKey ? row[matchingKey] : undefined;
}

function signedAmount(value, direction) {
  const amount = parseMoney(value);
  if (direction < 0) return -Math.abs(amount);
  if (direction > 0) return Math.abs(amount);
  return amount;
}

function getDirection(status) {
  const normalized = String(status || "").toLowerCase();
  if (normalized.includes("refund") || normalized.includes("chargeback") || normalized.includes("reversal")) return -1;
  if (normalized.includes("charg") || normalized.includes("paid") || normalized.includes("rebill")) return 1;
  return 0;
}

function normalizeFeeRate(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed >= 0 && parsed <= 1 ? parsed : DEFAULT_FEE_RATE;
}

export function monthsInRange(sinceUtc, untilUtc) {
  const start = new Date(sinceUtc);
  const end = new Date(untilUtc);
  if (Number.isNaN(start.getTime()) || Number.isNaN(end.getTime()) || start > end) return [];

  const months = [];
  let year = start.getUTCFullYear();
  let month = start.getUTCMonth();
  const endYear = end.getUTCFullYear();
  const endMonth = end.getUTCMonth();

  while (year < endYear || (year === endYear && month <= endMonth)) {
    months.push(`${year}${String(month + 1).padStart(2, "0")}`);
    month += 1;
    if (month === 12) {
      month = 0;
      year += 1;
    }
  }
  return months;
}

export function decodeGooglePlayCsv(bytes) {
  const data = bytes instanceof Uint8Array ? bytes : new Uint8Array(bytes);
  if (data.length >= 2 && data[0] === 0xff && data[1] === 0xfe) {
    return new TextDecoder("utf-16le").decode(data.subarray(2));
  }
  if (data.length >= 2 && data[0] === 0xfe && data[1] === 0xff) {
    const swapped = new Uint8Array(data.length - 2);
    for (let i = 2; i + 1 < data.length; i += 2) {
      swapped[i - 2] = data[i + 1];
      swapped[i - 1] = data[i];
    }
    return new TextDecoder("utf-16le").decode(swapped);
  }
  return new TextDecoder("utf-8").decode(data).replace(/^\uFEFF/, "");
}

export function parseGooglePlaySalesCsv(csvText, options = {}) {
  const sinceDate = String(options.sinceUtc || "0000-01-01").slice(0, 10);
  const untilDate = String(options.untilUtc || "9999-12-31").slice(0, 10);
  const packageId = options.packageId ? String(options.packageId).trim() : null;
  const feeRate = normalizeFeeRate(options.feeRate);
  const sourceFile = options.sourceFile || "salesreport.csv";

  const parsedRows = parse(csvText, {
    columns: true,
    bom: true,
    skip_empty_lines: true,
    relax_column_count: true,
    trim: true
  });

  const duplicateCounts = new Map();
  const result = [];

  for (const rawRow of parsedRows) {
    const date = String(getField(rawRow, "Order Charged Date") || "").slice(0, 10);
    const rowPackageId = String(getField(rawRow, "Package ID") || "").trim();
    if (!date || date < sinceDate || date > untilDate) continue;
    if (packageId && rowPackageId !== packageId) continue;

    const status = String(getField(rawRow, "Financial Status") || "unknown").trim();
    const direction = getDirection(status);
    if (direction === 0) continue;

    const itemPrice = signedAmount(getField(rawRow, "Item Price"), direction);
    const taxesCollected = signedAmount(getField(rawRow, "Taxes Collected"), direction);
    const chargedAmount = signedAmount(getField(rawRow, "Charged Amount"), direction);
    const rawSignature = JSON.stringify(rawRow);
    const occurrence = duplicateCounts.get(rawSignature) || 0;
    duplicateCounts.set(rawSignature, occurrence + 1);
    const transactionId = crypto
      .createHash("sha256")
      .update(`${sourceFile}\n${rawSignature}\n${occurrence}`)
      .digest("hex");

    const chargedTimestamp = Number(getField(rawRow, "Order Charged Timestamp"));
    const chargedAt = Number.isFinite(chargedTimestamp) && chargedTimestamp > 0
      ? new Date(chargedTimestamp * 1000).toISOString()
      : `${date}T00:00:00.000Z`;

    result.push({
      transaction_id: transactionId,
      order_number: String(getField(rawRow, "Order Number") || "").trim(),
      date,
      charged_at: chargedAt,
      status,
      package_id: rowPackageId,
      product_title: String(getField(rawRow, "Product Title") || "").trim(),
      product_type: String(getField(rawRow, "Product Type") || "").trim(),
      sku_id: String(getField(rawRow, "SKU ID") || "").trim(),
      country: String(getField(rawRow, "Country of Buyer") || "Unknown").trim().toUpperCase(),
      currency: String(getField(rawRow, "Currency of Sale") || "EUR").trim().toUpperCase(),
      units: direction,
      item_price: itemPrice,
      taxes_collected: taxesCollected,
      charged_amount: chargedAmount,
      net_revenue: itemPrice * (1 - feeRate),
      fee_rate: feeRate,
      source_file: sourceFile,
      raw: rawRow,
      ingested_at: new Date().toISOString()
    });
  }

  return result;
}

export function extractGooglePlaySalesRowsFromZip(zipBytes, options = {}) {
  const entries = unzipSync(zipBytes instanceof Uint8Array ? zipBytes : new Uint8Array(zipBytes));
  const csvEntries = Object.entries(entries).filter(([name]) => name.toLowerCase().endsWith(".csv"));
  if (csvEntries.length === 0) throw new Error("Google Play report ZIP did not contain a CSV file");

  return csvEntries.flatMap(([name, bytes]) => parseGooglePlaySalesCsv(decodeGooglePlayCsv(bytes), {
    ...options,
    sourceFile: options.sourceFile || name
  }));
}

