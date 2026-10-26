import { parse } from "csv-parse/sync";
import { decodeGooglePlayCsv, monthsInRange } from "./google-play.js";

export const GOOGLE_PLAY_INSTALL_COLUMNS = [
  "Date",
  "Package Name",
  "Country",
  "Daily User Installs",
  "Daily Device Installs"
];

export function installReportPaths(packageId, sinceUtc, untilUtc) {
  if (!packageId) throw new Error("GOOGLE_PLAY_PACKAGE_ID is required for install reports");
  return monthsInRange(sinceUtc, untilUtc).map(month =>
    `stats/installs/installs_${packageId}_${month}_country.csv`
  );
}

function count(value, field) {
  const text = String(value ?? "").trim();
  if (!/^\d+$/.test(text)) throw new Error(`Invalid ${field} in Google Play installs report: ${text}`);
  return Number(text);
}

export function parseGooglePlayInstallsCsv(csvText, { packageId, sinceUtc, untilUtc, onMissingCountry, onReport }) {
  const rows = parse(csvText, { columns: true, bom: true, skip_empty_lines: true, trim: true });
  const since = String(sinceUtc).slice(0, 10);
  const until = String(untilUtc).slice(0, 10);
  const packageColumn = rows.length ? Object.keys(rows[0]).find(column => column.trim().toLowerCase() === "package name") : null;
  if (rows.length) {
    const required = GOOGLE_PLAY_INSTALL_COLUMNS.filter(column => column !== "Package Name");
    const missing = required.filter(column => !(column in rows[0]));
    if (missing.length) throw new Error(`Google Play installs report is missing: ${missing.join(", ")}`);
  }
  // Inspect the source before filtering the selected dates, so a successful
  // download cannot be mistaken for newly published install data.
  // The report path identifies the package even when its CSV omits that column.
  const packageRows = rows.filter(row => !packageColumn || row[packageColumn] === packageId);
  const datedRows = packageRows.filter(row => /^\d{4}-\d{2}-\d{2}$/.test(row.Date || ""));
  const dates = datedRows.map(row => row.Date).sort();
  const countryDates = datedRows.filter(row => String(row.Country ?? "").trim()).map(row => row.Date).sort();
  onReport?.({
    sourceRows: rows.length,
    packageRows: packageRows.length,
    invalidDateRows: packageRows.length - datedRows.length,
    firstDate: dates[0] || null,
    latestDate: dates.at(-1) || null,
    latestCountryDate: countryDates.at(-1) || null
  });
  return packageRows.filter(row => row.Date >= since && row.Date <= until)
    .flatMap(row => {
      const country = String(row.Country ?? "").trim();
      if (!country) {
        onMissingCountry?.(row);
        return [];
      }
      return [{
        Date: row.Date,
        "Package Name": packageId,
        Country: country,
        "Daily User Installs": count(row["Daily User Installs"], "Daily User Installs"),
        "Daily Device Installs": count(row["Daily Device Installs"], "Daily Device Installs")
      }];
    });
}

export function parseGooglePlayInstallsBytes(bytes, options) {
  return parseGooglePlayInstallsCsv(decodeGooglePlayCsv(bytes), options);
}
