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

export function parseGooglePlayInstallsCsv(csvText, { packageId, sinceUtc, untilUtc }) {
  const rows = parse(csvText, { columns: true, bom: true, skip_empty_lines: true, trim: true });
  const since = String(sinceUtc).slice(0, 10);
  const until = String(untilUtc).slice(0, 10);
  if (rows.length) {
    const required = GOOGLE_PLAY_INSTALL_COLUMNS;
    const missing = required.filter(column => !(column in rows[0]));
    if (missing.length) throw new Error(`Google Play installs report is missing: ${missing.join(", ")}`);
  }
  return rows.filter(row => row.Date >= since && row.Date <= until && row["Package Name"] === packageId)
    .map(row => {
      const country = String(row.Country ?? "").trim();
      if (!country) throw new Error(`Missing Country in Google Play installs report for ${row.Date}`);
      return {
        Date: row.Date,
        "Package Name": row["Package Name"],
        Country: country,
        "Daily User Installs": count(row["Daily User Installs"], "Daily User Installs"),
        "Daily Device Installs": count(row["Daily Device Installs"], "Daily Device Installs")
      };
    });
}

export function parseGooglePlayInstallsBytes(bytes, options) {
  return parseGooglePlayInstallsCsv(decodeGooglePlayCsv(bytes), options);
}
