import { JWT } from "google-auth-library";
import { upsert } from "./db.js";

export const FIREBASE_INSTALL_COLUMNS = ["Date", "Package Name", "Country", "First Opens", "Stream ID", "Synced At"];
const DEFAULT_PROPERTY = "545026433";
const DEFAULT_PACKAGE = "com.wonderlang.app";

export function firstOpenRequest({ sinceUtc, untilUtc, streamId, offset = 0 }) {
  const expressions = [
    { filter: { fieldName: "eventName", stringFilter: { matchType: "EXACT", value: "first_open" } } },
    { filter: { fieldName: "platform", stringFilter: { matchType: "EXACT", value: "Android", caseSensitive: false } } }
  ];
  if (streamId) expressions.push({ filter: { fieldName: "streamId", stringFilter: { matchType: "EXACT", value: String(streamId) } } });
  return {
    dateRanges: [{ startDate: sinceUtc.slice(0, 10), endDate: untilUtc.slice(0, 10) }],
    dimensions: ["date", "countryId", "streamId", "streamName"].map(name => ({ name })),
    metrics: [{ name: "eventCount" }],
    dimensionFilter: { andGroup: { expressions } },
    limit: 10000, offset
  };
}

export function firstOpenRows(reports, { packageId = DEFAULT_PACKAGE, streamId, syncedAt }) {
  const rows = reports.flatMap(report => report.rows || []);
  const streams = [...new Set(rows.map(row => row.dimensionValues?.[2]?.value).filter(Boolean))];
  if (!streamId && streams.length > 1) throw new Error("Multiple Android streams returned. Set FIREBASE_ANDROID_STREAM_ID before importing to avoid combining apps.");
  return rows.map(row => {
    const [day, rawCountry, stream] = row.dimensionValues.map(value => value.value);
    const count = Number(row.metricValues?.[0]?.value);
    if (!/^\d{8}$/.test(day) || !Number.isSafeInteger(count) || count < 0 || !stream) throw new Error("Invalid Firebase first-open date, count or stream.");
    const country = /^[A-Z]{2}$/i.test(rawCountry || "") ? rawCountry.toUpperCase() : "ZZ";
    return {
      Date: `${day.slice(0, 4)}-${day.slice(4, 6)}-${day.slice(6)}`,
      "Package Name": packageId, Country: country, "First Opens": count,
      "Stream ID": stream, "Synced At": syncedAt
    };
  });
}

export async function etlFirebaseInstalls({ sinceUtc, untilUtc, dryRun = false }) {
  const propertyId = process.env.FIREBASE_GA4_PROPERTY_ID || DEFAULT_PROPERTY;
  const packageId = process.env.GOOGLE_PLAY_PACKAGE_ID || DEFAULT_PACKAGE;
  const streamId = process.env.FIREBASE_ANDROID_STREAM_ID;
  const email = process.env.GOOGLE_SERVICE_ACCOUNT_EMAIL;
  const key = (process.env.GOOGLE_PRIVATE_KEY || "").replace(/\\n/g, "\n");
  const syncedAt = new Date().toISOString();
  const logs = [];
  if (!email || !key) return { ok: false, msg: "Missing reporting service account credentials", logs };
  if (!/^\d+$/.test(propertyId)) return { ok: false, msg: "Invalid FIREBASE_GA4_PROPERTY_ID", logs };
  try {
    const auth = new JWT({ email, key, scopes: ["https://www.googleapis.com/auth/analytics.readonly"] });
    const credentials = await auth.authorize();
    const reports = [];
    let offset = 0;
    while (true) {
      const response = await fetch(`https://analyticsdata.googleapis.com/v1beta/properties/${propertyId}:runReport`, {
        method: "POST", headers: { Authorization: `Bearer ${credentials.access_token}`, "content-type": "application/json" },
        body: JSON.stringify(firstOpenRequest({ sinceUtc, untilUtc, streamId, offset }))
      });
      const report = await response.json();
      if (!response.ok) throw new Error(report.error?.message || `Analytics HTTP ${response.status}`);
      reports.push(report);
      offset += report.rows?.length || 0;
      if (offset >= Number(report.rowCount || 0)) break;
      if (!report.rows?.length || reports.length >= 100) throw new Error("Incomplete Firebase report pagination");
    }
    const rows = firstOpenRows(reports, { packageId, streamId, syncedAt });
    const firstOpens = rows.reduce((sum, row) => sum + row["First Opens"], 0);
    const unknownCountryOpens = rows.filter(row => row.Country === "ZZ").reduce((sum, row) => sum + row["First Opens"], 0);
    const propertyTimeZone = reports[0]?.metadata?.timeZone || null;
    if (!dryRun) await upsert("firebase_installs", ["Date", "Package Name", "Country", "Stream ID"], FIREBASE_INSTALL_COLUMNS, rows,
      { createSheet: true, updateExisting: true, userEnteredColumns: ["First Opens"] });
    logs.push({ time: syncedAt, type: "success", msg: `Firebase: ${firstOpens} Android first opens across ${rows.length} date/country rows. ${unknownCountryOpens} have unknown country. Reporting dates use ${propertyTimeZone || "the Analytics property timezone"}; recent counts are provisional.` });
    return { ok: true, dryRun, propertyId, propertyTimeZone, syncedAt, rows: rows.length, firstOpens, unknownCountryOpens,
      latestEventDate: rows.map(row => row.Date).sort().at(-1) || null,
      streams: [...new Set(rows.map(row => row["Stream ID"]))], subjectToThresholding: reports.some(report => report.metadata?.subjectToThresholding), logs };
  } catch (error) {
    return { ok: false, msg: `Firebase first-open sync: ${error.message}`, propertyId, serviceAccountEmail: email, logs };
  }
}
