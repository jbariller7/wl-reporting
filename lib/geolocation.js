import { isIP } from "node:net";

const COUNTRY_IS_ENDPOINT = "https://api.country.is/";
const COUNTRY_IS_BATCH_LIMIT = 100;

function errorDetail(payload) {
  if (!payload || typeof payload !== "object") return "unexpected response";
  const detail = payload.message || payload.error;
  return typeof detail === "string" ? detail : JSON.stringify(detail || payload).slice(0, 200);
}

function countryResults(payload) {
  if (Array.isArray(payload)) return payload;
  if (Array.isArray(payload?.results)) return payload.results;
  return null;
}

function pause(milliseconds) {
  return milliseconds > 0 ? new Promise((resolve) => setTimeout(resolve, milliseconds)) : Promise.resolve();
}

export async function geolocateIps(ips, options = {}) {
  const fetchImpl = options.fetchImpl || globalThis.fetch;
  const pauseMs = options.pauseMs ?? 125;
  const uniqueIps = [...new Set((ips || []).map((ip) => String(ip || "").trim()).filter(Boolean))];
  const validIps = uniqueIps.filter((ip) => isIP(ip) !== 0);
  const map = {};
  const warnings = [];

  if (uniqueIps.length > validIps.length) {
    warnings.push(`Skipped ${uniqueIps.length - validIps.length} invalid IP address(es).`);
  }

  for (let start = 0; start < validIps.length; start += COUNTRY_IS_BATCH_LIMIT) {
    if (start > 0) await pause(pauseMs);
    const batch = validIps.slice(start, start + COUNTRY_IS_BATCH_LIMIT);
    try {
      const response = await fetchImpl(COUNTRY_IS_ENDPOINT, {
        method: "POST",
        headers: { "Content-Type": "application/json", "Accept": "application/json" },
        body: JSON.stringify(batch)
      });
      const payload = await response.json();
      if (!response.ok) {
        warnings.push(`Country lookup batch ${Math.floor(start / COUNTRY_IS_BATCH_LIMIT) + 1} failed (${response.status}): ${errorDetail(payload)}`);
        continue;
      }

      const results = countryResults(payload);
      if (!results) {
        warnings.push(`Country lookup batch ${Math.floor(start / COUNTRY_IS_BATCH_LIMIT) + 1} returned an unexpected response: ${errorDetail(payload)}`);
        continue;
      }

      for (const result of results) {
        if (result?.ip && result?.country) map[result.ip] = String(result.country).toUpperCase();
      }
    } catch (error) {
      warnings.push(`Country lookup batch ${Math.floor(start / COUNTRY_IS_BATCH_LIMIT) + 1} failed: ${error.message}`);
    }
  }

  return {
    map,
    requested: uniqueIps.length,
    valid: validIps.length,
    resolved: Object.keys(map).length,
    warnings
  };
}
