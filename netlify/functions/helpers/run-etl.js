import {
  etlStripe,
  etlMeta,
  etlTikTok,
  etlMailerLite,
  etlSteamSalesApi
} from "../../../lib/etl.js";
import { setCursor } from "../../../lib/util.js";

const ETL_MAP = {
  stripe: etlStripe,
  meta: etlMeta,
  tiktok: etlTikTok,
  mailerlite: etlMailerLite,
  steam: etlSteamSalesApi
};

/**
 * Run ETL jobs for the provided sources using a shared range.
 *
 * @param {{ sinceUtc: string, untilUtc: string }} range
 * @param {string[]} sources
 * @param {{ persistCursor?: boolean }} options
 */
export async function runEtls(range, sources, { persistCursor = false } = {}) {
  const results = {};

  for (const source of sources) {
    const runner = ETL_MAP[source];
    if (!runner) continue;
    const res = await runner(range).catch((e) => ({ ok: false, msg: e.message }));
    results[source] = res;
    if (persistCursor && res?.ok) {
      await setCursor(source, range.untilUtc);
    }
  }

  return results;
}
