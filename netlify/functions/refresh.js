import { parseRange, ok, bad } from "../../lib/util.js";
import { etlStripe, etlMeta, etlTikTok, etlMailerLite, etlSteamData, etlGooglePlaySales, etlGooglePlayInstalls, etlTelemetry } from "../../lib/etl.js";

export const handler = async (event) => {
  if (event.httpMethod !== "POST") return bad("Use POST");
  const body = event.body ? JSON.parse(event.body) : {};
  const range = parseRange({ since: body.since, until: body.until });
  range.forceRefresh = body.forceRefresh === true;
  
  const sources = body.sources || ["stripe","meta","mailerlite","steam","google_play","google_play_installs","telemetry"];
  const results = {};
  
  for (const s of sources) {
    if (s === "stripe") results.stripe = await etlStripe(range).catch(e => ({ ok: false, msg: e.message }));
    if (s === "meta") results.meta = await etlMeta(range).catch(e => ({ ok: false, msg: e.message }));
    if (s === "tiktok") results.tiktok = await etlTikTok(range).catch(e => ({ ok: false, msg: e.message }));
    if (s === "mailerlite") results.mailerlite = await etlMailerLite(range).catch(e => ({ ok: false, msg: e.message }));
    if (s === "steam") results.steam = await etlSteamData(range).catch(e => ({ ok: false, msg: e.message }));
    if (s === "google_play") results.google_play = await etlGooglePlaySales(range).catch(e => ({ ok: false, msg: e.message }));
    if (s === "google_play_installs") results.google_play_installs = await etlGooglePlayInstalls(range).catch(e => ({ ok: false, msg: e.message }));
    if (s === "telemetry") results.telemetry = await etlTelemetry(range).catch(e => ({ ok: false, msg: e.message }));
  }
  return ok({ range, results });
};
