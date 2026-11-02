import '../public/steam-fx.js';
import snapshot from '../public/steam-fx-snapshot.json' with { type: 'json' };

const { SOURCE, parseEcbUsd, buildMonthlyRates } = globalThis.WLSteamFx;
let cached;
let cachedAt = 0;
let savedObservations = snapshot.observations;

export async function getSteamExchangeRates({ fetchImpl = fetch, now = new Date() } = {}) {
  const asOf = now.toISOString().slice(0, 10);
  if (cached && cached.asOf === asOf && Date.now() - cachedAt < 3600000) return cached;
  try {
    const response = await fetchImpl(SOURCE, { signal: AbortSignal.timeout(12000) });
    if (!response.ok) throw new Error(`ECB HTTP ${response.status}`);
    const observations = parseEcbUsd(await response.text());
    // A truncated history must not silently replace verified historical rates.
    const merged = new Map(snapshot.observations.map(o => [o.date, o]));
    observations.forEach(o => merged.set(o.date, o));
    const result = buildMonthlyRates([...merged.values()], asOf);
    if (result.latestRateDate < snapshot.observations.at(-1).date) throw new Error('ECB history regressed');
    cached = { ...result, fetchedAt: now.toISOString(), fallback: false };
    savedObservations = [...merged.values()];
    cachedAt = Date.now();
    return cached;
  } catch (error) {
    const fallback = buildMonthlyRates(savedObservations, asOf);
    return { ...fallback, fallback: true, warning: `Live Steam exchange-rate refresh unavailable; using saved ECB rates through ${fallback.latestRateDate}.`, error: error.message };
  }
}
