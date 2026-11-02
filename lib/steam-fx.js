import '../public/steam-fx.js';
import snapshot from '../public/steam-fx-snapshot.json' with { type: 'json' };
import twdSnapshot from '../public/twd-fx-snapshot.json' with { type: 'json' };

const { SOURCE, TWD_SOURCE, parseEcbRates, parseCbcTwd, addTwdRates, buildPlatformRates } = globalThis.WLSteamFx;
let cached;
let cachedAt = 0;
let savedObservations = snapshot.observations;
let savedTwd = twdSnapshot.observations;

export async function getSteamExchangeRates({ fetchImpl = fetch, now = new Date() } = {}) {
  const asOf = now.toISOString().slice(0, 10);
  if (cached && cached.asOf === asOf && Date.now() - cachedAt < 3600000) return cached;
  const read = async (url, parse) => {
    const response = await fetchImpl(url, { signal: AbortSignal.timeout(12000) });
    if (!response.ok) throw new Error(`Exchange-rate source HTTP ${response.status}`);
    return parse(await response.text());
  };
  const [ecb, twd] = await Promise.allSettled([
    read(SOURCE, parseEcbRates),
    read(TWD_SOURCE, text => parseCbcTwd(JSON.parse(text)))
  ]);
  const warnings = [];
  if (ecb.status === 'fulfilled') {
    const merged = new Map(savedObservations.map(o => [o.date, o]));
    ecb.value.forEach(o => merged.set(o.date, o));
    savedObservations = [...merged.values()].sort((a, b) => a.date.localeCompare(b.date));
  } else warnings.push('ECB refresh unavailable; using saved reference rates.');
  if (twd.status === 'fulfilled') {
    const merged = new Map(savedTwd.map(o => [o.date, o]));
    twd.value.forEach(o => merged.set(o.date, o));
    savedTwd = [...merged.values()].sort((a, b) => a.date.localeCompare(b.date));
  } else warnings.push('Taiwan-dollar refresh unavailable; using saved CBC reference rates.');
  savedObservations = addTwdRates(savedObservations, savedTwd);
  const result = buildPlatformRates(savedObservations, asOf);
  cached = { ...result, fetchedAt: now.toISOString(), fallback: warnings.length > 0,
    ...(warnings.length ? { warning: warnings.join(' ') } : {}) };
  cachedAt = Date.now();
  return cached;
}
