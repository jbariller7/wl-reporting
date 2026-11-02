(function attachSteamFx(global) {
  const FEE_RATE = 0.005;
  const SOURCE = 'https://www.ecb.europa.eu/stats/eurofxref/eurofxref-hist.xml';

  function parseEcbUsd(xml) {
    const observations = [];
    for (const match of String(xml).matchAll(/<Cube\b[^>]*\btime=['"](\d{4}-\d{2}-\d{2})['"][^>]*>([\s\S]*?)<\/Cube>/g)) {
      const usd = match[2].match(/<Cube\b[^>]*currency=['"]USD['"][^>]*rate=['"]([\d.]+)['"]/);
      if (usd && match[1] >= '2026-01-01' && Number(usd[1]) > 0) {
        observations.push({ date: match[1], usdPerEur: Number(usd[1]) });
      }
    }
    if (!observations.length) throw new Error('No valid ECB USD reference rates');
    return observations.sort((a, b) => a.date.localeCompare(b.date));
  }

  function buildMonthlyRates(observations, asOf = new Date().toISOString().slice(0, 10)) {
    const available = observations.filter(o => o.date <= asOf && Number.isFinite(o.usdPerEur) && o.usdPerEur > 0)
      .sort((a, b) => a.date.localeCompare(b.date));
    if (!available.length) throw new Error('No available Steam exchange rates');
    const months = {};
    const finalYear = Math.max(2026, Number(asOf.slice(0, 4)));
    for (let year = 2026; year <= finalYear; year++) {
      for (let month = 0; month < 12; month++) {
        const salesMonth = `${year}-${String(month + 1).padStart(2, '0')}`;
        // Payment is in the following month. February uses its final calendar
        // day when the requested 29th does not exist, never March 1st.
        const lastPaymentDay = new Date(Date.UTC(year, month + 2, 0)).getUTCDate();
        const paymentDate = new Date(Date.UTC(year, month + 1, Math.min(29, lastPaymentDay))).toISOString().slice(0, 10);
        const cutoff = paymentDate < asOf ? paymentDate : asOf;
        const observation = available.findLast(o => o.date <= cutoff);
        if (!observation) throw new Error(`Missing ECB reference rate for ${salesMonth}`);
        const ageAtPayment = (Date.parse(paymentDate) - Date.parse(observation.date)) / 86400000;
        const provisional = paymentDate > asOf || ageAtPayment > 7 || available.at(-1).date < paymentDate && asOf === paymentDate;
        months[salesMonth] = {
          salesMonth, paymentDate, rateDate: observation.date,
          usdPerEur: observation.usdPerEur,
          eurPerUsd: 1 / observation.usdPerEur,
          effectiveEurPerUsd: (1 - FEE_RATE) / observation.usdPerEur,
          provisional
        };
      }
    }
    return { source: SOURCE, feeRate: FEE_RATE, asOf, latestRateDate: available.at(-1).date, months };
  }

  function toEur(usd, salesDate, rates) {
    const date = String(salesDate || '').slice(0, 10);
    if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) return 0;
    // Preserve the existing valuation for periods outside this change.
    if (date < '2026-01-01') return Number(usd || 0) * 0.86;
    const rate = rates?.months?.[date.slice(0, 7)];
    if (!rate || !(rate.effectiveEurPerUsd > 0)) throw new Error(`Missing Steam exchange rate for ${date.slice(0, 7)}`);
    return Number(usd || 0) * rate.effectiveEurPerUsd;
  }

  global.WLSteamFx = Object.freeze({ FEE_RATE, SOURCE, parseEcbUsd, buildMonthlyRates, toEur });
})(typeof window === 'undefined' ? globalThis : window);
