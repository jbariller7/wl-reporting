(function attachSteamFx(global) {
  const FEE_RATE = 0.005;
  const SOURCE = 'https://www.ecb.europa.eu/stats/eurofxref/eurofxref-hist.xml';

  const TWD_SOURCE = 'https://cpx.cbc.gov.tw/api/OpenData/FTDOpenData_Day';
  function parseEcbRates(xml) {
    const observations = [];
    for (const match of String(xml).matchAll(/<Cube\b[^>]*\btime=['"](\d{4}-\d{2}-\d{2})['"][^>]*>([\s\S]*?)<\/Cube>/g)) {
      const currencyPerEur = { EUR: 1 };
      for (const currency of match[2].matchAll(/<Cube\b[^>]*currency=['"]([A-Z]{3})['"][^>]*rate=['"]([\d.]+)['"]/g)) {
        if (Number(currency[2]) > 0) currencyPerEur[currency[1]] = Number(currency[2]);
      }
      if (match[1] >= '2026-01-01' && currencyPerEur.USD) observations.push({ date: match[1], usdPerEur: currencyPerEur.USD, currencyPerEur });
    }
    if (!observations.length) throw new Error('No valid ECB USD reference rates');
    return observations.sort((a, b) => a.date.localeCompare(b.date));
  }
  function parseEcbUsd(xml) {
    return parseEcbRates(xml).map(({ date, usdPerEur }) => ({ date, usdPerEur }));
  }
  function parseCbcTwd(payload) {
    if (!Array.isArray(payload)) throw new Error('Invalid CBC exchange-rate response');
    const rows = payload.map(row => ({ date: String(row['日期']).replace(/^(\d{4})(\d{2})(\d{2})$/, '$1-$2-$3'), twdPerUsd: Number(row.NTD_USD) }))
      .filter(row => /^\d{4}-\d{2}-\d{2}$/.test(row.date) && row.date >= '2026-01-01' && row.twdPerUsd > 0).sort((a, b) => a.date.localeCompare(b.date));
    if (!rows.length) throw new Error('No valid CBC TWD reference rates');
    return rows;
  }
  function addTwdRates(observations, twdObservations) {
    return observations.map(o => {
      const twd = twdObservations.findLast(r => r.date <= o.date);
      if (!twd || Date.parse(o.date) - Date.parse(twd.date) > 10 * 86400000) return o;
      return { ...o, currencyPerEur: { ...o.currencyPerEur, TWD: o.usdPerEur * twd.twdPerUsd }, currencyRateDates: { ...o.currencyRateDates, TWD: twd.date } };
    });
  }
  function buildMonthlyRates(observations, asOf = new Date().toISOString().slice(0, 10), offsetMonths = 1) {
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
        const lastPaymentDay = new Date(Date.UTC(year, month + offsetMonths + 1, 0)).getUTCDate();
        const paymentDate = new Date(Date.UTC(year, month + offsetMonths, Math.min(29, lastPaymentDay))).toISOString().slice(0, 10);
        const currentMonth = salesMonth === asOf.slice(0, 7);
        const cutoff = currentMonth ? asOf : paymentDate < asOf ? paymentDate : asOf;
        const observation = available.findLast(o => o.date <= cutoff);
        if (!observation) throw new Error(`Missing ECB reference rate for ${salesMonth}`);
        const ageAtPayment = (Date.parse(paymentDate) - Date.parse(observation.date)) / 86400000;
        const provisional = currentMonth || paymentDate > asOf || ageAtPayment > 7 || available.at(-1).date < paymentDate && asOf === paymentDate;
        months[salesMonth] = {
          salesMonth, paymentDate, rateDate: observation.date,
          usdPerEur: observation.usdPerEur,
          eurPerUsd: 1 / observation.usdPerEur,
          effectiveEurPerUsd: (1 - FEE_RATE) / observation.usdPerEur,
          currencyRates: Object.fromEntries(Object.entries(observation.currencyPerEur || { USD: observation.usdPerEur, EUR: 1 }).map(([currency, perEur]) => [currency, {
            eurPerUnit: 1 / perEur,
            effectiveEurPerUnit: (currency === 'EUR' ? 1 : 1 - FEE_RATE) / perEur,
            rateDate: observation.currencyRateDates?.[currency] || observation.date
          }])),
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

  function buildPlatformRates(observations, asOf) {
    const rates = buildMonthlyRates(observations, asOf, 1);
    return { ...rates, paypalMonths: buildMonthlyRates(observations, asOf, 0).months,
      currencies: [...new Set(observations.flatMap(o => Object.keys(o.currencyPerEur || { USD: 1, EUR: 1 })))].sort(),
      sources: { ecb: SOURCE, twd: TWD_SOURCE } };
  }
  function convertCurrency(amount, currency, salesDate, rates, platform) {
    const value = Number(amount || 0), code = String(currency || 'EUR').toUpperCase();
    if (code === 'EUR') return value;
    const month = String(salesDate).slice(0, 7);
    const entry = (platform === 'paypal' ? rates?.paypalMonths : rates?.months)?.[month];
    const rate = entry?.currencyRates?.[code]?.effectiveEurPerUnit;
    if (!(rate > 0)) throw new Error(`Missing monthly ${code} exchange rate for ${month}`);
    return value * rate;
  }
  global.WLSteamFx = Object.freeze({ FEE_RATE, SOURCE, TWD_SOURCE, parseEcbUsd, parseEcbRates, parseCbcTwd, addTwdRates, buildMonthlyRates, buildPlatformRates, toEur, convertCurrency });
})(typeof window === 'undefined' ? globalThis : window);
