(function attachMetaOptimizer(global) {
  const DAY_MS = 24 * 60 * 60 * 1000;

  function number(value) {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : 0;
  }

  function normalizeDate(value) {
    const text = String(value || "").trim();
    const dayFirst = text.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})/);
    if (dayFirst) return `${dayFirst[3]}-${dayFirst[2].padStart(2, "0")}-${dayFirst[1].padStart(2, "0")}`;
    return text.slice(0, 10);
  }

  function shiftDate(iso, days) {
    const date = new Date(`${iso}T12:00:00Z`);
    date.setUTCDate(date.getUTCDate() + days);
    return date.toISOString().slice(0, 10);
  }

  function yesterdayIso(now = new Date()) {
    const localNoon = new Date(now.getFullYear(), now.getMonth(), now.getDate(), 12);
    localNoon.setDate(localNoon.getDate() - 1);
    return `${localNoon.getFullYear()}-${String(localNoon.getMonth() + 1).padStart(2, "0")}-${String(localNoon.getDate()).padStart(2, "0")}`;
  }

  function parseRaw(row) {
    try {
      return typeof row?.raw === "string" ? JSON.parse(row.raw) : (row?.raw || {});
    } catch (_) {
      return {};
    }
  }

  function actionValue(row, actionType) {
    const raw = parseRaw(row);
    const actions = Array.isArray(raw.actions) ? raw.actions : [];
    return number(actions.find((action) => action.action_type === actionType)?.value);
  }

  function inWindow(date, start, end) {
    const normalized = normalizeDate(date);
    return normalized && normalized >= start && normalized <= end;
  }

  function emptyMetrics() {
    return {
      spend: 0,
      impressions: 0,
      clicks: 0,
      linkClicks: 0,
      landingPageViews: 0,
      registrations: 0,
      metaPurchases: 0,
      reach: 0,
      weightedFrequency: 0,
      netRevenue: 0,
      subscribers: 0,
      demoStarts: 0,
      demoFinishes: 0,
      orders: 0,
      days: new Set()
    };
  }

  function addMeta(metrics, row) {
    const raw = parseRaw(row);
    const impressions = number(row.impressions ?? raw.impressions);
    const frequency = number(row.frequency ?? raw.frequency);
    metrics.spend += number(row.spend ?? raw.spend);
    metrics.impressions += impressions;
    metrics.clicks += number(row.clicks ?? raw.clicks);
    metrics.linkClicks += number(row.link_clicks) || actionValue(row, "link_click") || number(raw.inline_link_clicks);
    metrics.landingPageViews += number(row.landing_page_views) || actionValue(row, "landing_page_view");
    metrics.registrations += number(row.registrations) || actionValue(row, "complete_registration");
    metrics.metaPurchases += number(row.purchases) || actionValue(row, "purchase");
    metrics.reach += number(row.reach ?? raw.reach);
    metrics.weightedFrequency += frequency * impressions;
    const date = normalizeDate(row.date ?? raw.date_start);
    if (date) metrics.days.add(date);
  }

  function addMarket(metrics, row) {
    metrics.netRevenue += number(row.netRevenue);
    metrics.subscribers += number(row.subscribers);
    metrics.demoStarts += number(row.demoStarts);
    metrics.demoFinishes += number(row.demoFinishes);
    metrics.orders += number(row.orders);
    const date = normalizeDate(row.date);
    if (date) metrics.days.add(date);
  }

  function finalize(metrics) {
    const spend = metrics.spend;
    const impressions = metrics.impressions;
    const links = metrics.linkClicks;
    const lpv = metrics.landingPageViews;
    const subscribers = metrics.subscribers;
    return {
      ...metrics,
      days: metrics.days.size,
      cpm: impressions > 0 ? spend / impressions * 1000 : 0,
      broadCtr: impressions > 0 ? metrics.clicks / impressions * 100 : 0,
      linkCtr: impressions > 0 ? links / impressions * 100 : 0,
      linkCpc: links > 0 ? spend / links : null,
      landingPageRate: links > 0 ? lpv / links * 100 : 0,
      costPerLandingPageView: lpv > 0 ? spend / lpv : null,
      costPerRegistration: metrics.registrations > 0 ? spend / metrics.registrations : null,
      marketRoas: spend > 0 ? metrics.netRevenue / spend : null,
      costPerSubscriber: subscribers > 0 ? spend / subscribers : null,
      demoFinishRate: metrics.demoStarts > 0 ? metrics.demoFinishes / metrics.demoStarts * 100 : 0,
      averageFrequency: impressions > 0 ? metrics.weightedFrequency / impressions : 0
    };
  }

  function metricsForRows(rows, start, end, add) {
    const metrics = emptyMetrics();
    for (const row of rows) {
      if (inWindow(row.date, start, end)) add(metrics, row);
    }
    return finalize(metrics);
  }

  function currentConfigByAdset(rows, reportEnd) {
    const configs = new Map();
    for (const row of rows || []) {
      const id = String(row.adset_id || row.id || "");
      const snapshotDate = normalizeDate(row.snapshot_date || row.date);
      if (!id || !snapshotDate) continue;
      const existing = configs.get(id);
      if (!existing || normalizeDate(existing.snapshot_date) <= snapshotDate) configs.set(id, row);
    }
    return configs;
  }

  function lowerConfidence(confidence) {
    if (confidence === "High") return "Medium";
    return "Low";
  }

  function confidenceFor(adset, minSpend) {
    let confidence = "Low";
    if (adset.metrics28.spend >= minSpend * 3 && adset.metrics28.linkClicks >= 100) confidence = "High";
    else if (adset.metrics28.spend >= minSpend && adset.metrics28.linkClicks >= 30) confidence = "Medium";
    if (adset.countries.length > 1 || adset.maxCountryOverlap > 1) confidence = lowerConfidence(confidence);
    if (!adset.config) confidence = lowerConfidence(confidence);
    return confidence;
  }

  function minorCurrency(value, currency) {
    if (value === null || value === undefined || value === "") return null;
    const zeroDecimal = new Set(["BIF", "CLP", "DJF", "GNF", "JPY", "KMF", "KRW", "MGA", "PYG", "RWF", "UGX", "VND", "VUV", "XAF", "XOF", "XPF"]);
    return number(value) / (zeroDecimal.has(String(currency || "").toUpperCase()) ? 1 : 100);
  }

  function eventMatchesAdset(event, adset, recentStart, reportEnd) {
    const date = normalizeDate(event.date);
    if (!date || date < recentStart || date > reportEnd) return false;
    const target = String(event.target || "ALL").trim().toUpperCase();
    if (target === "ALL" || target === String(adset.name).toUpperCase()) return true;
    return adset.countries.includes(target);
  }

  function decideAdset(adset, options, events, reportEnd) {
    const {
      targetRoas,
      targetLinkCtr,
      minSpend,
      maxBudgetChangePct,
      minLandingPageRate
    } = options;
    const m7 = adset.metrics7;
    const m28 = adset.metrics28;
    const p7 = adset.previous7;
    const market7 = adset.marketRoas7;
    const market28 = adset.marketRoas28;
    const config = adset.config || {};
    const recentStart = shiftDate(reportEnd, -2);
    const recentEvents = (events || []).filter((event) => eventMatchesAdset(event, adset, recentStart, reportEnd));
    const configUpdatedDate = normalizeDate(config.updated_time);
    const recentConfigChange = configUpdatedDate && configUpdatedDate >= recentStart;
    const effectiveStatus = String(config.effective_status || config.status || "").toUpperCase();
    const currency = config.account_currency || "EUR";
    const dailyBudget = minorCurrency(config.daily_budget, currency);
    const bidAmount = minorCurrency(config.bid_amount, currency);
    const deliveryRatio = dailyBudget > 0 && m7.days > 0 ? m7.spend / (dailyBudget * m7.days) : null;
    const linkCtrDecline = p7.linkCtr > 0 ? (m7.linkCtr - p7.linkCtr) / p7.linkCtr * 100 : 0;
    const frequency = adset.creative7.averageFrequency || m7.averageFrequency;
    const fatigue = frequency >= 2.5 && linkCtrDecline <= -20 && m7.impressions >= 3000;
    const enoughData = m28.spend >= minSpend && m28.linkClicks >= 20 && m28.days >= 3;
    const strongTraffic = m7.linkCtr >= targetLinkCtr && m7.landingPageRate >= minLandingPageRate;
    const weakTraffic = m7.impressions >= 3000 && (m7.linkCtr < targetLinkCtr * 0.7 || (m7.linkClicks >= 30 && m7.landingPageRate < minLandingPageRate * 0.7));
    const strongMarket = market7 !== null && market28 !== null && market7 >= targetRoas && market28 >= 1;
    const weakMarket = market28 !== null && market28 < 0.75;
    const marginalMarket = market28 !== null && market28 >= 0.75 && market28 < targetRoas;
    let action = "MAINTAIN";
    let adjustment = "No change";
    const evidence = [];

    if (effectiveStatus && effectiveStatus !== "ACTIVE") {
      action = "PAUSED / NO ACTION";
      evidence.push(`Current Meta status: ${effectiveStatus}`);
    } else if (recentEvents.length || recentConfigChange) {
      action = "WAIT / RECENT CHANGE";
      if (recentEvents.length) evidence.push(`Recent logged change: ${recentEvents.map((event) => event.text).filter(Boolean).join("; ") || "event"}`);
      if (recentConfigChange) evidence.push(`Meta reports an ad-set update on ${configUpdatedDate}`);
    } else if (!enoughData) {
      action = "COLLECT DATA";
      evidence.push(`Only €${m28.spend.toFixed(0)} spend, ${m28.linkClicks} link clicks, ${m28.days} active days in 28d`);
    } else if (m7.spend < minSpend * 0.25 && m7.linkClicks < 20) {
      action = "COLLECT DATA";
      evidence.push(`Very little recent delivery: €${m7.spend.toFixed(0)} and ${m7.linkClicks} link clicks in 7d`);
    } else if (fatigue) {
      action = "REFRESH CREATIVES";
      evidence.push(`Frequency ${frequency.toFixed(2)} and 7d link CTR ${Math.abs(linkCtrDecline).toFixed(0)}% below previous 7d`);
    } else if (weakTraffic) {
      action = "TEST NEW CREATIVES";
      evidence.push(`7d link CTR ${m7.linkCtr.toFixed(2)}%, landing-page rate ${m7.landingPageRate.toFixed(1)}%`);
    } else if (weakMarket && m28.spend >= minSpend * 2 && !strongTraffic) {
      action = "STOP";
      adjustment = "Pause after review";
      evidence.push(`Shared 28d market ROAS ${market28.toFixed(2)}x with weak ad-set traffic`);
    } else if (strongMarket && strongTraffic) {
      if (deliveryRatio !== null && deliveryRatio < 0.65 && bidAmount > 0) {
        action = "INCREASE CPA";
        adjustment = `+${Math.min(10, maxBudgetChangePct)}% cost cap`;
        evidence.push(`Strong market and traffic, but only ${(deliveryRatio * 100).toFixed(0)}% of available daily budget delivered`);
      } else if (deliveryRatio !== null && deliveryRatio < 0.5 && bidAmount === 0) {
        action = "DUPLICATE TEST";
        adjustment = "Small controlled duplicate";
        evidence.push(`Strong market and traffic with ${(deliveryRatio * 100).toFixed(0)}% budget delivery and no visible cost cap`);
      } else {
        action = "INCREASE BUDGET";
        adjustment = `+${maxBudgetChangePct}%`;
        evidence.push(`Shared market ROAS ${market7.toFixed(2)}x (7d), link CTR ${m7.linkCtr.toFixed(2)}%`);
      }
    } else if (marginalMarket && bidAmount > 0) {
      action = "LOWER CPA";
      adjustment = `-${Math.min(10, maxBudgetChangePct)}% cost cap`;
      evidence.push(`Shared 28d market ROAS ${market28.toFixed(2)}x is below target ${targetRoas.toFixed(2)}x`);
    } else if (marginalMarket && m28.spend >= minSpend * 2) {
      action = "REDUCE BUDGET";
      adjustment = `-${Math.min(10, maxBudgetChangePct)}%`;
      evidence.push(`Shared 28d market ROAS ${market28.toFixed(2)}x is below target`);
    } else {
      evidence.push(`Shared 28d market ROAS ${market28 === null ? "n/a" : `${market28.toFixed(2)}x`}, link CTR ${m7.linkCtr.toFixed(2)}%`);
    }

    if (adset.maxCountryOverlap > 1) evidence.push(`Revenue is shared market evidence; up to ${adset.maxCountryOverlap} ad sets overlap a country`);
    if (adset.countries.length > 1) evidence.push(`${adset.countries.length} delivered countries`);

    let confidence = confidenceFor(adset, minSpend);
    if (recentEvents.length || recentConfigChange || market28 === null) confidence = "Low";
    return { action, adjustment, confidence, evidence, dailyBudget, bidAmount, deliveryRatio, linkCtrDecline, frequency };
  }

  function buildOptimizerReport(input = {}) {
    const metaRows = input.metaRows || [];
    const marketRows = input.marketRows || [];
    const adsetConfigRows = input.adsetConfigRows || [];
    const adRows = input.adRows || [];
    const events = input.events || [];
    const requestedEnd = normalizeDate(input.endDate) || yesterdayIso(input.now);
    const reportEnd = requestedEnd > yesterdayIso(input.now) ? yesterdayIso(input.now) : requestedEnd;
    const requestedStart = normalizeDate(input.startDate) || "2000-01-01";
    const start7 = [shiftDate(reportEnd, -6), requestedStart].sort().at(-1);
    const start28 = [shiftDate(reportEnd, -27), requestedStart].sort().at(-1);
    const previous7Start = [shiftDate(reportEnd, -13), requestedStart].sort().at(-1);
    const previous7End = shiftDate(reportEnd, -7);
    const options = {
      targetRoas: number(input.options?.targetRoas) || 2,
      targetCps: number(input.options?.targetCps) || 2,
      targetLinkCtr: number(input.options?.targetLinkCtr) || 1,
      minLandingPageRate: number(input.options?.minLandingPageRate) || 45,
      minSpend: number(input.options?.minSpend) || 20,
      maxBudgetChangePct: Math.min(25, Math.max(5, number(input.options?.maxBudgetChangePct) || 15))
    };

    const filteredMeta = metaRows.filter((row) => inWindow(row.date, start28, reportEnd));
    const filteredMarket = marketRows.filter((row) => inWindow(row.date, start28, reportEnd));
    const countryMeta = new Map();
    const countryMarket = new Map();
    const countryAdsets = new Map();
    const adsetMeta = new Map();

    for (const row of filteredMeta) {
      const country = String(row.country || "UNKNOWN").toUpperCase();
      const adsetId = String(row.adset_id || row.adset_name || "UNKNOWN");
      if (!countryMeta.has(country)) countryMeta.set(country, []);
      if (!adsetMeta.has(adsetId)) adsetMeta.set(adsetId, []);
      if (!countryAdsets.has(country)) countryAdsets.set(country, new Set());
      countryMeta.get(country).push(row);
      adsetMeta.get(adsetId).push(row);
      countryAdsets.get(country).add(adsetId);
    }
    for (const row of filteredMarket) {
      const country = String(row.country || "UNKNOWN").toUpperCase();
      if (!countryMarket.has(country)) countryMarket.set(country, []);
      countryMarket.get(country).push(row);
    }

    const markets = [];
    const allCountries = new Set([...countryMeta.keys(), ...countryMarket.keys()]);
    for (const country of allCountries) {
      const meta = countryMeta.get(country) || [];
      const market = countryMarket.get(country) || [];
      const combine = (start, end) => {
        const metrics = emptyMetrics();
        meta.filter((row) => inWindow(row.date, start, end)).forEach((row) => addMeta(metrics, row));
        market.filter((row) => inWindow(row.date, start, end)).forEach((row) => addMarket(metrics, row));
        return finalize(metrics);
      };
      const metrics7 = combine(start7, reportEnd);
      const metrics28 = combine(start28, reportEnd);
      const overlap = countryAdsets.get(country)?.size || 0;
      let action = "COLLECT DATA";
      if (metrics28.spend >= options.minSpend) {
        if (metrics28.marketRoas !== null && metrics28.marketRoas >= options.targetRoas) action = "GROW MARKET ENVELOPE";
        else if (metrics28.marketRoas !== null && metrics28.marketRoas >= 1) action = "MAINTAIN MARKET";
        else action = "REDUCE MARKET ENVELOPE";
      } else if (metrics28.netRevenue >= 30 || metrics28.orders >= 2 || metrics28.subscribers >= 10 || metrics28.demoStarts >= 10) {
        action = "TEST MARKET";
      }
      markets.push({ country, metrics7, metrics28, overlap, action });
    }
    markets.sort((a, b) => b.metrics28.spend - a.metrics28.spend || b.metrics28.netRevenue - a.metrics28.netRevenue);
    const marketByCountry = new Map(markets.map((market) => [market.country, market]));
    const configs = currentConfigByAdset(adsetConfigRows, reportEnd);

    const creativeByAdset = new Map();
    for (const row of adRows.filter((item) => inWindow(item.date, start28, reportEnd))) {
      const id = String(row.adset_id || "");
      if (!id) continue;
      if (!creativeByAdset.has(id)) creativeByAdset.set(id, []);
      creativeByAdset.get(id).push(row);
    }

    const adsets = [];
    for (const [adsetId, rows] of adsetMeta) {
      const name = rows.find((row) => row.adset_name)?.adset_name || adsetId;
      const countries = [...new Set(rows.map((row) => String(row.country || "UNKNOWN").toUpperCase()))].sort();
      const weightedMarketRoas = (window, start, end) => {
        const spendByCountry = new Map();
        for (const row of rows) {
          if (!inWindow(row.date, start, end)) continue;
          const country = String(row.country || "UNKNOWN").toUpperCase();
          spendByCountry.set(country, (spendByCountry.get(country) || 0) + number(row.spend));
        }
        let weighted = 0;
        let weight = 0;
        for (const [country, spend] of spendByCountry) {
          const roas = marketByCountry.get(country)?.[window]?.marketRoas;
          if (roas !== null && roas !== undefined && spend > 0) {
            weighted += roas * spend;
            weight += spend;
          }
        }
        return weight > 0 ? weighted / weight : null;
      };
      const creativeRows = creativeByAdset.get(adsetId) || [];
      const creative7 = metricsForRows(creativeRows, start7, reportEnd, addMeta);
      const creative28 = metricsForRows(creativeRows, start28, reportEnd, addMeta);
      const metrics7 = metricsForRows(rows, start7, reportEnd, addMeta);
      const metrics28 = metricsForRows(rows, start28, reportEnd, addMeta);
      const previous7 = metricsForRows(rows, previous7Start, previous7End, addMeta);
      const adset = {
        id: adsetId,
        name,
        countries,
        metrics7,
        metrics28,
        previous7,
        creative7,
        creative28,
        creativeCount: new Set(creativeRows.map((row) => row.ad_id).filter(Boolean)).size,
        marketRoas7: weightedMarketRoas("metrics7", start7, reportEnd),
        marketRoas28: weightedMarketRoas("metrics28", start28, reportEnd),
        maxCountryOverlap: Math.max(0, ...countries.map((country) => countryAdsets.get(country)?.size || 0)),
        config: configs.get(adsetId) || null
      };
      adset.recommendation = decideAdset(adset, options, events, reportEnd);
      adsets.push(adset);
    }
    const actionPriority = new Map(["STOP", "REFRESH CREATIVES", "TEST NEW CREATIVES", "LOWER CPA", "REDUCE BUDGET", "INCREASE CPA", "INCREASE BUDGET", "DUPLICATE TEST", "WAIT / RECENT CHANGE", "COLLECT DATA", "PAUSED / NO ACTION", "MAINTAIN"].map((action, index) => [action, index]));
    adsets.sort((a, b) => (actionPriority.get(a.recommendation.action) ?? 99) - (actionPriority.get(b.recommendation.action) ?? 99) || b.metrics7.spend - a.metrics7.spend);

    const activeDedicatedCountries = new Set(adsets.filter((adset) => adset.countries.length === 1 && adset.metrics28.spend >= options.minSpend / 2).map((adset) => adset.countries[0]));
    const eligibleCountries = new Set((input.eligibleCountries || []).map((country) => String(country).toUpperCase()));
    const opportunities = markets.filter((market) => {
      if (!market.country || market.country === "UNKNOWN" || activeDedicatedCountries.has(market.country)) return false;
      const metrics = market.metrics28;
      const signal = metrics.netRevenue >= 30 || metrics.orders >= 2 || metrics.subscribers >= 10 || metrics.demoStarts >= 10;
      return signal && (metrics.spend < options.minSpend || (metrics.marketRoas !== null && metrics.marketRoas >= options.targetRoas));
    }).map((market) => ({
      country: market.country,
      action: eligibleCountries.size > 0 && !eligibleCountries.has(market.country)
        ? "REVIEW ELIGIBILITY"
        : (market.metrics28.spend < options.minSpend ? "START COUNTRY TEST" : "CREATE DEDICATED TEST"),
      confidence: market.metrics28.orders >= 5 || market.metrics28.netRevenue >= 100 ? "Medium" : "Low",
      spend28: market.metrics28.spend,
      revenue28: market.metrics28.netRevenue,
      orders28: market.metrics28.orders,
      subscribers28: market.metrics28.subscribers,
      demos28: market.metrics28.demoStarts,
      sharedRoas28: market.metrics28.marketRoas
    })).sort((a, b) => b.revenue28 - a.revenue28 || b.subscribers28 - a.subscribers28);

    return {
      generatedAt: (input.now || new Date()).toISOString(),
      reportStart: requestedStart,
      reportEnd,
      windows: { start7, start28, previous7Start, previous7End },
      options,
      caveat: "Store revenue is country-level shared market evidence and is never assigned to an individual ad set.",
      dataFreshness: input.dataFreshness || {},
      adsets,
      markets,
      opportunities
    };
  }

  function md(value) {
    return String(value ?? "").replace(/\|/g, "\\|").replace(/\r?\n/g, " ");
  }

  function money(value) {
    return Number.isFinite(value) ? `€${value.toFixed(2)}` : "n/a";
  }

  function ratio(value) {
    return value === null || value === undefined ? "n/a" : `${value.toFixed(2)}x shared`;
  }

  function toMarkdown(report) {
    let output = `## WonderLang Meta Ads Optimizer Packet\n\n`;
    output += `- Reporting cutoff: ${report.reportEnd} (current day excluded)\n`;
    output += `- Windows: 7 days from ${report.windows.start7}; 28 days from ${report.windows.start28}\n`;
    output += `- Target market ROAS: ${report.options.targetRoas.toFixed(2)}x\n`;
    output += `- Target link CTR: ${report.options.targetLinkCtr.toFixed(2)}%\n`;
    output += `- Maximum suggested budget change: ${report.options.maxBudgetChangePct}%\n`;
    output += `- Attribution rule: ${report.caveat}\n\n`;
    const freshness = Object.entries(report.dataFreshness || {}).filter(([, value]) => value).map(([source, value]) => `${source}: ${value}`).join(", ");
    if (freshness) output += `- Data freshness: ${freshness}\n\n`;
    output += `### Requested analysis\nFor every ad set, validate or revise the rule-based action. Return one action, confidence, exact reason, proposed amount, and next review date. Keep country-market economics separate from exact ad-set delivery. Also review new-market opportunities.\n\n`;
    output += `### Ad-set recommendations\n| Ad Set | Countries | Action | Confidence | Adjustment | Spend 7d | Link CTR 7d | LPV rate 7d | Market ROAS 7d | Market ROAS 28d | Daily budget | CPA/cost cap | Evidence |\n|---|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|---|\n`;
    for (const adset of report.adsets) {
      const recommendation = adset.recommendation;
      output += `| ${md(adset.name)} | ${md(adset.countries.join(", "))} | ${recommendation.action} | ${recommendation.confidence} | ${md(recommendation.adjustment)} | ${money(adset.metrics7.spend)} | ${adset.metrics7.linkCtr.toFixed(2)}% | ${adset.metrics7.landingPageRate.toFixed(1)}% | ${ratio(adset.marketRoas7)} | ${ratio(adset.marketRoas28)} | ${money(recommendation.dailyBudget)} | ${money(recommendation.bidAmount)} | ${md(recommendation.evidence.join("; "))} |\n`;
    }
    output += `\n### Country-market signals\n| Country | Action | Spend 7d | Net revenue 7d | Shared ROAS 7d | Spend 28d | Net revenue 28d | Shared ROAS 28d | Orders 28d | Subscribers 28d | Overlapping ad sets |\n|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n`;
    for (const market of report.markets) {
      output += `| ${market.country} | ${market.action} | ${money(market.metrics7.spend)} | ${money(market.metrics7.netRevenue)} | ${ratio(market.metrics7.marketRoas)} | ${money(market.metrics28.spend)} | ${money(market.metrics28.netRevenue)} | ${ratio(market.metrics28.marketRoas)} | ${market.metrics28.orders} | ${market.metrics28.subscribers} | ${market.overlap} |\n`;
    }
    output += `\n### New-market opportunities\n| Country | Suggested test | Confidence | Spend 28d | Revenue 28d | Orders | Subscribers | Demos |\n|---|---|---|---:|---:|---:|---:|---:|\n`;
    for (const item of report.opportunities) {
      output += `| ${item.country} | ${item.action} | ${item.confidence} | ${money(item.spend28)} | ${money(item.revenue28)} | ${item.orders28} | ${item.subscribers28} | ${item.demos28} |\n`;
    }
    return output;
  }

  global.WLMetaOptimizer = Object.freeze({ buildOptimizerReport, toMarkdown, actionValue, normalizeDate, yesterdayIso });
})(typeof window === "undefined" ? globalThis : window);
