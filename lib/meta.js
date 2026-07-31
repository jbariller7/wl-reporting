import { upsert, clearDateRange, retypeSheetColumns } from "./db.js";

const META_API_VERSION = "v25.0";

export function metaActionValue(actions, types) {
  for (const type of types) {
    const match = (actions || []).find((action) => action.action_type === type);
    if (match) return Number(match.value || 0);
  }
  return 0;
}

async function fetchMetaPages(initialUrl, token, addLog, label) {
  let nextUrl = initialUrl instanceof URL ? initialUrl.toString() : String(initialUrl);
  const rows = [];
  const seenUrls = new Set();
  let pages = 0;

  while (nextUrl) {
    if (seenUrls.has(nextUrl)) throw new Error(`${label} returned a repeated pagination URL`);
    seenUrls.add(nextUrl);
    pages++;
    if (pages > 1000) throw new Error(`${label} pagination exceeded 1000 pages`);

    const resp = await fetch(nextUrl, { headers: { Authorization: `Bearer ${token}` } });
    const contentType = resp.headers.get("content-type") || "";
    if (!contentType.includes("application/json")) throw new Error(`${label} returned a non-JSON response`);
    const json = await resp.json();
    if (!resp.ok) throw new Error(json.error?.message || `${label} API Error ${resp.status}`);
    rows.push(...(json.data || []));
    nextUrl = json.paging?.next || null;
    if (nextUrl && pages % 10 === 0) addLog(`${label}: fetched ${pages} pages...`);
  }

  return rows;
}

function createInsightsUrl(accountRef, { sinceUtc, untilUtc, level, fields, breakdowns }) {
  const url = new URL(`https://graph.facebook.com/${META_API_VERSION}/${accountRef}/insights`);
  url.searchParams.set("time_increment", "1");
  url.searchParams.set("time_range", JSON.stringify({ since: sinceUtc.slice(0, 10), until: untilUtc.slice(0, 10) }));
  url.searchParams.set("level", level);
  url.searchParams.set("fields", fields.join(","));
  if (breakdowns) url.searchParams.set("breakdowns", breakdowns);
  url.searchParams.set("limit", "500");
  return url;
}

async function fetchInsightsWithFallback({ accountRef, range, level, baseFields, richFields, breakdowns, token, addLog, label }) {
  try {
    return await fetchMetaPages(createInsightsUrl(accountRef, { ...range, level, fields: richFields, breakdowns }), token, addLog, label);
  } catch (richError) {
    if (!shouldRetryMetaStableFields(richError)) throw richError;
    addLog(`${label}: rich metrics unavailable; retrying the stable field set: ${richError.message}`, "warn");
    return fetchMetaPages(createInsightsUrl(accountRef, { ...range, level, fields: baseFields, breakdowns }), token, addLog, label);
  }
}

export function shouldRetryMetaStableFields(error) {
  const message = String(error?.message || error || "");
  if (/time_range|since must be less than or equal to until/i.test(message)) return false;
  if (/access token|oauth|permission|not authorized/i.test(message)) return false;
  return /field|metric|breakdown|parameter|\(#100\)/i.test(message);
}

async function fetchAccountInfo(accountRef, token, addLog) {
  try {
    const url = new URL(`https://graph.facebook.com/${META_API_VERSION}/${accountRef}`);
    url.searchParams.set("fields", "currency,timezone_name,account_status");
    const resp = await fetch(url, { headers: { Authorization: `Bearer ${token}` } });
    const json = await resp.json();
    if (!resp.ok) throw new Error(json.error?.message || `HTTP ${resp.status}`);
    return json;
  } catch (error) {
    addLog(`Meta account metadata unavailable: ${error.message}`, "warn");
    return {};
  }
}

async function fetchAdsetSnapshots({ accountRef, accountId, accountInfo, token, addLog }) {
  const stableFields = [
    "id", "name", "campaign_id", "status", "effective_status", "daily_budget", "lifetime_budget",
    "bid_amount", "bid_strategy", "billing_event", "optimization_goal", "start_time", "end_time", "created_time",
    "updated_time", "targeting"
  ];
  const optionalFields = ["budget_remaining", "attribution_spec", "promoted_object"];
  const makeUrl = (fields) => {
    const url = new URL(`https://graph.facebook.com/${META_API_VERSION}/${accountRef}/adsets`);
    url.searchParams.set("fields", fields.join(","));
    url.searchParams.set("limit", "500");
    return url;
  };
  let source;
  try {
    source = await fetchMetaPages(makeUrl([...stableFields, ...optionalFields]), token, addLog, "Meta ad-set configuration");
  } catch (error) {
    addLog(`Meta optional configuration fields unavailable; retrying the stable field set: ${error.message}`, "warn");
    source = await fetchMetaPages(makeUrl(stableFields), token, addLog, "Meta ad-set configuration");
  }
  const snapshotAt = new Date().toISOString();
  const snapshotDate = snapshotAt.slice(0, 10);
  return source.map((row) => ({
    snapshot_date: snapshotDate,
    snapshot_at: snapshotAt,
    account_id: accountId,
    account_currency: accountInfo.currency || null,
    account_timezone: accountInfo.timezone_name || null,
    campaign_id: row.campaign_id || null,
    adset_id: row.id,
    adset_name: row.name || "Unknown Ad Set",
    status: row.status || null,
    effective_status: row.effective_status || null,
    daily_budget: row.daily_budget ?? null,
    lifetime_budget: row.lifetime_budget ?? null,
    budget_remaining: row.budget_remaining ?? null,
    bid_amount: row.bid_amount ?? null,
    bid_strategy: row.bid_strategy || null,
    billing_event: row.billing_event || null,
    optimization_goal: row.optimization_goal || null,
    start_time: row.start_time || null,
    end_time: row.end_time || null,
    created_time: row.created_time || null,
    updated_time: row.updated_time || null,
    targeting: row.targeting || {},
    attribution_spec: row.attribution_spec || [],
    promoted_object: row.promoted_object || {},
    raw: row
  }));
}

export async function etlMeta({ sinceUtc, untilUtc, forceRefresh, rangeWasReversed = false, includeConfiguration = true, syncPart = "all" }) {
  const token = process.env.FB_SYSTEM_USER_TOKEN;
  const configuredAccountId = process.env.FB_AD_ACCOUNT_ID;
  const logs = [];
  const addLog = (msg, type = "info") => logs.push({ time: new Date().toISOString(), msg, type });

  if (!token || !configuredAccountId) return { ok: false, msg: "Missing Meta token/account id", logs };

  const accountId = String(configuredAccountId).replace(/^act_/, "");
  const accountRef = `act_${accountId}`;
  const range = { sinceUtc, untilUtc };
  const validParts = new Set(["all", "insights", "creatives", "configuration", "repair"]);
  const syncInsights = syncPart === "all" || syncPart === "insights";
  const syncCreatives = syncPart === "all" || syncPart === "creatives";
  const syncConfiguration = includeConfiguration && (syncPart === "all" || syncPart === "configuration");
  const elapsed = (startedAt) => `${((Date.now() - startedAt) / 1000).toFixed(1)}s`;

  try {
    if (!validParts.has(syncPart)) throw new Error(`Unknown Meta sync part: ${syncPart}`);
    if (rangeWasReversed) {
      addLog(`The selected dates were reversed; using ${sinceUtc.slice(0, 10)} through ${untilUtc.slice(0, 10)}.`, "warn");
    }
    addLog(`Running Meta ${syncPart} sync for ${accountRef} from ${sinceUtc.slice(0, 10)} through ${untilUtc.slice(0, 10)}.`);
    if (syncPart === "repair") {
      const repairStarted = Date.now();
      const repairedRows = await retypeSheetColumns("meta_insights", ["date", "account_id"]);
      addLog(`Repaired Meta date/account cell types for ${repairedRows} rows in ${elapsed(repairStarted)}.`, "success");
      return { ok: true, rows: repairedRows, insightRows: repairedRows, creativeRows: 0, adsetRows: 0, logs };
    }
    let insightRows = [];
    if (syncInsights) {
      const fetchStarted = Date.now();
      const baseAdsetFields = ["date_start", "campaign_id", "campaign_name", "adset_id", "adset_name", "spend", "impressions", "clicks", "purchase_roas", "actions"];
      const sourceRows = await fetchInsightsWithFallback({
        accountRef,
        range,
        level: "adset",
        baseFields: baseAdsetFields,
        richFields: [...baseAdsetFields, "reach", "frequency", "inline_link_clicks"],
        breakdowns: "country",
        token,
        addLog,
        label: "Meta ad-set insights"
      });
      insightRows = sourceRows.map((row) => {
        const actions = row.actions || [];
        return {
          date: row.date_start,
          account_id: accountId,
          country: row.country || "Unknown",
          campaign_id: row.campaign_id,
          campaign_name: row.campaign_name || "Unknown Campaign",
          adset_id: row.adset_id,
          adset_name: row.adset_name || "Unknown Ad Set",
          impressions: Number(row.impressions || 0),
          clicks: Number(row.clicks || 0),
          spend: Number(row.spend || 0),
          purchases: metaActionValue(actions, ["purchase", "omni_purchase", "offsite_conversion.fb_pixel_purchase"]),
          roas: (Array.isArray(row.purchase_roas) && row.purchase_roas[0]) ? row.purchase_roas[0].value : null,
          raw: row
        };
      });
      addLog(`Fetched ${insightRows.length} ad-set/country rows in ${elapsed(fetchStarted)}.`);
    }

    let adsetRows = [];
    if (syncConfiguration) {
      const fetchStarted = Date.now();
      try {
        const accountInfo = await fetchAccountInfo(accountRef, token, addLog);
        adsetRows = await fetchAdsetSnapshots({ accountRef, accountId, accountInfo, token, addLog });
        addLog(`Fetched ${adsetRows.length} ad-set configuration rows in ${elapsed(fetchStarted)}.`);
      } catch (error) {
        if (syncPart === "configuration") throw error;
        addLog(`Meta ad-set configuration sync skipped: ${error.message}`, "warn");
      }
    }

    let creativeRows = [];
    if (syncCreatives) try {
      const fetchStarted = Date.now();
      const baseAdFields = ["date_start", "campaign_id", "campaign_name", "adset_id", "adset_name", "ad_id", "ad_name", "spend", "impressions", "clicks", "actions"];
      const adSource = await fetchInsightsWithFallback({
        accountRef,
        range,
        level: "ad",
        baseFields: baseAdFields,
        richFields: [...baseAdFields, "reach", "frequency", "inline_link_clicks"],
        token,
        addLog,
        label: "Meta creative insights"
      });
      creativeRows = adSource.map((row) => {
        const actions = row.actions || [];
        return {
          date: row.date_start,
          account_id: accountId,
          campaign_id: row.campaign_id || null,
          campaign_name: row.campaign_name || "Unknown Campaign",
          adset_id: row.adset_id || null,
          adset_name: row.adset_name || "Unknown Ad Set",
          ad_id: row.ad_id,
          ad_name: row.ad_name || "Unknown Ad",
          impressions: Number(row.impressions || 0),
          reach: Number(row.reach || 0),
          frequency: Number(row.frequency || 0),
          clicks: Number(row.clicks || 0),
          link_clicks: Number(row.inline_link_clicks || 0) || metaActionValue(actions, ["link_click"]),
          landing_page_views: metaActionValue(actions, ["landing_page_view"]),
          registrations: metaActionValue(actions, ["complete_registration", "omni_complete_registration", "offsite_conversion.fb_pixel_complete_registration"]),
          spend: Number(row.spend || 0),
          purchases: metaActionValue(actions, ["purchase", "omni_purchase", "offsite_conversion.fb_pixel_purchase"]),
          raw: row
        };
      });
      addLog(`Fetched ${creativeRows.length} creative rows in ${elapsed(fetchStarted)}.`);
    } catch (error) {
      if (syncPart === "creatives") throw error;
      addLog(`Meta creative insight sync skipped: ${error.message}`, "warn");
    }

    // Fetch first, then clear. A transient Meta error must never erase the selected range.
    if (forceRefresh) {
      const clearedInsights = syncInsights
        ? await clearDateRange("meta_insights", "date", sinceUtc, untilUtc)
        : 0;
      const clearedAds = syncCreatives
        ? await clearDateRange("meta_ad_insights", "date", sinceUtc, untilUtc)
        : 0;
      const clearedConfigs = syncConfiguration
        ? await clearDateRange("meta_adsets", "snapshot_date", sinceUtc, untilUtc)
        : 0;
      addLog(`Force refresh: cleared ${clearedInsights} ad-set rows, ${clearedAds} creative rows and ${clearedConfigs} configuration rows.`);
    }

    const writeStarted = Date.now();
    const insightResult = syncInsights
      ? await upsert(
        "meta_insights",
        ["date", "account_id", "adset_id", "country"],
        ["date", "account_id", "country", "campaign_id", "campaign_name", "adset_id", "adset_name", "impressions", "clicks", "spend", "purchases", "roas", "raw", "ingested_at"],
        insightRows,
        { updateExisting: true, userEnteredColumns: ["date", "account_id"] }
      )
      : { rowCount: 0 };
    const adsetResult = syncConfiguration
      ? await upsert(
        "meta_adsets",
        ["snapshot_date", "adset_id"],
        ["snapshot_date", "snapshot_at", "account_id", "account_currency", "account_timezone", "campaign_id", "adset_id", "adset_name", "status", "effective_status", "daily_budget", "lifetime_budget", "budget_remaining", "bid_amount", "bid_strategy", "billing_event", "optimization_goal", "start_time", "end_time", "created_time", "updated_time", "targeting", "attribution_spec", "promoted_object", "raw", "ingested_at"],
        adsetRows,
        { createSheet: true, updateExisting: true }
      )
      : { rowCount: 0 };
    const creativeResult = syncCreatives
      ? await upsert(
        "meta_ad_insights",
        ["date", "account_id", "ad_id"],
        ["date", "account_id", "campaign_id", "campaign_name", "adset_id", "adset_name", "ad_id", "ad_name", "impressions", "reach", "frequency", "clicks", "link_clicks", "landing_page_views", "registrations", "spend", "purchases", "raw", "ingested_at"],
        creativeRows,
        { createSheet: true, updateExisting: true }
      )
      : { rowCount: 0 };

    addLog(`Stored the ${syncPart} stage in Google Sheets in ${elapsed(writeStarted)}.`, "success");
    addLog(`Stored ${insightResult.rowCount} ad-set/country rows, ${creativeResult.rowCount} creative rows and ${adsetResult.rowCount} configuration snapshots.`, "success");
    return {
      ok: true,
      rows: insightResult.rowCount + creativeResult.rowCount + adsetResult.rowCount,
      insightRows: insightResult.rowCount,
      creativeRows: creativeResult.rowCount,
      adsetRows: adsetResult.rowCount,
      logs
    };
  } catch (error) {
    return { ok: false, msg: `Meta Sync Error: ${error.message}`, logs };
  }
}
