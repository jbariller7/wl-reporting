import { q, upsert, clearDateRange } from "./db.js";
import { getCursor, setCursor } from "./util.js";
import crypto from "node:crypto";

export async function etlStripe({ sinceUtc, untilUtc, forceRefresh }) {
  let logs = [];
  const addLog = (msg, type = 'info') => logs.push({ time: new Date().toISOString(), msg, type });
  
  const key = process.env.STRIPE_SECRET_KEY;
  if (!key) return { ok: false, msg: "Missing STRIPE_SECRET_KEY", logs };

  if (forceRefresh) {
    try {
      const delCount = await clearDateRange("stripe_orders", "created_at", sinceUtc, untilUtc);
      if (delCount > 0) addLog(`Force refresh: Cleared ${delCount} existing rows from Stripe.`);
    } catch (e) {
      addLog(`Force refresh error: ${e.message}`, 'error');
    }
  }

  const url = new URL("https://api.stripe.com/v1/checkout/sessions");
  url.searchParams.set("expand[]", "data.customer");
  url.searchParams.set("expand[]", "data.payment_intent");
  // NEW: Fetch the exact net deposit amount from Stripe!
  url.searchParams.set("expand[]", "data.payment_intent.latest_charge.balance_transaction");
  url.searchParams.set("limit", "100");
  url.searchParams.set("created[gte]", Math.floor(new Date(sinceUtc).getTime() / 1000).toString());
  url.searchParams.set("created[lte]", Math.floor(new Date(untilUtc).getTime() / 1000).toString());

  let more = true;
  let starting_after = null;
  let total = 0;
  
  while (more) {
    if (starting_after) url.searchParams.set("starting_after", starting_after);
    const resp = await fetch(url, { headers: { Authorization: `Bearer ${key}` } });
    if (!resp.ok) throw new Error(`Stripe ${resp.status}`);
    
    const data = await resp.json();
    const batch = (data.data || []).map((s) => {
      const meta = s.metadata || {};
      return {
        id: s.id,
        created_at: new Date(s.created * 1000).toISOString(),
        amount: s.amount_total ?? 0,
        currency: (s.currency || "eur").toLowerCase(),
        status: s.payment_status || "unknown",
        customer_email_hash: s.customer_details?.email ? hashEmail(s.customer_details.email) : null,
        checkout_session_id: s.id,
        product_id: s.line_items?.data?.[0]?.price?.product ?? null,
        price_id: s.line_items?.data?.[0]?.price?.id ?? null,
        fbp: meta.fbp ?? null,
        fbc: meta.fbc ?? null,
        ttclid: meta.ttclid ?? null,
        country: s.customer_details?.address?.country ?? null,
        metadata: s.metadata || {},
        raw: s
      };
    });
    
    await upsert("stripe_orders", ["id"], 
      ["id", "created_at", "amount", "currency", "status", "customer_email_hash", "checkout_session_id", "product_id", "price_id", "fbp", "fbc", "ttclid", "country", "metadata", "raw", "ingested_at"], 
      batch
    );
    
    total += batch.length;
    more = data.has_more;
    starting_after = data.data?.length ? data.data[data.data.length - 1].id : null;
  }
  return { ok: true, rows: total, logs };
}

function hashEmail(email) {
  return crypto.createHash("sha256").update(email.toLowerCase()).digest("hex");
}

export async function etlMeta({ sinceUtc, untilUtc, forceRefresh }) {
  const token = process.env.FB_SYSTEM_USER_TOKEN;
  const accountId = process.env.FB_AD_ACCOUNT_ID;
  let logs = [];
  const addLog = (msg, type = 'info') => logs.push({ time: new Date().toISOString(), msg, type });

  if (!token || !accountId) return { ok: false, msg: "Missing Meta token/account id", logs };

  try {
    if (forceRefresh) {
      try {
        const delCount = await clearDateRange("meta_insights", "date", sinceUtc, untilUtc);
        if (delCount > 0) addLog(`Force refresh: Cleared ${delCount} existing rows from Meta.`);
      } catch (e) {
        addLog(`Force refresh error: ${e.message}`, 'error');
      }
    }

    addLog(`Querying Meta Insights (Ad Set + Country) for: ${accountId}`);

    const fields = ["date_start", "campaign_id", "campaign_name", "adset_id", "adset_name", "spend", "impressions", "clicks", "purchase_roas", "actions"].join(",");
    let nextUrl = new URL(`https://graph.facebook.com/v25.0/${accountId.startsWith('act_') ? accountId : 'act_'+accountId}/insights`);
    
    nextUrl.searchParams.set("time_increment", "1");
    nextUrl.searchParams.set("time_range", JSON.stringify({ since: sinceUtc.slice(0, 10), until: untilUtc.slice(0, 10) }));
    nextUrl.searchParams.set("level", "adset"); 
    nextUrl.searchParams.set("fields", fields);
    nextUrl.searchParams.set("breakdowns", "country");
    nextUrl.searchParams.set("limit", "100"); 

    let allRows = [];
    let pages = 0;

    while (nextUrl && pages < 20) {
      pages++;
      const urlString = typeof nextUrl === 'string' ? nextUrl : nextUrl.toString();
      const resp = await fetch(urlString, { headers: { Authorization: `Bearer ${token}` } });

      const contentType = resp.headers.get("content-type");
      if (!contentType || !contentType.includes("application/json")) {
        addLog(`⚠️ Server returned non-JSON response. Stopping at page ${pages}.`, 'warn');
        break; 
      }

      const json = await resp.json();
      if (!resp.ok) throw new Error(json.error?.message || "Meta API Error");

      const batch = (json.data || []).map((r) => {
        const actions = r.actions || [];
        const purchases = actions.find((a) => a.action_type === "purchase");
        return {
          date: r.date_start,
          account_id: String(accountId).replace(/^act_/, ""),
          country: r.country || "Unknown",
          campaign_id: r.campaign_id,
          campaign_name: r.campaign_name || "Unknown Campaign",
          adset_id: r.adset_id, 
          adset_name: r.adset_name || "Unknown Ad Set", 
          impressions: Number(r.impressions || 0),
          clicks: Number(r.clicks || 0),
          spend: Number(r.spend || 0),
          purchases: purchases ? Number(purchases.value || 0) : null,
          roas: (Array.isArray(r.purchase_roas) && r.purchase_roas[0]) ? r.purchase_roas[0].value : null,
          raw: r
        };
      });

      allRows = allRows.concat(batch);
      nextUrl = json.paging?.next || null;
      if (nextUrl) addLog(`Fetched page ${pages}...`);
    }

    if (allRows.length > 0) {
      addLog(`✅ Sending ${allRows.length} adset-level rows to Sheets...`, 'success');
      const { rowCount } = await upsert("meta_insights", ["date", "account_id", "adset_id", "country"], 
        ["date","account_id","country","campaign_id","campaign_name","adset_id","adset_name","impressions","clicks","spend","purchases","roas","raw","ingested_at"],
        allRows
      );
      return { ok: true, rows: rowCount, logs };
    }
    
    return { ok: true, rows: 0, logs: [...logs, { msg: "No data found.", type: "warn" }] };

  } catch (err) {
    return { ok: false, msg: `Meta Sync Error: ${err.message}`, logs };
  }
}

export async function etlTikTok({ sinceUtc, untilUtc, forceRefresh }) {
  let logs = [];
  const addLog = (msg, type = 'info') => logs.push({ time: new Date().toISOString(), msg, type });

  const token = process.env.TIKTOK_ACCESS_TOKEN;
  const advertiserId = process.env.TIKTOK_ADVERTISER_ID;
  if (!token || !advertiserId) return { ok: false, msg: "Missing TikTok token or advertiser id", logs };

  if (forceRefresh) {
    try {
      const delCount = await clearDateRange("tiktok_insights", "date", sinceUtc, untilUtc);
      if (delCount > 0) addLog(`Force refresh: Cleared ${delCount} existing rows from TikTok.`);
    } catch (e) {
      addLog(`Force refresh error: ${e.message}`, 'error');
    }
  }

  const url = "https://business-api.tiktok.com/open_api/v1.3/reports/integrated/get/";
  const body = {
    advertiser_id: advertiserId,
    report_type: "BASIC",
    data_level: "AUCTION_AD",
    dimensions: ["stat_time_day", "ad_id", "adgroup_id", "campaign_id"],
    metrics: ["spend", "impressions", "clicks", "conversion", "conversions_value"],
    start_date: sinceUtc.slice(0, 10),
    end_date: untilUtc.slice(0, 10),
    time_granularity: "DAILY",
    page_size: 1000
  };
  
  const resp = await fetch(url, { method: "POST", headers: { "Content-Type": "application/json", "Access-Token": token }, body: JSON.stringify(body) });
  if (!resp.ok) throw new Error(`TikTok ${resp.status}`);
  
  const json = await resp.json();
  const list = json.data?.list || [];
  const rows = list.map((r) => {
    return {
      date: r.stat_time_day,
      advertiser_id: String(advertiserId),
      campaign_id: r.campaign_id ?? null,
      adgroup_id: r.adgroup_id ?? null,
      ad_id: r.ad_id ?? null,
      impressions: Number(r.impressions || 0),
      clicks: Number(r.clicks || 0),
      spend: Number(r.spend || 0),
      conversions: Number(r.conversion || 0),
      conversion_value: Number(r.conversions_value || 0),
      cpm: r.impressions > 0 ? (r.spend / r.impressions) * 1000 : null,
      cpc: r.clicks > 0 ? r.spend / r.clicks : null,
      roas: r.conversions_value > 0 ? r.conversions_value / (r.spend || 1) : null,
      raw: r
    };
  });
  
  await upsert("tiktok_insights", ["date", "advertiser_id", "ad_id"], 
    ["date", "advertiser_id", "campaign_id", "adgroup_id", "ad_id", "impressions", "clicks", "spend", "conversions", "conversion_value", "cpm", "cpc", "roas", "raw", "ingested_at"], 
    rows
  );
  return { ok: true, rows: rows.length, logs };
}

export async function etlMailerLite({ sinceUtc, untilUtc, forceRefresh }) {
  const key = process.env.MAILERLITE_API_KEY;
  let logs = [];
  const addLog = (msg, type = 'info') => logs.push({ time: new Date().toISOString(), msg, type });

  if (!key) return { ok: false, msg: "Missing MAILERLITE_API_KEY", logs };

  if (forceRefresh) {
    try {
      const delCount = await clearDateRange("mailerlite_subscribers", "created_at", sinceUtc, untilUtc);
      if (delCount > 0) addLog(`Force refresh: Cleared ${delCount} existing rows from MailerLite.`);
    } catch (e) {
      addLog(`Force refresh error: ${e.message}`, 'error');
    }
  }

  const headers = { 'Content-Type': 'application/json', 'Accept': 'application/json', 'Authorization': `Bearer ${key}` };

  try {
    addLog("Starting Bulk MailerLite sync...");
    const groupsResp = await fetch("https://connect.mailerlite.com/api/groups", { headers });
    const groupsData = await groupsResp.json();
    const groups = groupsData.data || [];

    let nextCursor = null;
    let totalSynced = 0;
    let hasMore = true;
    let pageCount = 0;

    while (hasMore && pageCount < 20) {
      pageCount++;
      let subUrl = `https://connect.mailerlite.com/api/subscribers?limit=100`;
      if (nextCursor) subUrl += `&cursor=${nextCursor}`;

      const resp = await fetch(subUrl, { headers });
      const data = await resp.json();
      if (!resp.ok) throw new Error(data.message || `API Error ${resp.status}`);

      const subs = data.data || [];
      if (subs.length === 0) break;

      const relevantSubs = subs.filter(s => {
        const created = s.created_at || s.subscribed_at;
        return created >= sinceUtc && created <= untilUtc;
      });

      if (relevantSubs.length > 0) {
        const ipsToLookup = [...new Set(relevantSubs.filter(s => !s.fields?.country && s.ip_address).map(s => s.ip_address))];
        let ipToCountryMap = {};
        if (ipsToLookup.length > 0) {
          try {
            const geoResp = await fetch(`https://api.country.is/`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(ipsToLookup) });
            const geoData = await geoResp.json();
            geoData.forEach(res => { if(res.country) ipToCountryMap[res.ip] = res.country; });
            addLog(`Bulk geolocated ${ipsToLookup.length} unique IPs.`);
          } catch (e) {
            addLog(`Geolocation error: ${e.message}`, 'warn');
          }
        }

        const batchToUpsert = relevantSubs.map(s => ({
          subscriber_id: String(s.id),
          email_hash: s.email ? crypto.createHash("sha256").update(s.email.toLowerCase()).digest("hex") : null,
          status: s.status,
          created_at: s.created_at || s.subscribed_at || null,
          country: s.fields?.country || s.fields?.Country || ipToCountryMap[s.ip_address] || null,
          raw: s
        }));

        await upsert("mailerlite_subscribers", ["subscriber_id"], ["subscriber_id", "email_hash", "status", "created_at", "country", "raw", "ingested_at"], batchToUpsert);
        totalSynced += batchToUpsert.length;
      }

      const oldestInBatch = subs[subs.length - 1]?.created_at || subs[subs.length - 1]?.subscribed_at;
      if (oldestInBatch && oldestInBatch < sinceUtc) {
        hasMore = false;
      } else {
        nextCursor = data.meta?.next_cursor || null;
        hasMore = !!nextCursor;
      }
    }

    addLog(`✅ Successfully synced ${totalSynced} subscribers.`, 'success');

    for (const g of groups.slice(0, 5)) {
      const gResp = await fetch(`https://connect.mailerlite.com/api/groups/${g.id}/subscribers?limit=100`, { headers });
      if (!gResp.ok) continue;
      const gData = await gResp.json();
      const membershipBatch = (gData.data || []).map(s => ({
        subscriber_id: String(s.id), group_id: String(g.id), added_at: s.subscribed_at || null
      }));
      if (membershipBatch.length > 0) {
        await upsert("mailerlite_group_memberships", ["subscriber_id", "group_id"], ["subscriber_id", "group_id", "added_at"], membershipBatch);
      }
    }

    return { ok: true, rows: totalSynced, logs };
  } catch (err) {
    return { ok: false, msg: `MailerLite Sync Error: ${err.message}`, logs };
  }
}

export async function etlSteamSalesApi({ sinceUtc, untilUtc, forceRefresh }) {
  const key = process.env.STEAM_PUBLISHER_KEY;
  const baseUrl = process.env.STEAM_SALES_API_URL; 
  const appIdsEnv = process.env.STEAM_APP_ID;

  let logs = [];
  const addLog = (msg, type = 'info') => logs.push({ time: new Date().toISOString(), msg, type });

  if (!key || !baseUrl || !appIdsEnv) return { ok: false, msg: "Steam Sales API disabled or missing env variables", logs };

  if (forceRefresh) {
    try {
      const delCount = await clearDateRange("steam_sales", "date", sinceUtc, untilUtc);
      if (delCount > 0) addLog(`Force refresh: Cleared ${delCount} existing rows from Steam.`);
    } catch(e) {
      addLog(`Force refresh error: ${e.message}`, 'error');
    }
  }

  const targetAppIds = appIdsEnv.split(',').map(id => id.trim());
  let totalRows = 0;

  try {
    const datesUrl = `${baseUrl.replace(/\/$/, '')}/IPartnerFinancialsService/GetChangedDatesForPartner/v1?key=${key}&highwatermark=0`;
    const datesResp = await fetch(datesUrl);
    const datesJson = await datesResp.json();
    const changedDates = datesJson.response?.dates || [];

    const filteredDates = changedDates.filter(d => {
      const dateStr = d.replace(/\//g, '-');
      return dateStr >= sinceUtc.slice(0, 10) && dateStr <= untilUtc.slice(0, 10);
    });

    if (filteredDates.length === 0) {
      addLog("No financial changes found in the requested range.");
      return { ok: true, rows: 0, logs };
    }

    addLog(`Syncing ${filteredDates.length} dates.`);

    for (const date of filteredDates) {
      addLog(`📅 Fetching details for: ${date}`);
      let highwatermarkId = 0;
      let hasMoreForDate = true;

      while (hasMoreForDate) {
        const detailUrl = `${baseUrl.replace(/\/$/, '')}/IPartnerFinancialsService/GetDetailedSales/v1?key=${key}&date=${date}&highwatermark_id=${highwatermarkId}`;
        const detailResp = await fetch(detailUrl);
        const detailJson = await detailResp.json();
        const responseObj = detailJson.response || {};
        
        const rawData = responseObj.results || responseObj.data || [];
        const maxId = responseObj.max_id || 0;

        if (rawData.length > 0) {
          const relevantRows = rawData.filter(r => {
            const id = String(r.primary_appid || r.appid || '');
            return targetAppIds.includes(id);
          });

          if (relevantRows.length > 0) {
            addLog(`✅ MATCH FOUND: Aggregating ${relevantRows.length} purchase types for ${date}`, 'success');
            
            // AGGREGATOR: Combine all DLC/Bundles into a single row per country/currency to prevent sheet overwrites!
            const aggMap = {};
            for (const r of relevantRows) {
              const rowDate = date.replace(/\//g, '-');
              const pId = String(r.primary_appid || r.appid);
              const cCode = r.country_code || 'Unknown';
              const cur = r.currency || 'USD';
              
              const key = `${rowDate}_${pId}_${cCode}_${cur}`;
              
              if (!aggMap[key]) {
                aggMap[key] = {
                  date: rowDate,
                  app_id: pId,
                  country: cCode,
                  currency: cur,
                  units: 0,
                  gross_revenue: 0,
                  refunds: 0,
                  net_units: 0,
                  net_revenue: 0,
                  source: "api",
                  raw: "aggregated"
                };
              }
              
              aggMap[key].units += Number(r.gross_units_sold || 0);
              aggMap[key].gross_revenue += parseFloat(r.gross_sales_usd || 0);
              aggMap[key].refunds += Math.abs(Number(r.gross_units_returned || 0));
              aggMap[key].net_units += Number(r.net_units_sold || 0);
              aggMap[key].net_revenue += parseFloat(r.net_sales_usd || 0);
            }
            
            const rowsToUpsert = Object.values(aggMap);

            const { rowCount } = await upsert("steam_sales", ["date", "app_id", "country", "currency"], 
              ["date", "app_id", "country", "currency", "units", "gross_revenue", "refunds", "net_units", "net_revenue", "source", "raw", "ingested_at"], 
              rowsToUpsert
            );
            totalRows += rowCount;
          }
        }

        if (maxId > highwatermarkId && rawData.length > 0) {
          highwatermarkId = maxId;
        } else {
          hasMoreForDate = false;
        }
      }
    }

    return { ok: true, rows: totalRows, logs };
  } catch (err) {
    return { ok: false, msg: `Fatal Error: ${err.message}`, logs };
  }
}
export async function etlTelemetry({ sinceUtc, untilUtc, forceRefresh }) {
  let logs = [];
  const addLog = (msg, type = 'info') => logs.push({ time: new Date().toISOString(), msg, type });
  
  // You will need to add TELEMETRY_SHEET_ID in your Netlify Environment Variables!
  const sourceSheetId = process.env.TELEMETRY_SHEET_ID;
  if (!sourceSheetId) return { ok: false, msg: "Missing TELEMETRY_SHEET_ID env var", logs };

  try {
    if (forceRefresh) {
      const delCount = await clearDateRange("telemetry", "timestamp", sinceUtc, untilUtc);
      if (delCount > 0) addLog(`Force refresh: Cleared ${delCount} rows from Telemetry.`);
    }

    addLog(`Connecting to Source Telemetry Sheet...`);
    // Connect to the source sheet using the same Google Credentials
    const { JWT } = await import('google-auth-library');
    const { GoogleSpreadsheet } = await import('google-spreadsheet');
    const authClient = new JWT({
      email: process.env.GOOGLE_SERVICE_ACCOUNT_EMAIL,
      key: (process.env.GOOGLE_PRIVATE_KEY || '').replace(/\\n/g, '\n'),
      scopes: ['https://www.googleapis.com/auth/spreadsheets'],
    });
    
    const sourceDoc = new GoogleSpreadsheet(sourceSheetId, authClient);
    await sourceDoc.loadInfo();
    const sourceSheet = sourceDoc.sheetsByIndex[0]; // Assumes telemetry is on the first tab
    const rows = await sourceSheet.getRows();

    // Filter for demo and date range
    const relevantRows = rows.filter(r => {
      const d = r.get('timestamp') || r.get('date');
      const variant = (r.get('variant') || '').toLowerCase();
      const platform = (r.get('platform') || '').toLowerCase();
      const isDemo = variant === 'demo' || platform === 'demo';
      return d && d >= sinceUtc && d <= untilUtc && isDemo;
    });

    if (relevantRows.length === 0) return { ok: true, rows: 0, logs: [...logs, { msg: "No new demo telemetry found.", type: "warn" }] };

// Bulk IP Geolocation translation (Exact Match to MailerLite)
    const ipsToLookup = [...new Set(relevantRows.map(r => r.get('ip_address') || r.get('ip') || r.get('IP') || r.get('IpAddress')).filter(Boolean))];
    let ipToCountryMap = {};
    if (ipsToLookup.length > 0) {
      try {
        const geoResp = await fetch(`https://api.country.is/`, { 
          method: 'POST', 
          headers: { 'Content-Type': 'application/json' }, 
          body: JSON.stringify(ipsToLookup) 
        });
        const geoData = await geoResp.json();
        geoData.forEach(res => { if(res.country) ipToCountryMap[res.ip] = res.country; });
        addLog(`Bulk geolocated ${ipsToLookup.length} unique IPs.`);
      } catch (e) {
        addLog(`Geolocation error: ${e.message}`, 'warn');
      }
    }

    const batchToUpsert = relevantRows.map(r => {
       const ip = r.get('ip_address') || r.get('ip') || r.get('IP') || r.get('IpAddress');
       const country = r.get('country') || (ip ? ipToCountryMap[ip] : null) || 'Unknown';
       return {
          timestamp: r.get('timestamp') || r.get('date'),
          playerId: r.get('playerId') || 'unknown',
          stageId: r.get('stageId'),
          country: country,
          variant: r.get('variant') || 'demo'
       };
    });

    await upsert("telemetry", ["playerId", "timestamp", "stageId"], ["timestamp", "playerId", "stageId", "country", "variant"], batchToUpsert);
    return { ok: true, rows: batchToUpsert.length, logs };
  } catch (err) {
    return { ok: false, msg: `Telemetry Sync Error: ${err.message}`, logs };
  }
}
