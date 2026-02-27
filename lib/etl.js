import { q, upsert } from "./db.js";
import { getCursor, setCursor } from "./util.js";
import crypto from "node:crypto";

/**
 * Fetch Stripe Checkout Sessions within a date range and upsert into stripe_orders.
 * Uses the Stripe REST API directly rather than the SDK to keep dependencies small.
 *
 * @param {Object} range Object with sinceUtc and untilUtc ISO strings
 */
export async function etlStripe({ sinceUtc, untilUtc }) {
  const key = process.env.STRIPE_SECRET_KEY;
  if (!key) return { ok: false, msg: "Missing STRIPE_SECRET_KEY" };

  const url = new URL("https://api.stripe.com/v1/checkout/sessions");
  url.searchParams.set("expand[]", "data.customer");
  url.searchParams.set("expand[]", "data.payment_intent");
  url.searchParams.set("limit", "100");
  url.searchParams.set(
    "created[gte]",
    Math.floor(new Date(sinceUtc).getTime() / 1000).toString()
  );
  url.searchParams.set(
    "created[lte]",
    Math.floor(new Date(untilUtc).getTime() / 1000).toString()
  );

  let more = true;
  let starting_after = null;
  let total = 0;
  while (more) {
    if (starting_after) url.searchParams.set("starting_after", starting_after);
    const resp = await fetch(url, {
      headers: { Authorization: `Bearer ${key}` }
    });
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
        customer_email_hash: s.customer_details?.email
          ? hashEmail(s.customer_details.email)
          : null,
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
    await upsert(
      "stripe_orders",
      ["id"],
      [
        "id",
        "created_at",
        "amount",
        "currency",
        "status",
        "customer_email_hash",
        "checkout_session_id",
        "product_id",
        "price_id",
        "fbp",
        "fbc",
        "ttclid",
        "country",
        "metadata",
        "raw",
        "ingested_at"
      ],
      batch
    );
    total += batch.length;
    more = data.has_more;
    starting_after = data.data?.length ? data.data[data.data.length - 1].id : null;
  }
  return { ok: true, rows: total };
}

function hashEmail(email) {
  // Return a SHA-256 hash of the lower-case email
  return crypto.createHash("sha256").update(email.toLowerCase()).digest("hex");
}



/**
 * Ad Set-Level Meta Ads ETL
 * Requests data by Ad Set + Country to provide detailed decomposition 
 * while avoiding the extreme row volume of the Ad level.
 */
export async function etlMeta({ sinceUtc, untilUtc }) {
  const token = process.env.FB_SYSTEM_USER_TOKEN;
  const accountId = process.env.FB_AD_ACCOUNT_ID;
  let logs = [];
  const addLog = (msg, type = 'info') => logs.push({ time: new Date().toISOString(), msg, type });

  if (!token || !accountId) return { ok: false, msg: "Missing Meta token/account id", logs };

  try {
    addLog(`Querying Meta Insights (Ad Set + Country) for: ${accountId}`);

    // FIELDS: Added adset_id and adset_name
    const fields = [
      "date_start",
      "campaign_id",
      "campaign_name",
      "adset_id",
      "adset_name",
      "spend",
      "impressions",
      "clicks",
      "purchase_roas",
      "actions"
    ].join(",");
    
    let nextUrl = new URL(`https://graph.facebook.com/v25.0/${accountId.startsWith('act_') ? accountId : 'act_'+accountId}/insights`);
    
    nextUrl.searchParams.set("time_increment", "1");
    nextUrl.searchParams.set("time_range", JSON.stringify({
      since: sinceUtc.slice(0, 10),
      until: untilUtc.slice(0, 10)
    }));
    
    // CHANGE 1: Set level to 'adset'
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
          adset_id: r.adset_id, // Map the adset ID
          adset_name: r.adset_name || "Unknown Ad Set", // Map the adset name
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
      
      // CHANGE 2: Unique key is now Date + Account + Ad Set + Country
      const { rowCount } = await upsert(
        "meta_insights",
        ["date", "account_id", "adset_id", "country"], 
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

/**
 * Fetch TikTok Ads insights and upsert into tiktok_insights.
 *
 * @param {Object} range Object with sinceUtc and untilUtc ISO strings
 */
export async function etlTikTok({ sinceUtc, untilUtc }) {
  const token = process.env.TIKTOK_ACCESS_TOKEN;
  const advertiserId = process.env.TIKTOK_ADVERTISER_ID;
  if (!token || !advertiserId)
    return { ok: false, msg: "Missing TikTok token or advertiser id" };

  const url = "https://business-api.tiktok.com/open_api/v1.3/reports/integrated/get/";
  const body = {
    advertiser_id: advertiserId,
    report_type: "BASIC",
    data_level: "AUCTION_AD",
    dimensions: ["stat_time_day", "ad_id", "adgroup_id", "campaign_id"],
    metrics: [
      "spend",
      "impressions",
      "clicks",
      "conversion",
      "conversions_value"
    ],
    start_date: sinceUtc.slice(0, 10),
    end_date: untilUtc.slice(0, 10),
    time_granularity: "DAILY",
    page_size: 1000
  };
  const resp = await fetch(url, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Access-Token": token
    },
    body: JSON.stringify(body)
  });
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
      roas:
        r.conversions_value > 0 ? r.conversions_value / (r.spend || 1) : null,
      raw: r
    };
  });
  await upsert(
    "tiktok_insights",
    ["date", "advertiser_id", "ad_id"],
    [
      "date",
      "advertiser_id",
      "campaign_id",
      "adgroup_id",
      "ad_id",
      "impressions",
      "clicks",
      "spend",
      "conversions",
      "conversion_value",
      "cpm",
      "cpc",
      "roas",
      "raw",
      "ingested_at"
    ],
    rows
  );
  return { ok: true, rows: rows.length };
}





/**
 * Bulk MailerLite ETL with Optimized Geolocation
 * Uses api.country.is POST endpoint to lookup 100 IPs at once.
 */
export async function etlMailerLite({ sinceUtc, untilUtc }) {
  const key = process.env.MAILERLITE_API_KEY;
  let logs = [];
  const addLog = (msg, type = 'info') => logs.push({ time: new Date().toISOString(), msg, type });

  if (!key) return { ok: false, msg: "Missing MAILERLITE_API_KEY", logs };

  const headers = {
    'Content-Type': 'application/json',
    'Accept': 'application/json',
    'Authorization': `Bearer ${key}`
  };

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

      // Filter only subs in range
      const relevantSubs = subs.filter(s => {
        const created = s.created_at || s.subscribed_at;
        return created >= sinceUtc && created <= untilUtc;
      });

      if (relevantSubs.length > 0) {
        // --- BULK GEOLOCATION LOGIC ---
        // 1. Identify IPs that need a lookup
        const ipsToLookup = [...new Set(relevantSubs
          .filter(s => !s.fields?.country && s.ip_address)
          .map(s => s.ip_address)
        )];

        let ipToCountryMap = {};
        if (ipsToLookup.length > 0) {
          try {
            // POST all IPs in one request (Max 100 per request)
            const geoResp = await fetch(`https://api.country.is/`, {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify(ipsToLookup)
            });
            const geoData = await geoResp.json();
            // Map: { "8.8.8.8": "US", ... }
            geoData.forEach(res => { if(res.country) ipToCountryMap[res.ip] = res.country; });
            addLog(`Bulk geolocated ${ipsToLookup.length} unique IPs.`);
          } catch (e) {
            addLog(`Geolocation error: ${e.message}`, 'warn');
          }
        }

        // 2. Map subscribers to final format
        const batchToUpsert = relevantSubs.map(s => ({
          subscriber_id: String(s.id),
          email_hash: s.email ? crypto.createHash("sha256").update(s.email.toLowerCase()).digest("hex") : null,
          status: s.status,
          created_at: s.created_at || s.subscribed_at || null,
          country: s.fields?.country || s.fields?.Country || ipToCountryMap[s.ip_address] || null,
          raw: s
        }));

        await upsert(
          "mailerlite_subscribers",
          ["subscriber_id"],
          ["subscriber_id", "email_hash", "status", "created_at", "country", "raw", "ingested_at"],
          batchToUpsert
        );
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

    // Quick Group Sync
    for (const g of groups.slice(0, 5)) {
      const gResp = await fetch(`https://connect.mailerlite.com/api/groups/${g.id}/subscribers?limit=100`, { headers });
      if (!gResp.ok) continue;
      const gData = await gResp.json();
      const membershipBatch = (gData.data || []).map(s => ({
        subscriber_id: String(s.id),
        group_id: String(g.id),
        added_at: s.subscribed_at || null
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



/**
 * Final Corrected Steam Financial ETL
 * 1. Correctly identifies the 'results' key in the Steam response.
 * 2. Maps 'primary_appid' to ensure matches against your game IDs.
 * 3. Handles gross vs net units and revenue exactly as Steam reports them.
 */
export async function etlSteamSalesApi({ sinceUtc, untilUtc }) {
  const key = process.env.STEAM_PUBLISHER_KEY;
  const baseUrl = process.env.STEAM_SALES_API_URL; 
  const appIdsEnv = process.env.STEAM_APP_ID;

  if (!key || !baseUrl || !appIdsEnv)
    return { ok: false, msg: "Steam Sales API disabled or missing env variables" };

  const targetAppIds = appIdsEnv.split(',').map(id => id.trim());
  let totalRows = 0;
  let logs = [];

  const addLog = (msg, type = 'info') => logs.push({ time: new Date().toISOString(), msg, type });

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

    addLog(`Syncing ${filteredDates.length} dates. Looking specifically for 'results' and 'primary_appid'.`);

    for (const date of filteredDates) {
      addLog(`📅 Fetching details for: ${date}`);
      let highwatermarkId = 0;
      let hasMoreForDate = true;

      while (hasMoreForDate) {
        const detailUrl = `${baseUrl.replace(/\/$/, '')}/IPartnerFinancialsService/GetDetailedSales/v1?key=${key}&date=${date}&highwatermark_id=${highwatermarkId}`;
        const detailResp = await fetch(detailUrl);
        const detailJson = await detailResp.json();
        const responseObj = detailJson.response || {};
        
        // FIX: Look specifically for the 'results' key found in your raw debug output
        const rawData = responseObj.results || responseObj.data || [];
        const maxId = responseObj.max_id || 0;

        if (rawData.length > 0) {
          // Filter by both appid AND primary_appid to ensure we catch all sales
          const relevantRows = rawData.filter(r => {
            const id = String(r.primary_appid || r.appid || '');
            return targetAppIds.includes(id);
          });

          if (relevantRows.length > 0) {
            addLog(`✅ MATCH FOUND: Found ${relevantRows.length} sales records for your games on ${date}`, 'success');
            
            const rowsToUpsert = relevantRows.map(r => ({
              date: date.replace(/\//g, '-'), 
              app_id: String(r.primary_appid || r.appid),
              country: r.country_code || null,
              currency: r.currency || "USD",
              // Use the exact fields from your raw debug output
              units: Number(r.gross_units_sold || 0),
              gross_revenue: parseFloat(r.gross_sales_usd || 0),
              refunds: Math.abs(Number(r.gross_units_returned || 0)), // Convert -1 to 1 for the sheet
              net_units: Number(r.net_units_sold || 0),
              net_revenue: parseFloat(r.net_sales_usd || 0),
              source: "api",
              raw: r
            }));

            // Send to Google Sheets
            const { rowCount } = await upsert("steam_sales", ["date", "app_id", "country", "currency"], 
              ["date", "app_id", "country", "currency", "units", "gross_revenue", "refunds", "net_units", "net_revenue", "source", "raw", "ingested_at"], 
              rowsToUpsert
            );
            totalRows += rowCount;
          } else {
            addLog(`ℹ️ Date ${date} has data, but no IDs match your specific list.`, 'info');
          }
        }

        if (maxId > highwatermarkId && rawData.length > 0) {
          highwatermarkId = maxId;
        } else {
          hasMoreForDate = false;
        }
      }
    }

    return { ok: true, rows: totalRows, logs, steam_raw_debug: "Success! Data sent to Sheets." };

  } catch (err) {
    return { ok: false, msg: `Fatal Error: ${err.message}`, logs };
  }
}
