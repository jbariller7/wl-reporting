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
 * Fetch Meta Ads insights and upsert into meta_insights.
 *
 * @param {Object} range Object with sinceUtc and untilUtc ISO strings
 */
export async function etlMeta({ sinceUtc, untilUtc }) {
  const token = process.env.FB_SYSTEM_USER_TOKEN;
  const accountId = process.env.FB_AD_ACCOUNT_ID;
  if (!token || !accountId)
    return { ok: false, msg: "Missing Meta token or account id" };

  // Build request to the Insights endpoint (v23.0)
  const fields = [
    "date_start",
    "date_stop",
    "spend",
    "impressions",
    "clicks",
    "purchase_roas",
    "actions"
  ].join(",");
  const url = new URL(
    `https://graph.facebook.com/v23.0/${accountId}/insights`
  );
  url.searchParams.set("time_increment", "1");
  url.searchParams.set(
    "time_range",
    JSON.stringify({
      since: sinceUtc.slice(0, 10),
      until: untilUtc.slice(0, 10)
    })
  );
  url.searchParams.set("level", "ad");
  url.searchParams.set("fields", fields);

  const resp = await fetch(url, {
    headers: { Authorization: `Bearer ${token}` }
  });
  if (!resp.ok) throw new Error(`Meta ${resp.status}`);
  const json = await resp.json();
  const rows = (json.data || []).map((r) => {
    const purchases = (r.actions || []).find((a) => a.action_type === "purchase");
    const roas =
      Array.isArray(r.purchase_roas) && r.purchase_roas[0]
        ? r.purchase_roas[0].value
        : null;
    return {
      date: r.date_start,
      account_id: String(accountId).replace(/^act_/, ""),
      campaign_id: r.campaign_id ?? null,
      adset_id: r.adset_id ?? null,
      ad_id: r.ad_id ?? null,
      impressions: Number(r.impressions || 0),
      clicks: Number(r.clicks || 0),
      spend: Number(r.spend || 0),
      purchases: purchases ? Number(purchases.value || 0) : null,
      purchase_value: null,
      cpm: r.impressions > 0 ? (r.spend / r.impressions) * 1000 : null,
      cpc: r.clicks > 0 ? r.spend / r.clicks : null,
      roas: roas ?? null,
      raw: r
    };
  });
  await upsert(
    "meta_insights",
    ["date", "account_id", "ad_id"],
    [
      "date",
      "account_id",
      "campaign_id",
      "adset_id",
      "ad_id",
      "impressions",
      "clicks",
      "spend",
      "purchases",
      "purchase_value",
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
 * Fetch MailerLite subscribers and group memberships, upserting into tables.
 *
 * @param {Object} range Object with sinceUtc and untilUtc ISO strings
 */
export async function etlMailerLite({ sinceUtc, untilUtc }) {
  const key = process.env.MAILERLITE_API_KEY;
  if (!key) return { ok: false, msg: "Missing MAILERLITE_API_KEY" };
  // Fetch groups
  const groupsResp = await fetch(
    "https://connect.mailerlite.com/api/groups",
    {
      headers: { Authorization: `Bearer ${key}` }
    }
  );
  if (!groupsResp.ok)
    throw new Error(`MailerLite groups ${groupsResp.status}`);
  const groups = await groupsResp.json();
  // Fetch subscribers in range (paginated)
  let page = 1;
  let total = 0;
  while (true) {
    const resp = await fetch(
      `https://connect.mailerlite.com/api/subscribers?limit=1000&page=${page}&filter[created_at][from]=${sinceUtc}&filter[created_at][to]=${untilUtc}`,
      {
        headers: { Authorization: `Bearer ${key}` }
      }
    );
    if (!resp.ok)
      throw new Error(`MailerLite subscribers ${resp.status}`);
    const data = await resp.json();
    const subs = data.data || [];
    if (!subs.length) break;
    const batch = subs.map((s) => {
      const emailHash = s.email
        ? crypto
            .createHash("sha256")
            .update(s.email.toLowerCase())
            .digest("hex")
        : null;
      return {
        subscriber_id: s.id,
        email_hash: emailHash,
        status: s.status,
        created_at: s.subscribed_at || s.created_at || null,
        country: s.country || null,
        raw: s
      };
    });
    await upsert(
      "mailerlite_subscribers",
      ["subscriber_id"],
      [
        "subscriber_id",
        "email_hash",
        "status",
        "created_at",
        "country",
        "raw",
        "ingested_at"
      ],
      batch
    );
    total += batch.length;
    if (!data.links?.next) break;
    page += 1;
  }
  // Fetch group memberships snapshot for each group
  for (const g of groups.data || []) {
    let page2 = 1;
    while (true) {
      const url = `https://connect.mailerlite.com/api/groups/${g.id}/subscribers?limit=1000&page=${page2}`;
      const r = await fetch(url, {
        headers: { Authorization: `Bearer ${key}` }
      });
      if (!r.ok) break;
      const d = await r.json();
      const subs = d.data || [];
      if (!subs.length) break;
      const batch2 = subs.map((s) => ({
        subscriber_id: s.id,
        group_id: g.id,
        added_at: s.subscribed_at || null
      }));
      await upsert(
        "mailerlite_group_memberships",
        ["subscriber_id", "group_id"],
        ["subscriber_id", "group_id", "added_at"],
        batch2
      );
      if (!d.links?.next) break;
      page2 += 1;
    }
  }
  return { ok: true, rows: total };
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
