import { ok, bad, parseRange } from "../../lib/util.js";
import { q } from "../../lib/db.js";

/**
 * GET /.netlify/functions/metrics
 *
 * Returns aggregated daily metrics for each source between a since/until range.
 * Query params: since, until (ISO strings).
 */
export const handler = async (event) => {
  if (event.httpMethod !== "GET") return bad("Use GET");
  const params = event.queryStringParameters || {};
  const { sinceUtc, untilUtc } = parseRange(params);
  // Build queries to aggregate daily metrics from each table
  const [stripe, meta, tiktok, ml, steam] = await Promise.all([
    q(
      "SELECT date_trunc('day', created_at)::date AS d, SUM(amount)/100.0 AS eur FROM stripe_orders WHERE created_at BETWEEN $1 AND $2 GROUP BY 1 ORDER BY 1",
      [sinceUtc, untilUtc]
    ),
    q(
      "SELECT date AS d, SUM(spend)::float AS spend, SUM(clicks)::bigint AS clicks FROM meta_insights WHERE date BETWEEN $1::date AND $2::date GROUP BY 1 ORDER BY 1",
      [sinceUtc.slice(0, 10), untilUtc.slice(0, 10)]
    ),
    q(
      "SELECT date AS d, SUM(spend)::float AS spend, SUM(clicks)::bigint AS clicks FROM tiktok_insights WHERE date BETWEEN $1::date AND $2::date GROUP BY 1 ORDER BY 1",
      [sinceUtc.slice(0, 10), untilUtc.slice(0, 10)]
    ),
    q(
      "SELECT date_trunc('day', created_at)::date AS d, COUNT(*) AS subs FROM mailerlite_subscribers WHERE created_at BETWEEN $1 AND $2 GROUP BY 1 ORDER BY 1",
      [sinceUtc, untilUtc]
    ),
    q(
      "SELECT date AS d, SUM(net_units)::bigint AS units, SUM(net_revenue)::float AS rev FROM steam_sales WHERE date BETWEEN $1::date AND $2::date GROUP BY 1 ORDER BY 1",
      [sinceUtc.slice(0, 10), untilUtc.slice(0, 10)]
    )
  ]);
  return ok({
    stripe_daily: stripe.rows,
    meta_daily: meta.rows,
    tiktok_daily: tiktok.rows,
    mailerlite_daily: ml.rows,
    steam_daily: steam.rows
  });
};