import { etlSteamData, etlGooglePlaySales } from "../../lib/etl.js";

/**
 * Scheduled function that runs daily at 04:00 UTC.
 * Fetches recent closed Steam sales and wishlist data.
 */
export default async (req) => {
  const { next_run } = await req.json();
  const now = new Date();
  const yesterday = new Date(now.getTime() - 1000 * 60 * 60 * 24);
  const twoDaysAgo = new Date(now.getTime() - 2 * 1000 * 60 * 60 * 24);
  // Re-read the two latest closed GMT dates so a late daily wishlist refresh
  // cannot permanently leave a false zero in the dashboard.
  const since = new Date(twoDaysAgo.setUTCHours(0, 0, 0, 0)).toISOString();
  const until = new Date(yesterday.setUTCHours(23, 59, 59, 999)).toISOString();
  const range = { sinceUtc: since, untilUtc: until };
  const steam = await etlSteamData(range).catch((e) => ({ ok: false, msg: e.message }));
  console.log("cron-daily steam", steam);

  // Estimated sales reports can lag by several days, so re-read a rolling 14-day window.
  const googlePlaySince = new Date(now.getTime() - 14 * 24 * 60 * 60 * 1000);
  googlePlaySince.setUTCHours(0, 0, 0, 0);
  const googlePlayRange = { sinceUtc: googlePlaySince.toISOString(), untilUtc: now.toISOString() };
  const googlePlay = await etlGooglePlaySales(googlePlayRange).catch((e) => ({ ok: false, msg: e.message }));
  console.log("cron-daily google_play", googlePlay);
};

export const config = {
  schedule: "0 4 * * *"
};
