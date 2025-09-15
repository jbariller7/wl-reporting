import {
  etlStripe,
  etlMeta,
  etlTikTok,
  etlMailerLite
} from "../../lib/etl.js";

/**
 * Scheduled function that runs hourly.
 * Fetches the last 48 hours of data for Stripe, Meta, TikTok and MailerLite.
 */
export default async (req) => {
  // Next run info provided by Netlify
  const { next_run } = await req.json();
  const now = new Date();
  const since = new Date(now.getTime() - 1000 * 60 * 60 * 48).toISOString();
  const until = now.toISOString();
  const range = { sinceUtc: since, untilUtc: until };
  const results = {
    stripe: await etlStripe(range).catch((e) => ({ ok: false, msg: e.message })),
    meta: await etlMeta(range).catch((e) => ({ ok: false, msg: e.message })),
    tiktok: await etlTikTok(range).catch((e) => ({ ok: false, msg: e.message })),
    mailerlite: await etlMailerLite(range).catch((e) => ({ ok: false, msg: e.message }))
  };
  console.log("cron-hourly results", results);
};

export const config = {
  schedule: "@hourly"
};