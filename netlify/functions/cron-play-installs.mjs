import { etlGooglePlayInstalls } from "../../lib/etl.js";

export default async () => {
  const now = new Date();
  const since = new Date(now.getTime() - 14 * 24 * 60 * 60 * 1000);
  since.setUTCHours(0, 0, 0, 0);
  const result = await etlGooglePlayInstalls({
    sinceUtc: since.toISOString(),
    untilUtc: now.toISOString()
  }).catch(error => ({ ok: false, msg: error.message }));
  console.log("cron-play-installs", result);
};

export const config = { schedule: "30 4 * * *" };
