import { runEtls } from "./helpers/run-etl.js";

/**
 * Scheduled function that runs daily at 04:00 UTC.
 * Fetches the prior day’s Steam sales data via the Sales API.
 */
export default async (req) => {
  const { next_run } = await req.json();
  const now = new Date();
  const yesterday = new Date(now.getTime() - 1000 * 60 * 60 * 24);
  // Build date range covering entire prior UTC day
  const since = new Date(yesterday.setUTCHours(0, 0, 0, 0)).toISOString();
  const until = new Date(yesterday.setUTCHours(23, 59, 59, 999)).toISOString();
  const range = { sinceUtc: since, untilUtc: until };
  const steam = await runEtls(range, ["steam"], { persistCursor: true });
  console.log("cron-daily steam", steam);
};

export const config = {
  schedule: "0 4 * * *"
};