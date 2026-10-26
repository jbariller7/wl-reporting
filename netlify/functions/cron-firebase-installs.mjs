import { etlFirebaseInstalls } from "../../lib/firebase-installs.js";

export default async () => {
  const now = new Date();
  const since = new Date(now.getTime() - 14 * 86400000);
  since.setUTCHours(0, 0, 0, 0);
  const result = await etlFirebaseInstalls({ sinceUtc: since.toISOString(), untilUtc: now.toISOString() });
  console.log("cron-firebase-installs", result);
};

export const config = { schedule: "@hourly" };
