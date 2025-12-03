import { bad, ok, rangeFromCursor } from "../../lib/util.js";
import { runEtls } from "./helpers/run-etl.js";

export const handler = async (event) => {
  if (event.httpMethod !== "POST") return bad("Use POST");
  const body = event.body ? JSON.parse(event.body) : {};
  const sources = body.sources || ["stripe", "meta", "tiktok", "steam"];
  const fallbackDays = body.fallbackDays ?? 30;

  const range = await rangeFromCursor(sources, fallbackDays);
  const results = await runEtls(range, sources, { persistCursor: true });
  return ok({ range, results });
};
