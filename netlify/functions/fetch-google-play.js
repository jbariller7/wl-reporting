import { parseRange, ok, bad } from "../../lib/util.js";
import { etlGooglePlaySales } from "../../lib/etl.js";

export const handler = async (event) => {
  if (event.httpMethod !== "POST") return bad("Use POST");
  const body = event.body ? JSON.parse(event.body) : {};
  const range = parseRange({ since: body.since, until: body.until });
  range.forceRefresh = body.forceRefresh === true;
  const result = await etlGooglePlaySales(range);
  return ok(result);
};

