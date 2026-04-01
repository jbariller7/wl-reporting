import { parseRange, ok, bad } from "../../lib/util.js";
import { etlTelemetry } from "../../lib/etl.js";

export const handler = async (event) => {
  if (event.httpMethod !== "POST") return bad("Use POST");
  const body = event.body ? JSON.parse(event.body) : {};
  const range = parseRange(body);
  range.forceRefresh = body.forceRefresh === true;
  const res = await etlTelemetry(range);
  return ok(res);
};
