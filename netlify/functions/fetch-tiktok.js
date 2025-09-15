import { parseRange, ok, bad } from "../../lib/util.js";
import { etlTikTok } from "../../lib/etl.js";

export const handler = async (event) => {
  if (event.httpMethod !== "POST") return bad("Use POST");
  const body = event.body ? JSON.parse(event.body) : {};
  const range = parseRange(body);
  const res = await etlTikTok(range);
  return ok(res);
};
