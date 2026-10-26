import { parseRange, ok, bad } from "../../lib/util.js";
import { etlFirebaseInstalls } from "../../lib/firebase-installs.js";

export const handler = async event => {
  if (event.httpMethod !== "POST") return bad("Use POST");
  try {
    const body = JSON.parse(event.body || "{}");
    return ok(await etlFirebaseInstalls({ ...parseRange(body), dryRun: body.dryRun === true }));
  } catch (error) {
    return bad(error.message);
  }
};
