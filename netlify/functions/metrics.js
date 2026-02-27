import { ok, bad } from "../../lib/util.js";

export const handler = async (event) => {
  if (event.httpMethod !== "GET") return bad("Use GET");
  
  return ok({
    message: "Metrics endpoint disabled. View your Google Sheets directly for dashboards and metrics!"
  });
};
