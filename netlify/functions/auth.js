// netlify/functions/auth.js
export const handler = async (event) => {
  if (event.httpMethod !== "POST") return { statusCode: 405, body: "Method Not Allowed" };
  
  try {
    const body = JSON.parse(event.body || "{}");
    const validPin = process.env.DASHBOARD_PIN;
    
    if (!validPin) {
      return { statusCode: 500, body: JSON.stringify({ error: "Server missing DASHBOARD_PIN env var" }) };
    }

    if (body.pin === validPin) {
      return { statusCode: 200, body: JSON.stringify({ ok: true }) };
    }
    
    return { statusCode: 401, body: JSON.stringify({ ok: false }) };
  } catch (e) {
    return { statusCode: 400, body: "Bad Request" };
  }
};
