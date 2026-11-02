import { getSteamExchangeRates } from '../../lib/steam-fx.js';

export const handler = async event => {
  if (event.httpMethod !== 'GET') return { statusCode: 405, body: 'Use GET' };
  const rates = await getSteamExchangeRates();
  return {
    statusCode: 200,
    headers: { 'content-type': 'application/json', 'cache-control': 'public, max-age=3600' },
    body: JSON.stringify(rates)
  };
};
