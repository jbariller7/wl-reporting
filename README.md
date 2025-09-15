# WonderLang Reporting - Netlify

This repository contains a Netlify-ready backend for aggregating data from Steam, Stripe, Meta Ads, TikTok Ads and MailerLite.  It sets up scheduled ETL functions to ingest data into a Postgres database and provides a simple metrics API for a dashboard front‑end.

## Environment variables

Set the following environment variables via the Netlify UI.  Secrets should be marked accordingly.

- `DB_URL` – connection string for your Postgres database (e.g. Netlify DB or another Postgres instance).
- `STRIPE_SECRET_KEY` – Stripe secret API key.
- `FB_SYSTEM_USER_TOKEN` – system user token for Meta’s Marketing API.
- `FB_AD_ACCOUNT_ID` – your Facebook Ads account ID (format `act_<id>` or bare ID).
- `TIKTOK_ACCESS_TOKEN` – access token for TikTok Business API.
- `TIKTOK_ADVERTISER_ID` – your TikTok advertiser ID.
- `MAILERLITE_API_KEY` – API key for the MailerLite v2 API.
- `STEAM_APP_ID` – your Steam app ID used for wishlist CSV uploads and sales.
- `STEAM_PUBLISHER_KEY` – (optional) new Steamworks Sales Data API key (if available).
- `STEAM_SALES_API_URL` – (optional) base URL for Valve’s new sales API if configured for your account.

## Setup

1. **Clone and install dependencies**

   ```bash
   git clone <this-repo> && cd wl-reporting
   npm install
   ```

2. **Run database migration locally** (optional) to set up the tables and indices:

   ```bash
   DB_URL=<your-db-url> npm run migrate
   ```

3. **Deploy to Netlify**

   - Connect this repo to Netlify using the Netlify UI.
   - Add the environment variables above in your site settings.
   - Deploy; the `build:db` script will run on first build to create the tables.

## API endpoints

The functions in `netlify/functions` expose the following endpoints under the `/.netlify/functions` path.

| Endpoint                       | Method | Description                                                       |
|-------------------------------|--------|-------------------------------------------------------------------|
| `/refresh`                    | POST   | Triggers ETL jobs for the specified sources and date range.       |
| `/fetch-stripe`               | POST   | Manually fetches Stripe Checkout Sessions for a date range.       |
| `/fetch-meta`                 | POST   | Manually fetches Meta Ads insights for a date range.              |
| `/fetch-tiktok`               | POST   | Manually fetches TikTok Ads insights for a date range.            |
| `/fetch-mailerlite`           | POST   | Manually fetches MailerLite subscribers and groups.               |
| `/fetch-steam-sales`          | POST   | Manually fetches Steam sales via the Sales API (optional).        |
| `/import-steam-wishlist-csv` | POST   | Parses and imports a Steam wishlist CSV report.                   |
| `/metrics`                    | GET    | Returns aggregated metrics for the dashboard front‑end.            |

## Cron functions

Two scheduled functions automate data collection:

- **cron-hourly** – runs every hour to fetch Stripe, Meta, TikTok and MailerLite data for the last 48 hours.
- **cron-daily** – runs at 04:00 UTC daily to fetch the prior day’s Steam sales data via the Sales API.

## Notes

* All timestamps are stored in UTC.  Convert to Europe/Paris in your front‑end when displaying dates.
* Sensitive values such as emails are hashed before storage.
* The script uses simple upsert logic to avoid duplicating rows when re‑ingesting overlapping data.
* See the source code in `lib/etl.js` and the SQL schema under `lib/sql/` for details on the data model.
