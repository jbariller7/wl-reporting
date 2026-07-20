# WonderLang Reporting - Netlify

This repository contains a Netlify-ready backend for aggregating data from Steam, Google Play, Stripe, Meta Ads, TikTok Ads and MailerLite. It stores ETL results in Google Sheets and provides a dashboard front-end.

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
- `GOOGLE_PLAY_REPORT_BUCKET` – Google Play financial-report bucket name, without the `gs://` prefix (for example `pubsite_prod_rev_0123456789`).
- `GOOGLE_PLAY_PACKAGE_ID` – (recommended) Android package ID to keep when the Play account contains more than one app.
- `GOOGLE_PLAY_FEE_RATE` – (optional) estimated Google Play service-fee rate used for the live dashboard; defaults to `0.15`.

## Google Play sales setup

Google Play exposes estimated sales as daily-updated monthly ZIP reports in a private Google Cloud Storage bucket. The importer reads those reports, creates the `Google_Play` tab if necessary, and stores sales and refunds as normalized transaction rows.

1. In Play Console, open **Download reports > Financial** and copy the Cloud Storage URI shown for **Estimated sales reports**. Put the bucket portion in `GOOGLE_PLAY_REPORT_BUCKET`.
2. In Play Console, open **Users and permissions** and invite the service account identified by `GOOGLE_SERVICE_ACCOUNT_EMAIL`.
3. Grant that service account permission to view financial data. The importer requests the read-only Cloud Storage scope.
4. Set `GOOGLE_PLAY_PACKAGE_ID` to the package name of the game and redeploy Netlify.
5. In the dashboard's **Sync** tab, choose a date range and click **Google Play**, or use **Update ALL**.

Google's estimated-sales report contains buyer-local gross and tax amounts but not the final Google fee. `net_revenue` therefore excludes reported taxes and applies `GOOGLE_PLAY_FEE_RATE`. Refund rows are negative. For accounting-grade finalized payouts, use the monthly Google Play earnings report instead. Reports may appear several days after a sale; the daily job rechecks the last 14 days. See [Google Play's financial report documentation](https://support.google.com/googleplay/android-developer/answer/6135870?hl=en-EN).

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
| `/fetch-google-play`          | POST   | Imports Google Play estimated sales reports into `Google_Play`.   |
| `/import-steam-wishlist-csv` | POST   | Parses and imports a Steam wishlist CSV report.                   |
| `/metrics`                    | GET    | Returns aggregated metrics for the dashboard front‑end.            |

## Cron functions

Two scheduled functions automate data collection:

- **cron-hourly** – runs every hour to fetch Stripe, Meta, TikTok and MailerLite data for the last 48 hours.
- **cron-daily** – runs at 04:00 UTC daily to fetch Steam sales and recheck the last 14 days of Google Play reports.

## Notes

* All timestamps are stored in UTC.  Convert to Europe/Paris in your front‑end when displaying dates.
* Sensitive values such as emails are hashed before storage.
* The script uses simple upsert logic to avoid duplicating rows when re‑ingesting overlapping data.
* See the source code in `lib/etl.js` and the SQL schema under `lib/sql/` for details on the data model.
