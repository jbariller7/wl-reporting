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
- `STEAM_APP_ID` – your Steam app ID (or comma-separated app IDs) used for sales and wishlist reporting.
- `STEAM_PUBLISHER_KEY` – Steamworks Financial Web API publisher key used for both sales and wishlist reporting.
- `STEAM_SALES_API_URL` – (optional) Steam partner API base URL override; defaults to `https://partner.steam-api.com`.
- `GOOGLE_PLAY_REPORT_BUCKET` – Google Play financial-report bucket name, without the `gs://` prefix (for example `pubsite_prod_rev_0123456789`).
- `GOOGLE_PLAY_PACKAGE_ID` – Android package ID to keep when the Play account contains more than one app; required for install reports (`com.wonderlang.app` for the production app).
- `GOOGLE_PLAY_FEE_RATE` – (optional) estimated Google Play service-fee rate used for the live dashboard; defaults to `0.15`.

## Google Play sales setup

Google Play exposes estimated sales as daily-updated monthly ZIP reports in a private Google Cloud Storage bucket. The importer reads those reports and stores rows in `Google_Play` using Google Play's native Estimated sales report headers. The same native schema can be pasted into the tab manually and remains compatible with future automatic syncs.

1. In Play Console, open **Download reports > Financial** and copy the Cloud Storage URI shown for **Estimated sales reports**. Put the bucket portion in `GOOGLE_PLAY_REPORT_BUCKET`.
2. In Play Console, open **Users and permissions** and invite the service account identified by `GOOGLE_SERVICE_ACCOUNT_EMAIL`.
3. Grant that service account permission to view financial data. The importer requests the read-only Cloud Storage scope.
4. Set `GOOGLE_PLAY_PACKAGE_ID` to the package name of the game and redeploy Netlify.
5. In the dashboard's **Sync** tab, choose a date range and click **Google Play**, or use **Update ALL**.

Google's estimated-sales report contains buyer-local gross and tax amounts but not the final Google fee. The dashboard derives net revenue from `Item Price`, applies the configured 15% Google Play fee assumption, converts `Currency of Sale` to EUR, then applies the shared 12.5% URSSAF rate. Refund rows are negative. For accounting-grade finalized payouts, use the monthly Google Play earnings report instead. Reports may appear several days after a sale; the daily job rechecks the last 14 days. See [Google Play's financial report documentation](https://support.google.com/googleplay/android-developer/answer/6135870?hl=en-EN).

## Android installs setup

The Android installs sync reads the Google Play Statistics country report from the same `GOOGLE_PLAY_REPORT_BUCKET`. Set `GOOGLE_PLAY_PACKAGE_ID` and grant `GOOGLE_SERVICE_ACCOUNT_EMAIL` the Play Console **global View app information and download bulk reports** permission. The importer creates a `Google_Play_Installs` tab with one row per date, package and country. Rows without a country are skipped with a sync warning; valid rows continue, and a force refresh preserves existing rows for a month containing missing-country source rows. The dashboard shows **Daily User Installs** as Android User Installs; the Sheet also keeps Daily Device Installs. These are Play installs, not Firebase first opens or Meta-attributed conversions.

In **Data Sync**, select a historical range and click **Android Installs** once to backfill. Subsequent daily runs recheck the last 14 days because Play's reports can arrive 3–7 days after the install. The metric uses Play's Pacific Time reporting dates. See [Google Play's install report format and availability](https://support.google.com/googleplay/android-developer/answer/6135870?hl=en-EN).

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

## Steam sales and wishlist setup

The Steam sync button and the daily job now update both `Steam_Sales` and a dedicated `Steam_Wishlist` tab. No additional credential is required: Valve's wishlist endpoint uses the same Financial Web API publisher key as the sales endpoints. The key must belong to a Steamworks Financial API Group, and `STEAM_APP_ID` must contain every app to query.

`Steam_Wishlist` stores one row per GMT date, app and country, with adds, deletes, purchases from wishlist, gifts and the Windows/macOS/Linux add breakdown. The dashboard treats wishlist adds as a country-level intent signal only; it never adds them to revenue or presents them as Meta-attributed conversions. Select a historical date range and click **Steam** once to backfill it. Later daily runs refresh the two most recent closed days automatically.

See Valve's [IPartnerFinancialsService API documentation](https://partner.steamgames.com/doc/webapi/IPartnerFinancialsService?l=english#GetAppWishlistReporting) and [wishlist reporting notes](https://partner.steamgames.com/doc/marketing/wishlist/reporting?l=english).

## API endpoints

The functions in `netlify/functions` expose the following endpoints under the `/.netlify/functions` path.

| Endpoint                       | Method | Description                                                       |
|-------------------------------|--------|-------------------------------------------------------------------|
| `/refresh`                    | POST   | Triggers ETL jobs for the specified sources and date range.       |
| `/fetch-stripe`               | POST   | Manually fetches Stripe Checkout Sessions for a date range.       |
| `/fetch-meta`                 | POST   | Manually fetches Meta Ads insights for a date range.              |
| `/fetch-tiktok`               | POST   | Manually fetches TikTok Ads insights for a date range.            |
| `/fetch-mailerlite`           | POST   | Manually fetches MailerLite subscribers and groups.               |
| `/fetch-steam-sales`          | POST   | Fetches Steam sales and daily country wishlist activity.          |
| `/fetch-google-play`          | POST   | Imports Google Play estimated sales reports into `Google_Play`.   |
| `/fetch-google-play-installs` | POST   | Imports Google Play daily country installs into `Google_Play_Installs`. |
| `/import-steam-wishlist-csv` | POST   | Parses and imports a Steam wishlist CSV report.                   |
| `/metrics`                    | GET    | Returns aggregated metrics for the dashboard front‑end.            |

## Cron functions

Two scheduled functions automate data collection:

- **cron-hourly** – runs every hour to fetch Stripe, Meta, TikTok and MailerLite data for the last 48 hours.
- **cron-daily** – runs at 04:00 UTC daily to fetch Steam sales and wishlist activity and recheck the last 14 days of Google Play sales reports.
- **cron-play-installs** – runs at 04:30 UTC daily to recheck the last 14 days of Google Play installs reports.

## Meta Ads decision optimizer

The **Ad Optimizer** dashboard tab deliberately separates two evidence layers:

- exact Meta ad-set delivery (`Meta`) and ad-level creative performance (`Meta_Ads`);
- shared country-market economics from Steam, Google Play, Stripe, PayPal, MailerLite and telemetry.

Store revenue is never assigned to an individual ad set. When multiple ad sets deliver in the same country, the optimizer lowers recommendation confidence and uses country revenue only to decide whether the overall market budget should grow, hold or shrink. The Meta sync also creates `Meta_AdSets`, a daily read-only snapshot of budget, bid/cost-cap, delivery status, optimization and targeting configuration.

Reports automatically stop at yesterday to avoid comparing partial current-day Meta spend with delayed Steam revenue. Use **Copy AI Optimizer Packet** to export the compact ad-set actions, shared market signals, evidence and new-market candidates.

## Notes

* All timestamps are stored in UTC.  Convert to Europe/Paris in your front‑end when displaying dates.
* Primary email lookup values are hashed, but provider `raw` payloads may still contain personal data. Restrict sheet sharing and do not treat the dashboard PIN as protection for a publicly readable sheet.
* The script uses simple upsert logic to avoid duplicating rows when re‑ingesting overlapping data.
* See the source code in `lib/etl.js` and the SQL schema under `lib/sql/` for details on the data model.
