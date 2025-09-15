-- Cursor table to track incremental ETL positions

CREATE TABLE IF NOT EXISTS etl_cursors (
  source TEXT PRIMARY KEY,
  since  TIMESTAMPTZ NOT NULL
);

-- Stripe Checkout Session orders
CREATE TABLE IF NOT EXISTS stripe_orders (
  id TEXT PRIMARY KEY,
  created_at TIMESTAMPTZ NOT NULL,
  amount BIGINT NOT NULL,
  currency TEXT NOT NULL,
  status TEXT NOT NULL,
  customer_email_hash TEXT,
  checkout_session_id TEXT,
  product_id TEXT,
  price_id TEXT,
  fbp TEXT,
  fbc TEXT,
  ttclid TEXT,
  country TEXT,
  metadata JSONB,
  raw JSONB,
  ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Meta Ads daily insights
CREATE TABLE IF NOT EXISTS meta_insights (
  date DATE NOT NULL,
  account_id TEXT NOT NULL,
  campaign_id TEXT,
  adset_id TEXT,
  ad_id TEXT,
  impressions BIGINT,
  clicks BIGINT,
  spend NUMERIC(14,4),
  purchases NUMERIC(14,4),
  purchase_value NUMERIC(14,4),
  cpm NUMERIC(14,4),
  cpc NUMERIC(14,4),
  roas NUMERIC(14,4),
  raw JSONB,
  ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (date, account_id, ad_id)
);

-- TikTok Ads daily insights
CREATE TABLE IF NOT EXISTS tiktok_insights (
  date DATE NOT NULL,
  advertiser_id TEXT NOT NULL,
  campaign_id TEXT,
  adgroup_id TEXT,
  ad_id TEXT,
  impressions BIGINT,
  clicks BIGINT,
  spend NUMERIC(14,4),
  conversions NUMERIC(14,4),
  conversion_value NUMERIC(14,4),
  cpm NUMERIC(14,4),
  cpc NUMERIC(14,4),
  roas NUMERIC(14,4),
  raw JSONB,
  ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (date, advertiser_id, ad_id)
);

-- MailerLite subscribers
CREATE TABLE IF NOT EXISTS mailerlite_subscribers (
  subscriber_id TEXT PRIMARY KEY,
  email_hash TEXT,
  status TEXT,
  created_at TIMESTAMPTZ,
  country TEXT,
  raw JSONB,
  ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- MailerLite group memberships
CREATE TABLE IF NOT EXISTS mailerlite_group_memberships (
  subscriber_id TEXT NOT NULL,
  group_id TEXT NOT NULL,
  added_at TIMESTAMPTZ,
  PRIMARY KEY (subscriber_id, group_id)
);

-- Steam sales (via API or CSV)
CREATE TABLE IF NOT EXISTS steam_sales (
  date DATE NOT NULL,
  app_id TEXT NOT NULL,
  country TEXT,
  currency TEXT,
  units INTEGER,
  gross_revenue NUMERIC(14,4),
  refunds INTEGER,
  net_units INTEGER,
  net_revenue NUMERIC(14,4),
  source TEXT,
  raw JSONB,
  ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (date, app_id, country, currency)
);

-- Steam wishlist events (CSV import)
CREATE TABLE IF NOT EXISTS steam_wishlist_events (
  date DATE NOT NULL,
  app_id TEXT NOT NULL,
  adds INTEGER,
  deletes INTEGER,
  purchases_from_wishlist INTEGER,
  source_file TEXT,
  ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (date, app_id)
);

-- Example materialized view to roll up Stripe revenue by day
CREATE MATERIALIZED VIEW IF NOT EXISTS orders_by_day AS
SELECT
  date_trunc('day', created_at)::date AS date,
  currency,
  SUM(amount) / 100.0 AS revenue
FROM stripe_orders
GROUP BY 1, 2;
