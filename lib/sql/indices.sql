-- Helpful indices for query performance

CREATE INDEX IF NOT EXISTS idx_stripe_orders_created ON stripe_orders(created_at);
CREATE INDEX IF NOT EXISTS idx_meta_insights_account ON meta_insights(account_id, date);
CREATE INDEX IF NOT EXISTS idx_tiktok_insights_adv ON tiktok_insights(advertiser_id, date);
CREATE INDEX IF NOT EXISTS idx_ml_subs_created ON mailerlite_subscribers(created_at);
CREATE INDEX IF NOT EXISTS idx_steam_sales_date ON steam_sales(date, app_id);
