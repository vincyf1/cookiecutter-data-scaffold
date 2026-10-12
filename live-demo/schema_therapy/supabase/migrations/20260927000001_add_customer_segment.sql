-- Existing demo customers start as bronze until the seed assigns their tier.
ALTER TABLE demo_source.customers
    ADD COLUMN IF NOT EXISTS customer_segment TEXT NOT NULL DEFAULT 'bronze'
    CHECK (customer_segment IN ('bronze', 'silver', 'gold'));
