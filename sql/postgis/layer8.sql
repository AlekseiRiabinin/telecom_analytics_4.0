-- 8.1. FACT TABLE — Individual sale Transactions
CREATE TABLE sale_transactions.sale (
    sale_id BIGSERIAL PRIMARY KEY,

    -- Core transaction fields
    price_aed NUMERIC NOT NULL,
    area_m2 NUMERIC,
    price_per_m2 NUMERIC,

    transaction_date DATE NOT NULL,
    period TEXT,                           -- e.g., "2025-Q1"

    -- Classification fields
    usage_type TEXT,                       -- Commercial, Residential, etc.
    registration_type TEXT,                -- Off-plan, Ready, etc.
    transaction_type TEXT,                 -- Mortgage, Sale, etc.
    master_project TEXT,
    parking TEXT,                          -- Included, None, Any
    property_type TEXT,                    -- Apartment, Villa, Office, etc.

    -- Metadata
    subtitle TEXT,
    source TEXT,
    source_date DATE,

    -- Administrative anchors
    emirate_id INT REFERENCES admin_divisions.emirate(emirate_id),
    city_id INT REFERENCES admin_divisions.city(city_id),
    sector_id INT REFERENCES admin_divisions.sector(sector_id),
    community_id INT REFERENCES admin_divisions.community(community_id),
    district_id INT REFERENCES admin_divisions.district(district_id)
);
CREATE INDEX ON sale_transactions.sale (transaction_date);
CREATE INDEX ON sale_transactions.sale (community_id);
CREATE INDEX ON sale_transactions.sale (district_id);
CREATE INDEX ON sale_transactions.sale (usage_type);
CREATE INDEX ON sale_transactions.sale (price_per_m2);


-- 8.2. Spatial Geometry
CREATE TABLE spatial.sale_transactions_geom (
    sale_id BIGINT PRIMARY KEY REFERENCES sale_transactions.sale(sale_id) ON DELETE CASCADE,
    geom geometry(POINT, 4326) NOT NULL
);
CREATE INDEX ON spatial.sale_transactions_geom USING GIST (geom);


-- 8.3. Aggregated KPIs (Materialized View)
CREATE MATERIALIZED VIEW analytics.mv_sale_kpis AS
SELECT
    COUNT(*) AS total_transactions,
    SUM(price_aed) AS total_sale_volume_aed,
    SUM(area_m2) / 1e6 AS total_area_km2,
    AVG(price_per_m2) AS avg_price_per_m2
FROM sale_transactions.sale;


-- 8.4. Price Distribution (Histogram)
CREATE MATERIALIZED VIEW analytics.mv_sale_price_distribution AS
SELECT
    width_bucket(price_per_m2, 0, 100000, 50) AS bucket,
    COUNT(*) AS transactions_count,
    MIN(price_per_m2) AS bucket_min,
    MAX(price_per_m2) AS bucket_max
FROM sale_transactions.sale
GROUP BY bucket
ORDER BY bucket;


-- 8.5. sale by Usage (Pie Chart)
CREATE MATERIALIZED VIEW analytics.mv_sale_usage_distribution AS
SELECT
    usage_type,
    COUNT(*) AS transactions_count,
    SUM(price_aed) AS total_sale_volume,
    SUM(area_m2) AS total_area_m2,
    SUM(area_m2) * 1.0 /
        SUM(SUM(area_m2)) OVER () AS percentage
FROM sale_transactions.sale
GROUP BY usage_type;


-- 8.6. API View (Flattened, Clean, No Admin Fields Exposed)
CREATE OR REPLACE VIEW api.v_sale_transactions AS
SELECT
    s.sale_id,
    s.subtitle,
    s.price_aed,
    s.price_per_m2,
    s.area_m2,
    s.transaction_date,
    s.usage_type,
    s.registration_type,
    s.transaction_type,
    s.master_project,
    s.parking,
    s.property_type,
    s.source,
    s.source_date,

    g.geom,

    u.transactions_count AS usage_transactions,
    u.total_area_m2 AS usage_area_m2,
    u.percentage AS usage_percentage

FROM sale_transactions.sale s
LEFT JOIN spatial.sale_transactions_geom g ON g.sale_id = s.sale_id
LEFT JOIN analytics.mv_sale_usage_distribution u ON u.usage_type = s.usage_type;
