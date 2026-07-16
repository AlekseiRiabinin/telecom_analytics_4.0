-- 15.1. FACT TABLE — Supply Listings
CREATE TABLE real_estate_supply.supply (
    supply_id SERIAL PRIMARY KEY,
    supply_name TEXT,
    subtitle TEXT,
    description TEXT,

    usage_type TEXT,                       -- Commercial, Residential, Industrial, etc.
    listing_type TEXT,                     -- Sale, Rent
    subtype TEXT,                          -- Office, Warehouse, Retail, etc.
    current_usage TEXT,                    -- Current usage of the property

    price_per_m2 NUMERIC,
    total_area_m2 NUMERIC,
    asking_price_aed NUMERIC,              -- For sale listings
    asking_rent_aed NUMERIC,               -- For rent listings
    estimated_cap_rate NUMERIC,            -- %

    price_range_min_aed NUMERIC,
    price_range_max_aed NUMERIC,

    total_samples INT,

    address TEXT,

    source TEXT,                           -- "Dubai Digital Authority. Digital Pulse"
    source_date DATE,                      -- "2025-05-25"

    -- Administrative anchors (internal only)
    emirate_id INT REFERENCES admin_divisions.emirate(emirate_id),
    city_id INT REFERENCES admin_divisions.city(city_id),
    sector_id INT REFERENCES admin_divisions.sector(sector_id),
    community_id INT REFERENCES admin_divisions.community(community_id),
    district_id INT REFERENCES admin_divisions.district(district_id)
);


-- 15.2. Spatial Geometry
CREATE TABLE spatial.real_estate_supply_geom (
    supply_id INT PRIMARY KEY REFERENCES real_estate_supply.supply(supply_id) ON DELETE CASCADE,
    geom geometry(POINT, 4326) NOT NULL
);
CREATE INDEX ON spatial.real_estate_supply_geom USING GIST (geom);


-- 15.3. Supply by Listing Type (Pie Chart)
CREATE MATERIALIZED VIEW analytics.mv_supply_listing_type AS
SELECT
    listing_type,
    COUNT(*) AS listings_count,
    SUM(total_area_m2) AS total_area_m2,
    SUM(total_area_m2) * 1.0 / SUM(SUM(total_area_m2)) OVER () AS percentage
FROM real_estate_supply.supply
GROUP BY listing_type;


-- 15.4. Top Subtypes (Top 3)
CREATE MATERIALIZED VIEW analytics.mv_supply_top_subtypes AS
SELECT
    subtype,
    COUNT(*) AS listings_count
FROM real_estate_supply.supply
GROUP BY subtype
ORDER BY listings_count DESC
LIMIT 3;


-- 15.5. Global KPIs
CREATE MATERIALIZED VIEW analytics.mv_supply_kpis AS
SELECT
    COUNT(*) AS total_listings,
    AVG(price_per_m2) AS avg_price_per_m2,
    SUM(total_area_m2) AS total_listed_area_m2,
    AVG(estimated_cap_rate) AS avg_cap_rate
FROM real_estate_supply.supply;


-- 15.6. API View (Flattened, Clean, No Admin Fields Exposed)
CREATE OR REPLACE VIEW api.v_real_estate_supply AS
SELECT
    s.supply_id,
    s.supply_name,
    s.subtitle,
    s.description,

    s.usage_type,
    s.listing_type,
    s.subtype,
    s.current_usage,

    s.price_per_m2,
    s.total_area_m2,
    s.asking_price_aed,
    s.asking_rent_aed,
    s.estimated_cap_rate,
    s.price_range_min_aed,
    s.price_range_max_aed,

    s.total_samples,
    s.address,
    s.source,
    s.source_date,

    g.geom,

    lt.listing_type AS dist_listing_type,
    lt.listings_count AS dist_listing_count,
    lt.total_area_m2 AS dist_listing_area_m2,
    lt.percentage AS dist_listing_percentage,

    ts.subtype AS top_subtype,
    ts.listings_count AS top_subtype_count

FROM real_estate_supply.supply s
LEFT JOIN spatial.real_estate_supply_geom g ON g.supply_id = s.supply_id
LEFT JOIN analytics.mv_supply_listing_type lt ON lt.listing_type = s.listing_type
LEFT JOIN analytics.mv_supply_top_subtypes ts ON ts.subtype = s.subtype;
