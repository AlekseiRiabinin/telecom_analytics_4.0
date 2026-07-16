-- 1.1. Dimension Tables (Hierarchy)
CREATE TABLE admin_divisions.emirate (
    emirate_id SERIAL PRIMARY KEY,
    name_en TEXT NOT NULL UNIQUE,
    name_ar TEXT
);

CREATE TABLE admin_divisions.city (
    city_id SERIAL PRIMARY KEY,
    emirate_id INT NOT NULL REFERENCES admin_divisions.emirate(emirate_id),
    name_en TEXT NOT NULL,
    name_ar TEXT,
    UNIQUE (emirate_id, name_en)
);

CREATE TABLE admin_divisions.sector (
    sector_id SERIAL PRIMARY KEY,
    city_id INT NOT NULL REFERENCES admin_divisions.city(city_id),
    name_en TEXT NOT NULL,
    name_ar TEXT,
    status TEXT,  -- 'Sector'
    UNIQUE (city_id, name_en)
);

CREATE TABLE admin_divisions.community (
    community_id SERIAL PRIMARY KEY,
    city_id INT NOT NULL REFERENCES admin_divisions.city(city_id),
    sector_id INT REFERENCES admin_divisions.sector(sector_id),
    name_en TEXT NOT NULL,
    name_ar TEXT,
    description TEXT,
    UNIQUE (city_id, name_en)
);

CREATE TABLE admin_divisions.district (
    district_id SERIAL PRIMARY KEY,
    community_id INT REFERENCES admin_divisions.community(community_id),
    sector_id INT REFERENCES admin_divisions.sector(sector_id),
    city_id INT REFERENCES admin_divisions.city(city_id),
    emirate_id INT REFERENCES admin_divisions.emirate(emirate_id),
    urbi_id TEXT UNIQUE,        -- payload.id from URBI
    name_en TEXT NOT NULL,
    name_ar TEXT,
    description TEXT
);


-- 1.2. Spatial Tables (Geometry)
CREATE TABLE spatial.district_geom (
    district_id INT PRIMARY KEY REFERENCES admin_divisions.district(district_id) ON DELETE CASCADE,
    geom geometry(MULTIPOLYGON, 4326) NOT NULL,
    area_km2 NUMERIC GENERATED ALWAYS AS (ST_Area(geom::geography) / 1e6) STORED
);
CREATE INDEX ON spatial.district_geom USING GIST (geom);

-- Optional
CREATE TABLE spatial.community_geom (
    community_id INT PRIMARY KEY REFERENCES admin_divisions.community(community_id) ON DELETE CASCADE,
    geom geometry(MULTIPOLYGON, 4326) NOT NULL,
    area_km2 NUMERIC GENERATED ALWAYS AS (ST_Area(geom::geography) / 1e6) STORED
);
CREATE INDEX ON spatial.community_geom USING GIST (geom);


-- 1.3. Population & Demographics
CREATE TABLE analytics.population (
    community_id INT PRIMARY KEY REFERENCES admin_divisions.community(community_id),
    total_population INT NOT NULL,
    density NUMERIC,
    updated_at DATE NOT NULL
);

CREATE TABLE analytics.sector_population (
    sector_id INT PRIMARY KEY REFERENCES admin_divisions.sector(sector_id),
    pop_2010 INT,
    pop_2015 INT,
    pop_2020 INT,
    pop_2023 INT
);

CREATE TABLE analytics.demographics (
    demo_id SERIAL PRIMARY KEY,
    community_id INT NOT NULL REFERENCES admin_divisions.community(community_id),
    nationality TEXT NOT NULL,
    population INT NOT NULL,
    percentage NUMERIC
);


-- 1.4. Companies (Active companies per community)
CREATE TABLE analytics.active_companies (
    community_id INT PRIMARY KEY REFERENCES admin_divisions.community(community_id),
    active_companies INT NOT NULL,
    updated_at DATE NOT NULL
);


-- 1.5. Real Estate Supply (m²)
CREATE TABLE analytics.supply (
    community_id INT PRIMARY KEY REFERENCES admin_divisions.community(community_id),
    total_supply_m2 NUMERIC NOT NULL,
    updated_at DATE NOT NULL
);


-- 1.6. Transactions (Sales / Lease)
CREATE TABLE facts.transactions (
    transaction_id BIGSERIAL PRIMARY KEY,
    district_id INT REFERENCES admin_divisions.district(district_id),
    community_id INT REFERENCES admin_divisions.community(community_id),
    property_type TEXT NOT NULL,
    price_aed NUMERIC NOT NULL,
    area_m2 NUMERIC,
    date DATE NOT NULL
);
CREATE INDEX ON facts.transactions (community_id, date);
CREATE INDEX ON facts.transactions (district_id, date);


-- 1.7. Materialized Analytics Views
-- 1Y statistics per community
CREATE MATERIALIZED VIEW analytics.mv_community_1y AS
SELECT
    c.community_id,
    SUM(t.price_aed) AS transactions_volume_1y,
    COUNT(*) AS transactions_count_1y
FROM facts.transactions t
JOIN admin_divisions.community c ON c.community_id = t.community_id
WHERE t.date >= CURRENT_DATE - INTERVAL '1 year'
GROUP BY c.community_id;

-- Top performing property types
CREATE MATERIALIZED VIEW analytics.mv_top_property_types AS
SELECT
    community_id,
    property_type,
    COUNT(*) AS transactions_count,
    AVG(price_aed / NULLIF(area_m2, 0)) AS avg_price_per_m2
FROM facts.transactions
WHERE date >= CURRENT_DATE - INTERVAL '1 year'
GROUP BY community_id, property_type;

-- District counts
CREATE MATERIALIZED VIEW analytics.mv_district_count AS
SELECT
    community_id,
    COUNT(*) AS district_count
FROM admin_divisions.district
GROUP BY community_id;

CREATE INDEX ON analytics.mv_district_count (community_id);


-- 1.8. API‑Friendly Flattened View
CREATE OR REPLACE VIEW api.v_admin_divisions AS
SELECT
    e.emirate_id,
    e.name_en AS emirate_name,

    ci.city_id,
    ci.name_en AS city_name,

    se.sector_id,
    se.name_en AS sector_name,

    co.community_id,
    co.name_en AS community_name,
    co.description,

    g.geom,
    g.area_km2,

    p.total_population,
    p.density,

    ac.active_companies,
    s.total_supply_m2,

    mv.transactions_volume_1y,
    mv.transactions_count_1y,

    dc.district_count

FROM admin_divisions.community co
JOIN admin_divisions.city ci ON ci.city_id = co.city_id
JOIN admin_divisions.emirate e ON e.emirate_id = ci.emirate_id
LEFT JOIN admin_divisions.sector se ON se.sector_id = co.sector_id
LEFT JOIN spatial.community_geom g ON g.community_id = co.community_id
LEFT JOIN analytics.population p ON p.community_id = co.community_id
LEFT JOIN analytics.active_companies ac ON ac.community_id = co.community_id
LEFT JOIN analytics.supply s ON s.community_id = co.community_id
LEFT JOIN analytics.mv_community_1y mv ON mv.community_id = co.community_id
LEFT JOIN analytics.mv_district_count dc ON dc.community_id = co.community_id;
