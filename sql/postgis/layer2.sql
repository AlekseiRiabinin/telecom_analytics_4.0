-- 2.1. Dimension Table — Development Plan
CREATE TABLE development_plans.plan (
    plan_id SERIAL PRIMARY KEY,
    plan_name TEXT NOT NULL,
    subtitle TEXT,
    description TEXT,
    source TEXT,          -- "Dubai Digital Authority. Digital Pulse"
    source_date DATE,     -- "2025-05-25"

    -- Administrative anchors (internal only)
    emirate_id INT REFERENCES admin_divisions.emirate(emirate_id),
    city_id INT REFERENCES admin_divisions.city(city_id),
    sector_id INT REFERENCES admin_divisions.sector(sector_id),
    community_id INT REFERENCES admin_divisions.community(community_id),
    district_id INT REFERENCES admin_divisions.district(district_id)
);


-- 2.2. Spatial Geometry
CREATE TABLE spatial.development_plans_geom (
    plan_id INT PRIMARY KEY REFERENCES development_plans.plan(plan_id) ON DELETE CASCADE,
    geom geometry(MULTIPOLYGON, 4326) NOT NULL,
    area_km2 NUMERIC GENERATED ALWAYS AS (ST_Area(geom::geography) / 1e6) STORED
);
CREATE INDEX ON spatial.development_plans_geom USING GIST (geom);


-- 2.3. Usage Categories (Commercial, Residential, Industrial, etc.)
CREATE TABLE development_plans.usage_category (
    usage_id SERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE
);


-- 2.4. Zone Usage Mix (Many‑to‑Many)
CREATE TABLE development_plans.zone_usage (
    plan_id INT NOT NULL REFERENCES development_plans.plan(plan_id),
    usage_id INT NOT NULL REFERENCES development_plans.usage_category(usage_id),
    area_m2 NUMERIC NOT NULL,
    PRIMARY KEY (plan_id, usage_id)
);


-- 2.5. Materialized Analytics Views
-- Usage Mix (percentage per zone)
CREATE MATERIALIZED VIEW analytics.mv_plan_usage_mix AS
SELECT
    zu.plan_id,
    u.name AS usage_name,
    zu.area_m2,
    zu.area_m2 / SUM(zu.area_m2) OVER (PARTITION BY zu.plan_id) AS percentage
FROM development_plans.zone_usage zu
JOIN development_plans.usage_category u ON u.usage_id = zu.usage_id;

-- Top Zones by Area 
CREATE MATERIALIZED VIEW analytics.mv_plan_top_zones AS
SELECT
    plan_id,
    area_km2
FROM spatial.development_plans_geom
ORDER BY area_km2 DESC
LIMIT 3;


-- 2.6. API View (Flattened, Clean, No Admin Fields Exposed)
CREATE OR REPLACE VIEW api.v_development_plans AS
SELECT
    p.plan_id,
    p.plan_name,
    p.subtitle,
    p.description,
    p.source,
    p.source_date,

    g.geom,
    g.area_km2 AS total_zone_area_km2,          -- total polygon area

    u.usage_name AS land_use_category,          -- e.g., Commercial, Residential
    u.area_m2 AS land_use_area_m2,              -- area for this specific usage
    u.percentage AS land_use_percentage         -- share of zone area
FROM development_plans.plan p
JOIN spatial.development_plans_geom g ON g.plan_id = p.plan_id
LEFT JOIN analytics.mv_plan_usage_mix u ON u.plan_id = p.plan_id;
