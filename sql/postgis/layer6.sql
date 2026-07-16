-- 6.1. Dimension Table — Permitted Land Use
CREATE TABLE land_use.use (
    use_id SERIAL PRIMARY KEY,
    use_name TEXT,
    subtitle TEXT,
    description TEXT,

    permitted_usage TEXT NOT NULL,          -- Agricultural, Commercial, Industrial, etc.
    total_area_km2 NUMERIC,                 -- authoritative area of this use
    total_samples INT,                      -- number of parcels/samples

    source TEXT,                            -- "Dubai Digital Authority. Digital Pulse"
    source_date DATE,                       -- "2025-05-25"

    -- Administrative anchors (internal only)
    emirate_id INT REFERENCES admin_divisions.emirate(emirate_id),
    city_id INT REFERENCES admin_divisions.city(city_id),
    sector_id INT REFERENCES admin_divisions.sector(sector_id),
    community_id INT REFERENCES admin_divisions.community(community_id),
    district_id INT REFERENCES admin_divisions.district(district_id)
);


-- 6.2. Spatial Geometry
CREATE TABLE spatial.land_use_geom (
    use_id INT PRIMARY KEY REFERENCES land_use.use(use_id) ON DELETE CASCADE,
    geom geometry(MULTIPOLYGON, 4326) NOT NULL,
    area_km2 NUMERIC GENERATED ALWAYS AS (ST_Area(geom::geography) / 1e6) STORED
);
CREATE INDEX ON spatial.land_use_geom USING GIST (geom);


-- 6.3. Land‑Use Distribution (Pie Chart)
CREATE TABLE land_use.distribution (
    permitted_usage TEXT PRIMARY KEY NOT NULL,
    area_m2 NUMERIC NOT NULL,
    percentage NUMERIC
);


-- 6.4. Top uses by Area (Top 3)
CREATE MATERIALIZED VIEW analytics.mv_land_use_top_uses AS
SELECT
    use_id,
    total_area_km2
FROM land_use.use
ORDER BY total_area_km2 DESC
LIMIT 3;


-- 6.5. Land‑Use Distribution (Computed)
CREATE MATERIALIZED VIEW analytics.mv_land_use_distribution AS
SELECT
    permitted_usage,
    SUM(total_area_km2 * 1e6) AS area_m2,
    SUM(total_area_km2 * 1e6) * 1.0 /
        SUM(SUM(total_area_km2 * 1e6)) OVER () AS percentage
FROM land_use.use
GROUP BY permitted_usage;


-- 6.6. API View (Flattened, Clean, No Admin Fields Exposed)
CREATE OR REPLACE VIEW api.v_land_use AS
SELECT
    u.use_id,
    u.use_name,
    u.subtitle,
    u.description,

    u.permitted_usage,
    u.total_area_km2,
    u.total_samples,

    u.source,
    u.source_date,

    g.geom,
    g.area_km2 AS polygon_area_km2,

    d.area_m2 AS usage_area_m2,
    d.percentage AS usage_percentage,

    tu.use_id AS top_use_id,
    tu.total_area_km2 AS top_use_area_km2

FROM land_use.use u
JOIN spatial.land_use_geom g ON g.use_id = u.use_id
LEFT JOIN analytics.mv_land_use_distribution d ON d.permitted_usage = u.permitted_usage
LEFT JOIN analytics.mv_land_use_top_uses tu ON tu.use_id = u.use_id;
