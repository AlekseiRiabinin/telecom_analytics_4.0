-- 5.1. Dimension Table — Ownership ownerships
CREATE TABLE ownership_types.ownership (
    ownership_id SERIAL PRIMARY KEY,
    ownership_name TEXT,
    subtitle TEXT,
    description TEXT,

    ownership_types TEXT NOT NULL,         -- Freehold, Leasehold, Musataha, etc.
    ownership_regimes INT,
    total_area_km2 NUMERIC,                -- total area of this ownership ownership
    total_samples INT,                     -- number of parcels/samples

    source TEXT,                           -- "Dubai Digital Authority. Digital Pulse"
    source_date DATE,                      -- "2025-05-25"

    -- Administrative anchors (internal only)
    emirate_id INT REFERENCES admin_divisions.emirate(emirate_id),
    city_id INT REFERENCES admin_divisions.city(city_id),
    sector_id INT REFERENCES admin_divisions.sector(sector_id),
    community_id INT REFERENCES admin_divisions.community(community_id),
    district_id INT REFERENCES admin_divisions.district(district_id)
);


-- 5.2. Spatial Geometry
CREATE TABLE spatial.ownership_types_geom (
    ownership_id INT PRIMARY KEY REFERENCES ownership_types.ownership(ownership_id) ON DELETE CASCADE,
    geom geometry(MULTIPOLYGON, 4326) NOT NULL,
    area_km2 NUMERIC GENERATED ALWAYS AS (ST_Area(geom::geography) / 1e6) STORED
);
CREATE INDEX ON spatial.ownership_types_geom USING GIST (geom);


-- 5.3. Ownership Distribution (Pie Chart)
CREATE TABLE ownership_types.distribution (
    ownership_types TEXT PRIMARY KEY NOT NULL,
    area_m2 NUMERIC NOT NULL,
    percentage NUMERIC
);


-- 5.4. Top ownerships by Area (Top 3)
CREATE MATERIALIZED VIEW analytics.mv_ownership_top_ownerships AS
SELECT
    ownership_id,
    total_area_km2
FROM ownership_types.ownership
ORDER BY total_area_km2 DESC
LIMIT 3;


-- 5.5. Ownership Distribution (Computed)
CREATE MATERIALIZED VIEW analytics.mv_ownership_distribution AS
SELECT
    ownership_types,
    SUM(total_area_km2 * 1e6) AS area_m2,
    SUM(total_area_km2 * 1e6) * 1.0 /
        SUM(SUM(total_area_km2 * 1e6)) OVER () AS percentage
FROM ownership_types.ownership
GROUP BY ownership_types;


-- 5.6. API View (Flattened, Clean, No Admin Fields Exposed)
CREATE OR REPLACE VIEW api.v_ownership_types AS
SELECT
    o.ownership_id,
    o.ownership_name,
    o.subtitle,
    o.description,

    o.ownership_types,
    o.ownership_regimes,
    o.total_area_km2,
    o.total_samples,

    o.source,
    o.source_date,

    g.geom,
    g.area_km2 AS polygon_area_km2,

    d.area_m2 AS ownership_area_m2,
    d.percentage AS ownership_percentage,

    tow.ownership_id AS top_ownership_id,
    tow.total_area_km2 AS top_ownership_area_km2

FROM ownership_types.ownership o
JOIN spatial.ownership_types_geom g ON g.ownership_id = o.ownership_id
LEFT JOIN analytics.mv_ownership_distribution d ON d.ownership_types = o.ownership_types
LEFT JOIN analytics.mv_ownership_top_ownerships tow ON tow.ownership_id = o.ownership_id;
