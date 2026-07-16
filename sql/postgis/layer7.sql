-- 7.1. Dimension Table — Economic Zones
CREATE TABLE economic_zones.zone (
    zone_id SERIAL PRIMARY KEY,
    zone_name TEXT,
    subtitle TEXT,
    description TEXT,

    zone_type TEXT NOT NULL,               -- Commercial, Industrial, Free Zone, etc.
    total_area_km2 NUMERIC,                -- authoritative area of this zone
    total_samples INT,                     -- number of parcels/samples
    zones_count INT,                       -- number of sub-zones or components

    license_requirements TEXT,
    restricted_activities TEXT,
    authority TEXT,                        -- e.g., "Dubai Airport Free Zone Authority"

    source TEXT,                           -- "Dubai Digital Authority. Digital Pulse"
    source_date DATE,                      -- "2025-05-25"

    -- Administrative anchors (internal only)
    emirate_id INT REFERENCES admin_divisions.emirate(emirate_id),
    city_id INT REFERENCES admin_divisions.city(city_id),
    sector_id INT REFERENCES admin_divisions.sector(sector_id),
    community_id INT REFERENCES admin_divisions.community(community_id),
    district_id INT REFERENCES admin_divisions.district(district_id)
);


-- 7.2. Spatial Geometry
CREATE TABLE spatial.economic_zones_geom (
    zone_id INT PRIMARY KEY REFERENCES economic_zones.zone(zone_id) ON DELETE CASCADE,
    geom geometry(MULTIPOLYGON, 4326) NOT NULL,
    area_km2 NUMERIC GENERATED ALWAYS AS (ST_Area(geom::geography) / 1e6) STORED
);
CREATE INDEX ON spatial.economic_zones_geom USING GIST (geom);


-- 7.3. Zone Type Distribution (Pie Chart)
CREATE TABLE economic_zones.distribution (
    zone_type TEXT PRIMARY KEY NOT NULL,
    area_m2 NUMERIC NOT NULL,
    percentage NUMERIC
);


-- 7.4. Top Zones by Area (Top 3)
CREATE MATERIALIZED VIEW analytics.mv_economic_zones_top AS
SELECT
    zone_id,
    total_area_km2
FROM economic_zones.zone
ORDER BY total_area_km2 DESC
LIMIT 3;


-- 7.5. Zone Type Distribution (Computed)
CREATE MATERIALIZED VIEW analytics.mv_economic_zones_distribution AS
SELECT
    zone_type,
    SUM(total_area_km2 * 1e6) AS area_m2,
    SUM(total_area_km2 * 1e6) * 1.0 /
        SUM(SUM(total_area_km2 * 1e6)) OVER () AS percentage
FROM economic_zones.zone
GROUP BY zone_type;


-- 7.6. API View (Flattened, Clean, No Admin Fields Exposed)
CREATE OR REPLACE VIEW api.v_economic_zones AS
SELECT
    z.zone_id,
    z.zone_name,
    z.subtitle,
    z.description,

    z.zone_type,
    z.total_area_km2,
    z.total_samples,
    z.zones_count,

    z.license_requirements,
    z.restricted_activities,
    z.authority,

    z.source,
    z.source_date,

    g.geom,
    g.area_km2 AS polygon_area_km2,

    d.area_m2 AS zone_type_area_m2,
    d.percentage AS zone_type_percentage,

    tz.zone_id AS top_zone_id,
    tz.total_area_km2 AS top_zone_area_km2

FROM economic_zones.zone z
JOIN spatial.economic_zones_geom g ON g.zone_id = z.zone_id
LEFT JOIN analytics.mv_economic_zones_distribution d ON d.zone_type = z.zone_type
LEFT JOIN analytics.mv_economic_zones_top tz ON tz.zone_id = z.zone_id;
