-- 3.1. Dimension Table — Master Developer
CREATE TABLE master_developers.developer (
    developer_id SERIAL PRIMARY KEY,
    developer_name TEXT NOT NULL,          -- e.g., Emaar, Nakheel, Meraas
    zone_name TEXT NOT NULL,               -- project or development name
    subtitle TEXT,
    description TEXT,

    total_samples INT,                     -- number of parcels/projects
    total_development_area_m2 NUMERIC,     -- total area of all projects
    total_development_value_aed NUMERIC,   -- total value of all projects
    projects_under_construction INT,       -- count of active projects

    source TEXT,                           -- "Dubai Digital Authority. Digital Pulse"
    source_date DATE,                      -- "2025-05-25"

    -- Administrative anchors (internal only)
    emirate_id INT REFERENCES admin_divisions.emirate(emirate_id),
    city_id INT REFERENCES admin_divisions.city(city_id),
    sector_id INT REFERENCES admin_divisions.sector(sector_id),
    community_id INT REFERENCES admin_divisions.community(community_id),
    district_id INT REFERENCES admin_divisions.district(district_id)
);


-- 3.2. Spatial Geometry
CREATE TABLE spatial.master_developers_geom (
    developer_id INT PRIMARY KEY REFERENCES master_developers.developer(developer_id) ON DELETE CASCADE,
    geom geometry(MULTIPOLYGON, 4326) NOT NULL,
    area_m2 NUMERIC GENERATED ALWAYS AS (ST_Area(geom::geography)) STORED
);
CREATE INDEX ON spatial.master_developers_geom USING GIST (geom);


-- 3.3. Market Share by Area (Pie Chart)
-- developer‑level aggregation, not per zone.
CREATE TABLE master_developers.market_share (
    developer_name TEXT PRIMARY KEY NOT NULL,
    area_m2 NUMERIC NOT NULL,
    percentage NUMERIC
);


-- 3.4. Completion Status (Pie Chart)
CREATE TABLE master_developers.completion_status (
    developer_id INT NOT NULL REFERENCES master_developers.developer(developer_id),
    status TEXT NOT NULL,                  -- 'Completed', 'In construction'
    area_m2 NUMERIC NOT NULL,
    percentage NUMERIC,
    PRIMARY KEY (developer_id, status)
);


-- 3.5. Materialized Analytics Views
-- Market Share (computed from zones)
CREATE MATERIALIZED VIEW analytics.mv_master_dev_market_share AS
SELECT
    developer_name,
    SUM(total_development_area_m2) AS area_m2,
    SUM(total_development_area_m2) * 1.0 /
        SUM(SUM(total_development_area_m2)) OVER () AS percentage
FROM master_developers.developer
GROUP BY developer_name;

-- Completion Status (computed from zone data)
CREATE MATERIALIZED VIEW analytics.mv_master_dev_completion AS
SELECT
    developer_id,
    status,
    area_m2,
    area_m2 * 1.0 /
        SUM(area_m2) OVER (PARTITION BY developer_id) AS percentage
FROM master_developers.completion_status;


-- 3.6. API View (Flattened, Clean, No Admin Fields Exposed)
CREATE OR REPLACE VIEW api.v_master_developers AS
SELECT
    d.developer_id,
    d.developer_name,
    d.zone_name,
    d.subtitle,
    d.description,

    d.total_samples,
    d.total_development_area_m2,
    d.total_development_value_aed,
    d.projects_under_construction,

    d.source,
    d.source_date,

    g.geom,
    g.area_m2 AS zone_polygon_area_m2,

    ms.area_m2 AS developer_market_area_m2,
    ms.percentage AS developer_market_share,

    cs.status AS completion_status,
    cs.area_m2 AS completion_area_m2,
    cs.percentage AS completion_percentage

FROM master_developers.developer d
JOIN spatial.master_developers_geom g ON g.developer_id = d.developer_id
LEFT JOIN analytics.mv_master_dev_market_share ms ON ms.developer_name = d.developer_name
LEFT JOIN analytics.mv_master_dev_completion cs ON cs.developer_id = d.developer_id;
