-- 12.1. Dimension Table — Logistics Hubs
CREATE TABLE freight_logistics.logistics (
    logistics_id SERIAL PRIMARY KEY,
    logistics_name TEXT,
    subtitle TEXT,
    description TEXT,

    total_samples INT,
    storage_area_m2 NUMERIC,
    annual_throughput_mt NUMERIC,          -- Mt/yr

    cargo_type TEXT,                       -- General Cargo, Bulk, Container, etc.
    operational_status TEXT,               -- Operational, Under Construction, etc.
    storage_specialization TEXT,           -- Refrigerated, Dry, Hazardous, etc.
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


-- 12.2. Spatial Geometry
CREATE TABLE spatial.freight_logistics_geom (
    logistics_id INT PRIMARY KEY REFERENCES freight_logistics.logistics(logistics_id) ON DELETE CASCADE,
    geom geometry(POINT, 4326) NOT NULL
);
CREATE INDEX ON spatial.freight_logistics_geom USING GIST (geom);


-- 12.3. Top Hubs by Throughput (Top 3)
CREATE MATERIALIZED VIEW analytics.mv_logistics_top_hubs AS
SELECT
    logistics_id,
    annual_throughput_mt
FROM freight_logistics.logistics
ORDER BY annual_throughput_mt DESC
LIMIT 3;


-- 12.4. Top Types by Throughput (Top 3)
CREATE MATERIALIZED VIEW analytics.mv_logistics_top_types AS
SELECT
    cargo_type,
    SUM(annual_throughput_mt) AS total_throughput_mt
FROM freight_logistics.logistics
GROUP BY cargo_type
ORDER BY total_throughput_mt DESC
LIMIT 3;


-- 12.5. Category Distribution (Pie Chart)
CREATE MATERIALIZED VIEW analytics.mv_logistics_type_distribution AS
SELECT
    cargo_type,
    SUM(annual_throughput_mt) AS total_throughput_mt,
    SUM(annual_throughput_mt) * 1.0 /
        SUM(SUM(annual_throughput_mt)) OVER () AS percentage
FROM freight_logistics.logistics
GROUP BY cargo_type;


-- 12.6. API View (Flattened, Clean, No Admin Fields Exposed)
CREATE OR REPLACE VIEW api.v_freight_logistics AS
SELECT
    l.logistics_id,
    l.logistics_name,
    l.subtitle,
    l.description,

    l.total_samples,
    l.storage_area_m2,
    l.annual_throughput_mt,
    l.cargo_type,
    l.operational_status,
    l.storage_specialization,
    l.address,

    l.source,
    l.source_date,

    g.geom,

    td.cargo_type AS dist_cargo_type,
    td.total_throughput_mt AS dist_cargo_throughput,
    td.percentage AS dist_cargo_percentage,

    th.logistics_id AS top_hub_id,
    th.annual_throughput_mt AS top_hub_throughput,

    tt.cargo_type AS top_type,
    tt.total_throughput_mt AS top_type_throughput

FROM freight_logistics.logistics l
LEFT JOIN spatial.freight_logistics_geom g ON g.logistics_id = l.logistics_id
LEFT JOIN analytics.mv_logistics_type_distribution td ON td.cargo_type = l.cargo_type
LEFT JOIN analytics.mv_logistics_top_hubs th ON th.logistics_id = l.logistics_id
LEFT JOIN analytics.mv_logistics_top_types tt ON tt.cargo_type = l.cargo_type;
