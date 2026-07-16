-- 14.1. Dimension Table — Buildings
CREATE TABLE buildings.building (
    building_id SERIAL PRIMARY KEY,
    building_name TEXT,
    subtitle TEXT,
    description TEXT,

    total_samples INT,
    floor_area_m2 NUMERIC,
    properties_count INT,
    completion_year INT,
    construction_status TEXT,              -- Constructed, Under construction, Under repairing
    building_function TEXT,                -- Construction and repair, etc.
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


-- 14.2. Spatial Geometry
CREATE TABLE spatial.buildings_geom (
    building_id INT PRIMARY KEY REFERENCES buildings.building(building_id) ON DELETE CASCADE,
    geom geometry(MULTIPOLYGON, 4326) NOT NULL,
    area_km2 NUMERIC GENERATED ALWAYS AS (ST_Area(geom::geography) / 1e6) STORED
);
CREATE INDEX ON spatial.buildings_geom USING GIST (geom);


-- 14.3. Building Age Profile (Pie Chart)
CREATE MATERIALIZED VIEW analytics.mv_building_age_profile AS
SELECT
    CASE
        WHEN EXTRACT(YEAR FROM CURRENT_DATE) - completion_year <= 5 THEN 'New (0-5 years)'
        WHEN EXTRACT(YEAR FROM CURRENT_DATE) - completion_year <= 15 THEN 'Modern (5-15 years)'
        ELSE 'Mature (15+ years)'
    END AS age_group,
    COUNT(*) AS buildings_count,
    COUNT(*) * 1.0 / SUM(COUNT(*)) OVER () AS percentage
FROM buildings.building
GROUP BY age_group;


-- 14.4. Construction Status Distribution
CREATE MATERIALIZED VIEW analytics.mv_building_status_distribution AS
SELECT
    construction_status,
    COUNT(*) AS buildings_count
FROM buildings.building
GROUP BY construction_status;


-- 14.5. Global KPIs
CREATE MATERIALIZED VIEW analytics.mv_building_kpis AS
SELECT
    COUNT(*) AS total_buildings,
    SUM(floor_area_m2) AS total_floor_area_m2,
    SUM(properties_count) AS total_properties_count,
    AVG(completion_year) AS avg_completion_year
FROM buildings.building;


-- 14.6. Building age group
CREATE MATERIALIZED VIEW analytics.mv_building_age_group AS
SELECT
    building_id,
    CASE
        WHEN EXTRACT(YEAR FROM CURRENT_DATE) - completion_year <= 5 THEN 'New (0-5 years)'
        WHEN EXTRACT(YEAR FROM CURRENT_DATE) - completion_year <= 15 THEN 'Modern (5-15 years)'
        ELSE 'Mature (15+ years)'
    END AS age_group
FROM buildings.building;


-- 14.7. API View (Flattened, Clean, No Admin Fields Exposed)
CREATE OR REPLACE VIEW api.v_buildings AS
SELECT
    b.building_id,
    b.building_name,
    b.subtitle,
    b.description,

    b.floor_area_m2,
    b.properties_count,
    b.completion_year,
    b.construction_status,
    b.building_function,
    b.address,

    b.total_samples,
    b.source,
    b.source_date,

    g.geom,
    g.area_km2 AS polygon_area_km2,

    ag.age_group

FROM buildings.building b
LEFT JOIN spatial.buildings_geom g ON g.building_id = b.building_id
LEFT JOIN analytics.mv_building_age_group ag ON ag.building_id = b.building_id;
