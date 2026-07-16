-- 4.1. Dimension Table — Cadastral Plots
CREATE TABLE cadastral_maps.plot (
    plot_id SERIAL PRIMARY KEY,
    plot_name TEXT,
    subtitle TEXT,
    description TEXT,

    land_use TEXT,                     -- Industrial, Commercial, Residential, etc.
    land_area_m2 NUMERIC,              -- area of the land plot in m²

    total_samples INT,                 -- number of raw samples (optional)
    source TEXT,                       -- "Dubai Digital Authority. Digital Pulse"
    source_date DATE,                  -- "2025-05-25"

    -- Administrative anchors (internal only)
    emirate_id INT REFERENCES admin_divisions.emirate(emirate_id),
    city_id INT REFERENCES admin_divisions.city(city_id),
    sector_id INT REFERENCES admin_divisions.sector(sector_id),
    community_id INT REFERENCES admin_divisions.community(community_id),
    district_id INT REFERENCES admin_divisions.district(district_id)
);


-- 4.2. Spatial Geometry
CREATE TABLE spatial.cadastral_maps_geom (
    plot_id INT PRIMARY KEY REFERENCES cadastral_maps.plot(plot_id) ON DELETE CASCADE,
    geom geometry(MULTIPOLYGON, 4326) NOT NULL,
    area_m2 NUMERIC GENERATED ALWAYS AS (ST_Area(geom::geography)) STORED
);
CREATE INDEX ON spatial.cadastral_maps_geom USING GIST (geom);


-- 4.3. Materialized Analytics Views
-- Total number of cadastral plots
CREATE MATERIALIZED VIEW analytics.mv_cadastral_plot_count AS
SELECT COUNT(*) AS total_plots
FROM cadastral_maps.plot;

-- Index (not required but good practice)
CREATE INDEX ON analytics.mv_cadastral_plot_count ((total_plots));


-- 4.4. API View (Flattened, Clean, No Admin Fields Exposed)
CREATE OR REPLACE VIEW api.v_cadastral_maps AS
SELECT
    p.plot_id,
    p.plot_name,
    p.subtitle,
    p.description,

    p.land_use,
    p.land_area_m2,

    p.source,
    p.source_date,

    g.geom,
    g.area_m2 AS plot_polygon_area_m2,

    c.total_plots AS total_cadastral_samples

FROM cadastral_maps.plot p
JOIN spatial.cadastral_maps_geom g ON g.plot_id = p.plot_id
LEFT JOIN analytics.mv_cadastral_plot_count c ON TRUE;
