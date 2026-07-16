-- 11.1. Dimension Table — Points of Interest
CREATE TABLE points_of_interests.poi (
    poi_id SERIAL PRIMARY KEY,
    poi_name TEXT,
    subtitle TEXT,
    description TEXT,

    category TEXT,                         -- Shopping center, Park, etc.
    subcategory TEXT,                      -- Recreation area, Mall, etc.

    total_samples INT,
    avg_weekday_footfall NUMERIC,          -- ppl/hr
    avg_weekend_footfall NUMERIC,          -- ppl/hr
    visitor_profile TEXT,                  -- Residents, Tourists, Mixed

    source TEXT,                           -- "Dubai Digital Authority. Digital Pulse"
    source_date DATE,                      -- "2025-05-25"

    -- Administrative anchors (internal only)
    emirate_id INT REFERENCES admin_divisions.emirate(emirate_id),
    city_id INT REFERENCES admin_divisions.city(city_id),
    sector_id INT REFERENCES admin_divisions.sector(sector_id),
    community_id INT REFERENCES admin_divisions.community(community_id),
    district_id INT REFERENCES admin_divisions.district(district_id)
);


-- 11.2. Spatial Geometry
CREATE TABLE spatial.points_of_interests_geom (
    poi_id INT PRIMARY KEY REFERENCES points_of_interests.poi(poi_id) ON DELETE CASCADE,
    geom geometry(POINT, 4326) NOT NULL
);
CREATE INDEX ON spatial.points_of_interests_geom USING GIST (geom);


-- 11.3. Most Visited Locations (Top 3)
CREATE MATERIALIZED VIEW analytics.mv_poi_top_locations AS
SELECT
    poi_id,
    avg_weekday_footfall + avg_weekend_footfall AS combined_footfall
FROM points_of_interests.poi
ORDER BY combined_footfall DESC
LIMIT 3;


-- 11.4. Footfall Share by Category (Pie Chart)
CREATE MATERIALIZED VIEW analytics.mv_poi_category_distribution AS
SELECT
    category,
    SUM(avg_weekday_footfall + avg_weekend_footfall) AS total_footfall,
    SUM(avg_weekday_footfall + avg_weekend_footfall) * 1.0 /
        SUM(SUM(avg_weekday_footfall + avg_weekend_footfall)) OVER () AS percentage
FROM points_of_interests.poi
GROUP BY category;


-- 11.5. Global KPIs
CREATE MATERIALIZED VIEW analytics.mv_poi_kpis AS
SELECT
    COUNT(*) AS total_pois,
    AVG(avg_weekday_footfall) AS avg_weekday_footfall,
    AVG(avg_weekend_footfall) AS avg_weekend_footfall
FROM points_of_interests.poi;


-- 11.6. API View (Flattened, Clean, No Admin Fields Exposed)
CREATE OR REPLACE VIEW api.v_points_of_interests AS
SELECT
    p.poi_id,
    p.poi_name,
    p.subtitle,
    p.description,

    p.category,
    p.subcategory,
    p.total_samples,
    p.avg_weekday_footfall,
    p.avg_weekend_footfall,
    p.visitor_profile,

    p.source,
    p.source_date,

    g.geom,

    cd.category AS dist_category,
    cd.total_footfall AS dist_category_footfall,
    cd.percentage AS dist_category_percentage,

    tl.poi_id AS top_poi_id,
    tl.combined_footfall AS top_poi_combined_footfall

FROM points_of_interests.poi p
LEFT JOIN spatial.points_of_interests_geom g ON g.poi_id = p.poi_id
LEFT JOIN analytics.mv_poi_category_distribution cd ON cd.category = p.category
LEFT JOIN analytics.mv_poi_top_locations tl ON tl.poi_id = p.poi_id;
