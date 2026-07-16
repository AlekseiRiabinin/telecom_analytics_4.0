-- 10.1. Dimension Table — Population Areas
CREATE TABLE population.population (
    population_id SERIAL PRIMARY KEY,
    population_name TEXT,
    subtitle TEXT,
    description TEXT,

    total_samples INT,
    total_population INT,
    density_ppl_km2 NUMERIC,
    residents INT,
    workforce INT,

    source TEXT,                           -- "Dubai Digital Authority. Digital Pulse"
    source_date DATE,                      -- "2025-05-25"

    -- Administrative anchors (internal only)
    emirate_id INT REFERENCES admin_divisions.emirate(emirate_id),
    city_id INT REFERENCES admin_divisions.city(city_id),
    sector_id INT REFERENCES admin_divisions.sector(sector_id),
    community_id INT REFERENCES admin_divisions.community(community_id),
    district_id INT REFERENCES admin_divisions.district(district_id)
);


-- 10.2. Spatial Geometry
CREATE TABLE spatial.population_geom (
    population_id INT PRIMARY KEY REFERENCES population.population(population_id) ON DELETE CASCADE,
    geom geometry(MULTIPOLYGON, 4326) NOT NULL,
    area_km2 NUMERIC GENERATED ALWAYS AS (ST_Area(geom::geography) / 1e6) STORED
);
CREATE INDEX ON spatial.population_geom USING GIST (geom);


-- 10.3. Demographics Distribution (Pie Chart)
CREATE TABLE population.demographics (
    population_id INT REFERENCES population.population(population_id),
    nationality_group TEXT NOT NULL,       -- Emirati, Pakistani, Indian, Other
    people_count INT NOT NULL,
    percentage NUMERIC,
    PRIMARY KEY (population_id, nationality_group)
);


-- 10.4. Age Distribution (Pie Chart)
CREATE TABLE population.age_distribution (
    population_id INT REFERENCES population.population(population_id),
    age_group TEXT NOT NULL,               -- 0-16, 16-25, 25-50, 50-100
    people_count INT NOT NULL,
    percentage NUMERIC,
    PRIMARY KEY (population_id, age_group)
);


-- 10.5. Gender Split (Pie Chart)
CREATE TABLE population.gender_split (
    population_id INT REFERENCES population.population(population_id),
    gender TEXT NOT NULL,                  -- Male, Female
    people_count INT NOT NULL,
    percentage NUMERIC,
    PRIMARY KEY (population_id, gender)
);


-- 10.6. Materialized Views (Aggregated KPIs)
CREATE MATERIALIZED VIEW analytics.mv_population_kpis AS
SELECT
    COUNT(*) AS total_areas,
    SUM(total_population) AS total_population,
    SUM(residents) AS total_residents,
    SUM(workforce) AS total_workforce
FROM population.population;


-- 10.7. API View (Flattened, Clean, No Admin Fields Exposed)
CREATE OR REPLACE VIEW api.v_population AS
SELECT
    p.population_id,
    p.population_name,
    p.subtitle,
    p.description,

    p.total_samples,
    p.total_population,
    p.density_ppl_km2,
    p.residents,
    p.workforce,

    p.source,
    p.source_date,

    g.geom,
    g.area_km2 AS polygon_area_km2,

    d.nationality_group,
    d.people_count AS nationality_people,
    d.percentage AS nationality_percentage,

    a.age_group,
    a.people_count AS age_people,
    a.percentage AS age_percentage,

    gs.gender,
    gs.people_count AS gender_people,
    gs.percentage AS gender_percentage

FROM population.population p
JOIN spatial.population_geom g ON g.population_id = p.population_id
LEFT JOIN population.demographics d ON d.population_id = p.population_id
LEFT JOIN population.age_distribution a ON a.population_id = p.population_id
LEFT JOIN population.gender_split gs ON gs.population_id = p.population_id;
