-- 13.1. Dimension Table — Companies
CREATE TABLE companies.company (
    company_id SERIAL PRIMARY KEY,
    company_name TEXT,
    subtitle TEXT,
    description TEXT,

    business_type TEXT,                    -- General education, Retail, etc.
    subtype TEXT,                          -- Early childhood development centers, etc.
    headcount INT,
    customer_rating NUMERIC,               -- 1–5 scale
    legal_form TEXT,                       -- Private Shareholding Company, etc.
    language_spoken TEXT,                  -- Japanese, English, Arabic, etc.
    branch_count INT,
    address TEXT,

    total_samples INT,

    source TEXT,                           -- "Dubai Digital Authority. Digital Pulse"
    source_date DATE,                      -- "2025-05-25"

    -- Administrative anchors (internal only)
    emirate_id INT REFERENCES admin_divisions.emirate(emirate_id),
    city_id INT REFERENCES admin_divisions.city(city_id),
    sector_id INT REFERENCES admin_divisions.sector(sector_id),
    community_id INT REFERENCES admin_divisions.community(community_id),
    district_id INT REFERENCES admin_divisions.district(district_id)
);


-- 13.2. Spatial Geometry
CREATE TABLE spatial.companies_geom (
    company_id INT PRIMARY KEY REFERENCES companies.company(company_id) ON DELETE CASCADE,
    geom geometry(POINT, 4326) NOT NULL
);
CREATE INDEX ON spatial.companies_geom USING GIST (geom);


-- 13.3. Company Type Distribution (Pie Chart)
CREATE MATERIALIZED VIEW analytics.mv_company_type_distribution AS
SELECT
    business_type,
    COUNT(*) AS companies_count,
    COUNT(*) * 1.0 /
        SUM(COUNT(*)) OVER () AS percentage
FROM companies.company
GROUP BY business_type;


-- 13.4. Company Size Distribution (Graph)
CREATE MATERIALIZED VIEW analytics.mv_company_size_distribution AS
SELECT
    width_bucket(headcount, 0, 1000, 10) AS headcount_bucket,
    MIN(headcount) AS bucket_min,
    MAX(headcount) AS bucket_max,
    COUNT(*) AS companies_count
FROM companies.company
GROUP BY headcount_bucket
ORDER BY headcount_bucket;


-- 13.5. Top Company Types (Top 3)
CREATE MATERIALIZED VIEW analytics.mv_company_top_types AS
SELECT
    business_type,
    COUNT(*) AS companies_count
FROM companies.company
GROUP BY business_type
ORDER BY companies_count DESC
LIMIT 3;


-- 13.6. Global KPIs
CREATE MATERIALIZED VIEW analytics.mv_company_kpis AS
SELECT
    COUNT(*) AS total_companies,
    AVG(headcount) AS avg_headcount,
    AVG(customer_rating) AS avg_customer_rating
FROM companies.company;


-- 13.7. API View (Flattened, Clean, No Admin Fields Exposed)
CREATE OR REPLACE VIEW api.v_companies AS
SELECT
    c.company_id,
    c.company_name,
    c.subtitle,
    c.description,

    c.business_type,
    c.subtype,
    c.headcount,
    c.customer_rating,
    c.legal_form,
    c.language_spoken,
    c.branch_count,
    c.address,

    c.total_samples,
    c.source,
    c.source_date,

    g.geom,

    td.business_type AS dist_business_type,
    td.companies_count AS dist_business_count,
    td.percentage AS dist_business_percentage,

    st.headcount_bucket,
    st.bucket_min,
    st.bucket_max,
    st.companies_count AS size_bucket_count,

    tt.business_type AS top_business_type,
    tt.companies_count AS top_business_count

FROM companies.company c
LEFT JOIN spatial.companies_geom g ON g.company_id = c.company_id
LEFT JOIN analytics.mv_company_type_distribution td ON td.business_type = c.business_type
LEFT JOIN analytics.mv_company_size_distribution st ON TRUE
LEFT JOIN analytics.mv_company_top_types tt ON tt.business_type = c.business_type;
