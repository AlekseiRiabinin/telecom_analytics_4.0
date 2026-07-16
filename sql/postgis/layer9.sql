-- 9.1. FACT TABLE — Lease Transactions
CREATE TABLE IF NOT EXISTS lease_transactions.lease (
    lease_id BIGSERIAL PRIMARY KEY,

    -- Core lease fields
    annual_rent_aed NUMERIC NOT NULL,
    total_rent_aed NUMERIC,
    area_m2 NUMERIC,
    rent_per_m2 NUMERIC,

    contract_duration TEXT,                -- "1 year 5 months"
    contract_type TEXT,                    -- New, Renewal
    tenant_type TEXT,                      -- Individual, Corporate
    usage_type TEXT,                       -- Commercial, Residential, Industrial
    premise_type TEXT,                     -- Building, Office, Warehouse
    premise_subtype TEXT,                  -- Sub-classification
    master_project TEXT,

    parking TEXT,                          -- Included, None, Any

    -- Time fields
    contract_date DATE NOT NULL,
    period TEXT,                           -- e.g., "2025-Q1"

    -- Metadata
    subtitle TEXT,
    description TEXT,
    source TEXT,
    source_date DATE,

    -- Administrative anchors
    emirate_id INT REFERENCES admin_divisions.emirate(emirate_id),
    city_id INT REFERENCES admin_divisions.city(city_id),
    sector_id INT REFERENCES admin_divisions.sector(sector_id),
    community_id INT REFERENCES admin_divisions.community(community_id),
    district_id INT REFERENCES admin_divisions.district(district_id)
);
CREATE INDEX ON lease_transactions.lease (contract_date);
CREATE INDEX ON lease_transactions.lease (community_id);
CREATE INDEX ON lease_transactions.lease (district_id);
CREATE INDEX ON lease_transactions.lease (usage_type);
CREATE INDEX ON lease_transactions.lease (contract_type);


-- 9.2. Spatial Geometry
CREATE TABLE spatial.lease_transactions_geom (
    lease_id BIGINT PRIMARY KEY REFERENCES lease_transactions.lease(lease_id) ON DELETE CASCADE,
    geom geometry(POINT, 4326) NOT NULL
);
CREATE INDEX ON spatial.lease_transactions_geom USING GIST (geom);


-- 9.3. Aggregated KPIs (Materialized View)
CREATE MATERIALIZED VIEW analytics.mv_lease_kpis AS
SELECT
    COUNT(*) AS total_contracts,
    SUM(total_rent_aed) AS total_rental_value_aed,
    SUM(area_m2) / 1e6 AS total_area_km2,
    AVG(rent_per_m2) AS avg_rent_per_m2
FROM lease_transactions.lease;


-- 9.4. Contract Status Distribution (Pie Chart)
CREATE MATERIALIZED VIEW analytics.mv_lease_contract_status AS
SELECT
    contract_type,
    COUNT(*) AS contracts_count,
    SUM(area_m2) AS total_area_m2,
    SUM(area_m2) * 1.0 / SUM(SUM(area_m2)) OVER () AS percentage
FROM lease_transactions.lease
GROUP BY contract_type;


-- 9.5. Tenant Profile Distribution (Pie Chart)
CREATE MATERIALIZED VIEW analytics.mv_lease_tenant_profile AS
SELECT
    tenant_type,
    COUNT(*) AS contracts_count,
    SUM(area_m2) AS total_area_m2,
    SUM(area_m2) * 1.0 / SUM(SUM(area_m2)) OVER () AS percentage
FROM lease_transactions.lease
GROUP BY tenant_type;


-- 9.6. Top Property Types (Top 3)
CREATE MATERIALIZED VIEW analytics.mv_lease_top_property_types AS
SELECT
    premise_type,
    SUM(area_m2) / 1e6 AS total_area_km2
FROM lease_transactions.lease
GROUP BY premise_type
ORDER BY total_area_km2 DESC
LIMIT 3;


-- 9.7. API View (Flattened, Clean, No Admin Fields Exposed)
CREATE OR REPLACE VIEW api.v_lease_transactions AS
SELECT
    l.lease_id,
    l.subtitle,
    l.description,

    l.annual_rent_aed,
    l.total_rent_aed,
    l.rent_per_m2,
    l.area_m2,
    l.contract_date,
    l.contract_duration,
    l.contract_type,
    l.usage_type,
    l.tenant_type,
    l.master_project,
    l.premise_type,
    l.premise_subtype,
    l.parking,

    l.source,
    l.source_date,

    g.geom,

    ks.total_contracts,
    ks.total_rental_value_aed,
    ks.total_area_km2,
    ks.avg_rent_per_m2,

    cs.contract_type AS dist_contract_type,
    cs.contracts_count AS dist_contracts_count,
    cs.total_area_m2 AS dist_contract_area_m2,
    cs.percentage AS dist_contract_percentage,

    tp.tenant_type AS dist_tenant_type,
    tp.contracts_count AS dist_tenant_count,
    tp.total_area_m2 AS dist_tenant_area_m2,
    tp.percentage AS dist_tenant_percentage,

    pt.premise_type AS top_premise_type,
    pt.total_area_km2 AS top_premise_area_km2

FROM lease_transactions.lease l
LEFT JOIN spatial.lease_transactions_geom g ON g.lease_id = l.lease_id
LEFT JOIN analytics.mv_lease_kpis ks ON TRUE
LEFT JOIN analytics.mv_lease_contract_status cs ON cs.contract_type = l.contract_type
LEFT JOIN analytics.mv_lease_tenant_profile tp ON tp.tenant_type = l.tenant_type
LEFT JOIN analytics.mv_lease_top_property_types pt ON pt.premise_type = l.premise_type;
