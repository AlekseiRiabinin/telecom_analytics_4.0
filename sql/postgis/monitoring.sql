CREATE TABLE monitoring.query_performance (
    id            BIGSERIAL PRIMARY KEY,
    query_name    TEXT,
    duration_ms   DOUBLE PRECISION,
    rows_returned BIGINT,
    executed_at   TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE monitoring.spatial_stats (
    id           BIGSERIAL PRIMARY KEY,
    table_name   TEXT,
    geom_count   BIGINT,
    avg_area     DOUBLE PRECISION,
    collected_at TIMESTAMPTZ DEFAULT now()
);

-- Stats collector
CREATE OR REPLACE FUNCTION monitoring.collect_spatial_stats()
RETURNS VOID AS $$
BEGIN
    INSERT INTO monitoring.spatial_stats (table_name, geom_count, avg_area)
    SELECT
        'spatial.building_geom',
        COUNT(*),
        AVG(ST_Area(geom::geography))
    FROM spatial.building_geom;
END;
$$ LANGUAGE plpgsql;
