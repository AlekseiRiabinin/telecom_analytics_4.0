CREATE TABLE analytics.building_metrics (
  building_id UUID PRIMARY KEY,
  risk_score DOUBLE PRECISION,
  accessibility_score DOUBLE PRECISION,
  updated_at TIMESTAMPTZ
);


-- Materialized views for heavy analytics
CREATE MATERIALIZED VIEW analytics.building_accessibility AS
SELECT
    b.building_id,
    c.distance_to_road,
    c.nearest_road_id,
    r.road_type,
    r.maxspeed
FROM core.building b
JOIN graph.node n ON n.ref_id = b.building_id
JOIN graph.node_spatial_cache c ON c.node_id = n.node_id
LEFT JOIN spatial.road_geom r ON r.road_id = c.nearest_road_id;

-- Refresh periodically:
REFRESH MATERIALIZED VIEW analytics.building_accessibility;


CREATE OR REPLACE FUNCTION analytics.compute_risk_score(p_building_id UUID)
RETURNS DOUBLE PRECISION AS $$
DECLARE
    dist DOUBLE PRECISION;
BEGIN
    SELECT distance_to_road INTO dist
    FROM graph.node_spatial_cache c
    JOIN graph.node n ON n.node_id = c.node_id
    WHERE n.ref_id = p_building_id;

    RETURN 1 / (1 + dist); -- example
END;
$$ LANGUAGE plpgsql;


UPDATE analytics.building_metrics
SET risk_score = analytics.compute_risk_score(building_id),
    updated_at = now();

