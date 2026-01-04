CREATE TABLE ml.feature_store (
    entity_id    UUID,
    entity_type  TEXT, -- 'BUILDING','NODE','PARCEL', etc.
    feature_name TEXT,
    feature_value DOUBLE PRECISION,
    computed_at  TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (entity_id, entity_type, feature_name)
);
CREATE INDEX idx_feature_store_entity
  ON ml.feature_store (entity_type, entity_id);


-- Populate features
CREATE OR REPLACE FUNCTION ml.compute_feature_distance_to_road(p_building_id UUID)
RETURNS VOID AS $$
DECLARE
    dist DOUBLE PRECISION;
BEGIN
    SELECT c.distance_to_road INTO dist
    FROM graph.node_spatial_cache c
    JOIN graph.node n ON n.node_id = c.node_id
    WHERE n.ref_id = p_building_id;

    INSERT INTO ml.feature_store (entity_id, entity_type, feature_name, feature_value)
    VALUES (p_building_id, 'BUILDING', 'distance_to_road', dist)
    ON CONFLICT (entity_id, entity_type, feature_name)
    DO UPDATE SET feature_value = EXCLUDED.feature_value,
                  computed_at = now();
END;
$$ LANGUAGE plpgsql;


-- Feature pipeline runner
CREATE OR REPLACE FUNCTION ml.refresh_building_features()
RETURNS VOID AS $$
DECLARE
    b_id UUID;
BEGIN
    FOR b_id IN SELECT building_id FROM core.building 
	LOOP
        PERFORM ml.compute_feature_distance_to_road(b_id);
        PERFORM ml.compute_feature_building_density(b_id);
        PERFORM ml.compute_feature_accessibility(b_id);
    END LOOP;
END;
$$ LANGUAGE plpgsql;
