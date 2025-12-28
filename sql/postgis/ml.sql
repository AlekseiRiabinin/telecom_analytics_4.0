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


-- Populate building features from analytics
--INSERT INTO ml.feature_store (entity_id, entity_type, feature_name, feature_value)
--SELECT
--    bm.building_id,
--    'BUILDING',
--    'risk_score',
--    bm.risk_score
--FROM analytics.building_metrics bm
--ON CONFLICT (entity_id, entity_type, feature_name)
--DO UPDATE SET
--    feature_value = EXCLUDED.feature_value,
--    computed_at   = now();
