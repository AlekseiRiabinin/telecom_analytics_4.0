CREATE VIEW api.building AS
SELECT
  b.building_id,
  b.name,
  bg.geom,
  m.risk_score
FROM core.building b
JOIN spatial.building_geom bg USING (building_id)
LEFT JOIN analytics.building_metrics m USING (building_id);


CREATE OR REPLACE VIEW api.building_enriched AS
SELECT
    b.building_id,
    b.name,
    b.building_type,
    b.status,
    bg.geom,
    m.risk_score,
    m.accessibility_score,
    gm.centrality,
    gm.reachability_score
FROM core.building b
JOIN spatial.building_geom bg USING (building_id)
LEFT JOIN analytics.building_metrics m USING (building_id)
LEFT JOIN graph.node n ON n.ref_id = b.building_id
LEFT JOIN analytics.graph_metrics gm ON gm.node_id = n.node_id;


CREATE OR REPLACE VIEW api.building_features AS
SELECT
    b.building_id,
    f.feature_name,
    f.feature_value,
    f.computed_at
FROM core.building b
JOIN ml.feature_store f
  ON f.entity_id = b.building_id
 AND f.entity_type = 'BUILDING';
