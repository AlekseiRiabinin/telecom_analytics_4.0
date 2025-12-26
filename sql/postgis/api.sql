CREATE VIEW api.building AS
SELECT
  b.building_id,
  b.name,
  bg.geom,
  m.risk_score
FROM core.building b
JOIN spatial.building_geom bg USING (building_id)
LEFT JOIN analytics.building_metrics m USING (building_id);
