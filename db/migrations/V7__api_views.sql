CREATE OR REPLACE VIEW api.buildings AS
SELECT
    b.building_id,
    b.name,
    b.building_type,
    ST_AsGeoJSON(g.geom)::json AS geometry
FROM core.building b
JOIN spatial.building_geom g USING (building_id);
