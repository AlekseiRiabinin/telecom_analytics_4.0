CREATE VIEW tiles.buildings AS
SELECT
    building_id AS id,
    geom_3857 AS geom
FROM spatial.building_geom;

CREATE VIEW tiles.roads AS
SELECT
    road_id AS id,
    geom_3857 AS geom
FROM spatial.road_geom;
