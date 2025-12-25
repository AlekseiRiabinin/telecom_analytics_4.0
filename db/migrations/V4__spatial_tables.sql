CREATE TABLE spatial.building_geom (
    building_id UUID PRIMARY KEY REFERENCES core.building(building_id),
    geom geometry(POLYGON, 4326),
    geog geography(POLYGON, 4326),
    centroid geometry(POINT, 4326),
    updated_at TIMESTAMP DEFAULT now()
);

CREATE TABLE spatial.road_geom (
    road_id UUID PRIMARY KEY,
    geom geometry(LINESTRING, 4326),
    road_type TEXT
);
