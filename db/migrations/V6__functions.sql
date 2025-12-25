CREATE OR REPLACE FUNCTION spatial.buildings_within_radius(
    lon DOUBLE PRECISION,
    lat DOUBLE PRECISION,
    radius_m DOUBLE PRECISION
)
RETURNS TABLE(building_id UUID)
LANGUAGE sql STABLE AS
$$
SELECT b.building_id
FROM spatial.building_geom b
WHERE ST_DWithin(
    b.geog,
    ST_SetSRID(ST_MakePoint(lon, lat), 4326)::geography,
    radius_m
);
$$;
