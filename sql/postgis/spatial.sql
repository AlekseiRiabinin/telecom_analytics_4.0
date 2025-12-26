CREATE TABLE spatial.building_geom (
  building_id UUID PRIMARY KEY REFERENCES core.building(building_id),
  geom geometry(POLYGON, 4326) NOT NULL,
  geog geography(POLYGON, 4326) NOT NULL
);

CREATE INDEX idx_building_geom_geom
  ON spatial.building_geom USING GIST (geom);

CREATE INDEX idx_building_geom_geog
  ON spatial.building_geom USING GIST (geog);


-- Spatial filtering → graph
CREATE FUNCTION spatial.buildings_within_radius(
  lon DOUBLE PRECISION,
  lat DOUBLE PRECISION,
  radius_m DOUBLE PRECISION
)
RETURNS TABLE (building_id UUID)
LANGUAGE sql STABLE AS $$
  SELECT building_id
  FROM spatial.building_geom
  WHERE ST_DWithin(
    geog,
    ST_MakePoint(lon, lat)::geography,
    radius_m
  );
$$;

-- Graph → spatial enrichment
CREATE FUNCTION spatial.enrich_nodes(
  node_ids UUID[]
)
RETURNS TABLE (
  node_id UUID,
  geom geometry
)
LANGUAGE sql STABLE AS $$
  SELECT n.node_id, bg.geom
  FROM graph.node n
  JOIN spatial.building_geom bg
    ON bg.building_id = n.ref_id
  WHERE n.node_id = ANY(node_ids);
$$;

-- sync geometry/geography
CREATE FUNCTION spatial.sync_geog()
RETURNS TRIGGER AS $$
BEGIN
  NEW.geog := NEW.geom::geography;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_sync_geog
BEFORE INSERT OR UPDATE ON spatial.building_geom
FOR EACH ROW EXECUTE FUNCTION spatial.sync_geog();
