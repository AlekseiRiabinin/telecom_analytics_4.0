CREATE TABLE graph.node (
  node_id UUID PRIMARY KEY,
  node_type TEXT,
  ref_id UUID,
  geom geometry(POINT, 4326)
);

CREATE TABLE graph.edge (
  edge_id UUID PRIMARY KEY,
  from_node UUID REFERENCES graph.node,
  to_node UUID REFERENCES graph.node,
  weight DOUBLE PRECISION,
  edge_type TEXT
);

-- Spatial–graph cache tables
CREATE TABLE graph.node_spatial_cache (
    node_id          UUID PRIMARY KEY REFERENCES graph.node(node_id),
    building_id      UUID,
    nearest_road_id  UUID,
    snapped_geom     geometry(POINT, 4326),
    distance_to_bldg DOUBLE PRECISION,
    distance_to_road DOUBLE PRECISION,
    updated_at       TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX idx_node_spatial_cache_building
  ON graph.node_spatial_cache (building_id);


CREATE OR REPLACE FUNCTION graph.refresh_node_spatial_cache(p_node_id UUID)
RETURNS VOID AS $$
DECLARE
    n_geom geometry(POINT, 4326);
    b_id   UUID;
    b_dist DOUBLE PRECISION;
BEGIN
    SELECT geom INTO n_geom
    FROM graph.node
    WHERE node_id = p_node_id;

    IF n_geom IS NULL THEN
        RETURN;
    END IF;

    SELECT bg.building_id,
           ST_Distance(n_geom::geography, bg.geog)
    INTO b_id, b_dist
    FROM spatial.building_geom bg
    ORDER BY n_geom <-> bg.geom
    LIMIT 1;

    INSERT INTO graph.node_spatial_cache (
        node_id, building_id, snapped_geom, distance_to_bldg, updated_at
    )
    VALUES (
        p_node_id, b_id, n_geom, b_dist, now()
    )
    ON CONFLICT (node_id) DO UPDATE
    SET building_id      = EXCLUDED.building_id,
        snapped_geom     = EXCLUDED.snapped_geom,
        distance_to_bldg = EXCLUDED.distance_to_bldg,
        updated_at       = now();
END;
$$ LANGUAGE plpgsql;


-- Snap functions (graph → spatial enrichment)
-- Nearest building
CREATE OR REPLACE FUNCTION spatial.snap_to_nearest_building(p_geom geometry(POINT, 4326))
RETURNS TABLE (
    building_id UUID,
    snapped_point geometry(POINT, 4326),
    distance_m DOUBLE PRECISION
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        bg.building_id,
        ST_ClosestPoint(bg.geom, p_geom) AS snapped_point,
        ST_Distance(p_geom::geography, bg.geog) AS distance_m
    FROM spatial.building_geom bg
    ORDER BY p_geom <-> bg.geom
    LIMIT 1;
END;
$$ LANGUAGE plpgsql STABLE;

-- Use it for nodes
CREATE OR REPLACE FUNCTION spatial.snap_node_to_building(p_node_id UUID)
RETURNS TABLE (
    node_id UUID,
    building_id UUID,
    snapped_point geometry(POINT, 4326),
    distance_m DOUBLE PRECISION
) AS $$
DECLARE
    n_geom geometry(POINT, 4326);
BEGIN
    SELECT geom INTO n_geom
    FROM graph.node
    WHERE node_id = p_node_id;

    IF n_geom IS NULL THEN
        RETURN;
    END IF;

    RETURN QUERY
    SELECT
        p_node_id,
        s.building_id,
        s.snapped_point,
        s.distance_m
    FROM spatial.snap_to_nearest_building(n_geom) s;
END;
$$ LANGUAGE plpgsql STABLE;
