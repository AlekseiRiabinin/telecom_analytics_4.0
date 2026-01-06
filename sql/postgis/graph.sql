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
CREATE TABLE IF NOT EXISTS graph.node_spatial_cache (
    node_id UUID PRIMARY KEY REFERENCES graph.node(node_id),
    building_id UUID,
    nearest_road_id UUID,
    snapped_geom geometry(POINT, 4326),
    distance_to_bldg DOUBLE PRECISION,
    distance_to_road DOUBLE PRECISION,
    updated_at TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX idx_node_spatial_cache_building
  ON graph.node_spatial_cache (building_id);


CREATE OR REPLACE FUNCTION graph.refresh_node_spatial_cache(p_node_id UUID)
RETURNS VOID AS $$
DECLARE
    n_geom geometry(POINT, 4326);

    -- nearest building
    b_id   UUID;
    b_dist DOUBLE PRECISION;

    -- nearest road
    r_id   UUID;
    r_dist DOUBLE PRECISION;
BEGIN
    -- Load node geometry
    SELECT geom INTO n_geom
    FROM graph.node
    WHERE node_id = p_node_id;

    IF n_geom IS NULL THEN
        RETURN;
    END IF;

    ----------------------------------------------------------------------
    -- 1. Find nearest building
    ----------------------------------------------------------------------
    SELECT bg.building_id,
           ST_Distance(n_geom::geography, bg.geog)
    INTO b_id, b_dist
    FROM spatial.building_geom bg
    ORDER BY n_geom <-> bg.geom
    LIMIT 1;

    ----------------------------------------------------------------------
    -- 2. Find nearest road
    ----------------------------------------------------------------------
    SELECT rg.road_id,
           ST_Distance(n_geom::geography, rg.geog)
    INTO r_id, r_dist
    FROM spatial.road_geom rg
    ORDER BY n_geom <-> rg.geom
    LIMIT 1;

    ----------------------------------------------------------------------
    -- 3. Upsert into cache
    ----------------------------------------------------------------------
    INSERT INTO graph.node_spatial_cache (
        node_id,
        building_id,
        nearest_road_id,
        snapped_geom,
        distance_to_bldg,
        distance_to_road,
        updated_at
    )
    VALUES (
        p_node_id,
        b_id,
        r_id,
        n_geom,
        b_dist,
        r_dist,
        now()
    )
    ON CONFLICT (node_id) DO UPDATE
    SET building_id      = EXCLUDED.building_id,
        nearest_road_id  = EXCLUDED.nearest_road_id,
        snapped_geom     = EXCLUDED.snapped_geom,
        distance_to_bldg = EXCLUDED.distance_to_bldg,
        distance_to_road = EXCLUDED.distance_to_road,
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


-- Business domain layer
INSERT INTO graph.node (node_id, node_type, ref_id, geom)
SELECT
    gen_random_uuid(),
    'building_centroid',
    building_id,
    ST_Centroid(geom)
FROM spatial.building_geom;

INSERT INTO graph.node (node_id, node_type, ref_id, geom)
SELECT
    gen_random_uuid(),
    'road_endpoint',
    r.road_id,
    (ST_DumpPoints(r.geom)).geom
FROM spatial.road_geom r;


-- Create road edges (segments between nodes)
WITH segments AS (
    SELECT
        r.road_id,
        (ST_Dump(ST_Segmentize(r.geom, 0.00001))).geom AS seg
    FROM spatial.road_geom r
)
INSERT INTO graph.edge (edge_id, from_node, to_node, weight, edge_type)
SELECT
    gen_random_uuid(),
    n1.node_id,
    n2.node_id,
    ST_Length(seg::geography),
    'road'
FROM segments s
JOIN graph.node n1 ON ST_StartPoint(s.seg) = n1.geom
JOIN graph.node n2 ON ST_EndPoint(s.seg) = n2.geom;


-- JOBS
INSERT INTO graph.node_spatial_cache (node_id)
SELECT node_id FROM graph.node;

-- Populate graph.node_spatial_cache
SELECT graph.refresh_node_spatial_cache(node_id)
FROM graph.node;

-- Verify
SELECT COUNT(*) FROM graph.node_spatial_cache;
SELECT COUNT(*) FROM graph.node_spatial_cache WHERE nearest_road_id IS NOT NULL;
SELECT COUNT(*) FROM graph.node_spatial_cache WHERE nearest_road_id IS NULL;
