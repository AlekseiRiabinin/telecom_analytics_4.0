-- ============================================================
-- PostgreSQL + PostGIS initialization
-- Compatible with existing Graph Analytics Engine
-- ============================================================

CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS postgis_topology;

-- ============================================================
-- ENUM TYPES (mirrors Scala sealed traits)
-- ============================================================

DO $$ BEGIN
  CREATE TYPE vertex_type AS ENUM (
    'Subscriber',
    'Device',
    'Cell',
    'IP',
    'Building',
    'Road',
    'Parcel'
  );
EXCEPTION
  WHEN duplicate_object THEN null;
END $$;

DO $$ BEGIN
  CREATE TYPE edge_type AS ENUM (
    'Call',
    'SMS',
    'DataSession',
    'ConnectedTo',
    'AdjacentTo',
    'LocatedOn',
    'AccessibleVia'
  );
EXCEPTION
  WHEN duplicate_object THEN null;
END $$;

-- ============================================================
-- VERTICES TABLE (Graph Nodes)
-- ============================================================

CREATE TABLE IF NOT EXISTS vertices (
  id            TEXT PRIMARY KEY,
  value         DOUBLE PRECISION NOT NULL,
  v_type        vertex_type NOT NULL,
  properties    JSONB DEFAULT '{}'::jsonb,
  created_at    TIMESTAMP WITH TIME ZONE DEFAULT now(),

  -- Spatial extensions (optional, non-breaking)
  geom          GEOMETRY(GEOMETRY, 4326),
  bbox          GEOMETRY(POLYGON, 4326)
);

-- Spatial index
CREATE INDEX IF NOT EXISTS idx_vertices_geom
  ON vertices USING GIST (geom);

-- Property index for DSL queries
CREATE INDEX IF NOT EXISTS idx_vertices_properties
  ON vertices USING GIN (properties);

-- ============================================================
-- EDGES TABLE (Graph Edges)
-- ============================================================

CREATE TABLE IF NOT EXISTS edges (
  id            BIGSERIAL PRIMARY KEY,
  src           TEXT NOT NULL REFERENCES vertices(id) ON DELETE CASCADE,
  dst           TEXT NOT NULL REFERENCES vertices(id) ON DELETE CASCADE,
  weight        DOUBLE PRECISION NOT NULL,
  e_type        edge_type NOT NULL,
  properties    JSONB DEFAULT '{}'::jsonb,
  timestamp     TIMESTAMP WITH TIME ZONE DEFAULT now(),

  -- Spatial extensions
  geom                  GEOMETRY(LINESTRING, 4326),
  distance_meters       DOUBLE PRECISION,
  travel_time_seconds   DOUBLE PRECISION
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_edges_src_dst
  ON edges (src, dst);

CREATE INDEX IF NOT EXISTS idx_edges_geom
  ON edges USING GIST (geom);

CREATE INDEX IF NOT EXISTS idx_edges_properties
  ON edges USING GIN (properties);

-- ============================================================
-- OPTIONAL GRAPH VIEWS (Spark / GraphQL Friendly)
-- ============================================================

-- Spark-friendly vertex view
CREATE OR REPLACE VIEW graph_vertices AS
SELECT
  id,
  value,
  v_type,
  properties,
  created_at
FROM vertices;

-- Spark-friendly edge view
CREATE OR REPLACE VIEW graph_edges AS
SELECT
  src,
  dst,
  weight,
  e_type,
  properties,
  EXTRACT(EPOCH FROM timestamp) * 1000 AS timestamp_ms
FROM edges;

-- ============================================================
-- SAMPLE DATA (Minimal, Safe)
-- ============================================================

INSERT INTO vertices (id, value, v_type, geom)
VALUES
  ('v1', 1.0, 'Subscriber', ST_SetSRID(ST_MakePoint(-73.9857, 40.7484), 4326)),
  ('v2', 2.0, 'Building',  ST_SetSRID(ST_MakePoint(-73.9875, 40.7491), 4326))
ON CONFLICT (id) DO NOTHING;

INSERT INTO edges (src, dst, weight, e_type, geom, distance_meters)
VALUES
  (
    'v1',
    'v2',
    1.0,
    'ConnectedTo',
    ST_MakeLine(
      ST_SetSRID(ST_MakePoint(-73.9857, 40.7484), 4326),
      ST_SetSRID(ST_MakePoint(-73.9875, 40.7491), 4326)
    ),
    200
  )
ON CONFLICT DO NOTHING;
