CREATE TABLE graph.node (
  node_id UUID PRIMARY KEY,
  node_type TEXT,
  ref_id UUID
);

CREATE TABLE graph.edge (
  edge_id UUID PRIMARY KEY,
  from_node UUID REFERENCES graph.node,
  to_node UUID REFERENCES graph.node,
  weight DOUBLE PRECISION,
  edge_type TEXT
);

ALTER TABLE graph.node
ADD COLUMN geom geometry(POINT, 4326);
