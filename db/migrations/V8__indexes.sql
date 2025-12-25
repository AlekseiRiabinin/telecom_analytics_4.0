CREATE INDEX IF NOT EXISTS idx_building_geom_gist
ON spatial.building_geom
USING GIST (geom);

CREATE INDEX IF NOT EXISTS idx_building_geog_gist
ON spatial.building_geom
USING GIST (geog);

CREATE INDEX IF NOT EXISTS idx_graph_edge_src
ON graph.edge (src);

CREATE INDEX IF NOT EXISTS idx_graph_edge_dst
ON graph.edge (dst);
