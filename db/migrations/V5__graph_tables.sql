CREATE TABLE graph.node (
    node_id UUID PRIMARY KEY,
    node_type TEXT,
    ref_id UUID,
    geom geometry(POINT, 4326)
);

CREATE TABLE graph.edge (
    edge_id UUID PRIMARY KEY,
    src UUID REFERENCES graph.node(node_id),
    dst UUID REFERENCES graph.node(node_id),
    weight DOUBLE PRECISION,
    edge_type TEXT
);
