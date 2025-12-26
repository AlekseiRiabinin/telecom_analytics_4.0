CREATE TABLE test.geometry_cases (
    test_id      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    description  TEXT,
    geom         geometry(GEOMETRY, 4326),
    expected     JSONB,
    created_at   TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE test.graph_cases (
    test_id      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    vertices     JSONB NOT NULL,
    edges        JSONB NOT NULL,
    expected     JSONB,
    created_at   TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE test.test_results (
    result_id    UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    test_id      UUID,
    test_type    TEXT,
    status       TEXT,
    output       JSONB,
    executed_at  TIMESTAMPTZ DEFAULT now()
);
