CREATE TABLE ingest.raw_features (
    ingest_id      BIGSERIAL PRIMARY KEY,
    source_system  TEXT NOT NULL,
    source_object  TEXT,
    payload        JSONB NOT NULL,
    geom_raw       GEOMETRY,
    received_at    TIMESTAMPTZ DEFAULT now(),
    checksum       TEXT,
    is_processed   BOOLEAN DEFAULT FALSE
);

CREATE TABLE ingest.validation_errors (
    ingest_id   BIGINT REFERENCES ingest.raw_features,
    error_code  TEXT,
    error_msg   TEXT,
    created_at  TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE ingest.valid_features (
    ingest_id    BIGINT PRIMARY KEY,
    feature_type TEXT,
    geom         GEOMETRY NOT NULL,
    properties   JSONB,
    validated_at TIMESTAMPTZ DEFAULT now()
);


CREATE OR REPLACE FUNCTION ingest.validate_feature(p_ingest_id BIGINT)
RETURNS BOOLEAN AS $$
DECLARE
    g GEOMETRY;
BEGIN
    SELECT geom_raw INTO g
    FROM ingest.raw_features
    WHERE ingest_id = p_ingest_id;

    IF g IS NULL OR NOT ST_IsValid(g) THEN
        INSERT INTO ingest.validation_errors
        VALUES (p_ingest_id, 'INVALID_GEOMETRY', 'Geometry invalid');
        RETURN FALSE;
    END IF;

    INSERT INTO ingest.valid_features (ingest_id, geom)
    VALUES (p_ingest_id, ST_MakeValid(g));

    UPDATE ingest.raw_features
    SET is_processed = TRUE
    WHERE ingest_id = p_ingest_id;

    RETURN TRUE;
END;
$$ LANGUAGE plpgsql;
