CREATE TABLE ingest.raw_features (
	ingest_id BIGSERIAL NOT NULL,
	source_system TEXT NOT NULL,
	source_object TEXT NULL,
	payload JSONB NOT NULL,
	geom_raw public.geometry NULL,
	received_at TIMESTAMPTZ DEFAULT now() NULL,
	checksum TEXT NULL,
	is_processed BOOLEAN DEFAULT FALSE NULL,
	CONSTRAINT raw_features_pkey PRIMARY KEY (ingest_id),
	CONSTRAINT raw_unique_checksum UNIQUE (checksum)
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


-- Ingestion rules engine
-- Rules table
CREATE TABLE ingest.validation_rules (
    rule_id       BIGSERIAL PRIMARY KEY,
    rule_name     TEXT UNIQUE,
    rule_type     TEXT,        -- 'GEOMETRY','ATTRIBUTE','TOPOLOGY'
    is_enabled    BOOLEAN DEFAULT TRUE,
    config        JSONB,       -- parameters for the rule
    created_at    TIMESTAMPTZ DEFAULT now()
);


-- Extend validation to use rules
CREATE OR REPLACE FUNCTION ingest.validate_feature(p_ingest_id BIGINT)
RETURNS BOOLEAN AS $$
DECLARE
    g            GEOMETRY;
    v_ok         BOOLEAN := TRUE;
    r            RECORD;
    v_source     TEXT;
    v_properties JSONB;
BEGIN
    -- Load raw feature
    SELECT source_system, payload, geom_raw
    INTO v_source, v_properties, g
    FROM ingest.raw_features
    WHERE ingest_id = p_ingest_id;

    -- Basic geometry check
    IF g IS NULL OR NOT ST_IsValid(g) THEN
        INSERT INTO ingest.validation_errors (ingest_id, error_code, error_msg)
        VALUES (p_ingest_id, 'INVALID_GEOMETRY', 'Geometry invalid');
        v_ok := FALSE;
    END IF;

    -- Dynamic rules
    FOR r IN
        SELECT * FROM ingest.validation_rules
        WHERE is_enabled = TRUE
    LOOP
        IF r.rule_type = 'GEOMETRY' THEN
            IF r.config ? 'bbox' THEN
                IF NOT ST_Within(g, ST_GeomFromText(r.config->>'bbox', 4326)) THEN
                    INSERT INTO ingest.validation_errors (ingest_id, error_code, error_msg)
                    VALUES (p_ingest_id, r.rule_name, 'Geometry outside allowed extent');
                    v_ok := FALSE;
                END IF;
            END IF;
        END IF;
    END LOOP;

    -- If valid, insert into valid_features
    IF v_ok THEN
        INSERT INTO ingest.valid_features (ingest_id, feature_type, geom, properties)
        VALUES (
            p_ingest_id,
			CASE
			    WHEN v_source LIKE 'OSM_Dubai_Buildings%' THEN 'building'
			    WHEN v_source LIKE 'OSM_Dubai_Roads%' THEN 'road'
			    WHEN v_source LIKE 'OSM_Dubai_POIs%' THEN 'poi'
			    WHEN v_source LIKE 'OSM_Dubai_Landuse%' THEN 'landuse'
			END,
            ST_MakeValid(g),
            v_properties
        )
        ON CONFLICT (ingest_id) DO UPDATE
        SET feature_type = EXCLUDED.feature_type,
            geom         = EXCLUDED.geom,
            properties   = EXCLUDED.properties,
            validated_at = now();

        UPDATE ingest.raw_features
        SET is_processed = TRUE
        WHERE ingest_id = p_ingest_id;
    END IF;

    RETURN v_ok;
END;
$$ LANGUAGE plpgsql;
