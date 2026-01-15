CREATE TABLE spatial.building_geom (
	building_id UUID PRIMARY KEY,
	geom geometry(MULTIPOLYGON, 4326) NOT NULL,
	geog geography(MULTIPOLYGON, 4326) NOT NULL,
	geom_3857 geometry(MULTIPOLYGON, 3857) GENERATED ALWAYS AS (ST_Transform(geom, 3857)) STORED
);
CREATE INDEX idx_building_geom_geog ON spatial.building_geom USING gist (geog);
CREATE INDEX idx_building_geom_geom ON spatial.building_geom USING gist (geom);
CREATE INDEX idx_building_geom_geom_3857 ON spatial.building_geom USING GIST (geom_3857);


CREATE TE TABLE spatial.road_geom (
    road_id UUID PRIMARY KEY,
    geom geometry(MULTILINESTRING, 4326) NOT NULL,
    geog geography(MULTILINESTRING, 4326),
    osm_id TEXT,
    road_type TEXT,
    oneway TEXT,
    maxspeed INTEGER,
    created_at TIMESTAMPTZ DEFAULT now(),
    geom_3857 geometry(MULTILINESTRING, 3857) GENERATED ALWAYS AS (ST_Transform(geom, 3857)) STORED
);
CREATE INDEX idx_road_geom_geom ON spatial.road_geom USING GIST (geom);
CREATE INDEX idx_road_geom_geom_3857 ON spatial.road_geom USING GIST (geom_3857);

-- Versioned spatial data (temporal GIS)
CREATE TABLE spatial.building_geom_history (
	history_id BIGSERIAL PRIMARY KEY,
	building_id UUID NOT NULL,
	geom geometry(MULTIPOLYGON, 4326) NOT NULL,
	geog geography(MULTIPOLYGON, 4326) NOT NULL,
	valid_from TIMESTAMPTZ NOT NULL,
	valid_to TIMESTAMPTZ NULL,
	operation TEXT NULL,
	changed_by TEXT NULL,
	changed_at TIMESTAMPTZ DEFAULT now() NULL
);

CREATE INDEX idx_building_geom_history_building
  ON spatial.building_geom_history (building_id);

CREATE INDEX idx_building_geom_history_valid
  ON spatial.building_geom_history (valid_from, COALESCE(valid_to, 'infinity'::timestamptz));



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


CREATE OR REPLACE FUNCTION spatial.building_geom_history_trigger()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        INSERT INTO spatial.building_geom_history (
            building_id, geom, geog, valid_from, operation, changed_by
        )
        VALUES (
            NEW.building_id, NEW.geom, NEW.geog, now(), 'INSERT', current_user
        );
    ELSIF TG_OP = 'UPDATE' THEN
        UPDATE spatial.building_geom_history
        SET valid_to = now()
        WHERE building_id = OLD.building_id
          AND valid_to IS NULL;

        INSERT INTO spatial.building_geom_history (
            building_id, geom, geog, valid_from, operation, changed_by
        )
        VALUES (
            NEW.building_id, NEW.geom, NEW.geog, now(), 'UPDATE', current_user
        );
    ELSIF TG_OP = 'DELETE' THEN
        UPDATE spatial.building_geom_history
        SET valid_to = now(), operation = 'DELETE', changed_by = current_user
        WHERE building_id = OLD.building_id
          AND valid_to IS NULL;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_building_geom_history
AFTER INSERT OR UPDATE OR DELETE ON spatial.building_geom
FOR EACH ROW EXECUTE FUNCTION spatial.building_geom_history_trigger();


-- Business domain layer
INSERT INTO spatial.building_geom (building_id, geom)
SELECT
    b.building_id,
    v.geom
FROM core.building b
JOIN ingest.valid_features v
  ON v.properties->>'osm_id' = b.osm_id
WHERE v.feature_type = 'building';
