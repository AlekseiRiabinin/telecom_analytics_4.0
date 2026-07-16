CREATE TABLE core.building (
	building_id UUID PRIMARY KEY,
	"name" TEXT NULL,
	building_type TEXT NULL,
	status TEXT NULL,
	created_at TIMESTAMPTZ DEFAULT now() NULL,
	osm_id TEXT NULL
);

CREATE TABLE core.parcel (
  parcel_id UUID PRIMARY KEY,
  zoning TEXT,
  created_at TIMESTAMPTZ DEFAULT now()
);


----------------------------------------------------------------------

CREATE TABLE core.cadastral_plot (
    plot_id TEXT PRIMARY KEY,    -- payload.id
    district_id TEXT NOT NULL,   -- from ingest table
    district_name TEXT NOT NULL, -- normalized district name
    plot_number TEXT,            -- payload.name
    land_id NUMERIC,             -- field_groups → land_id
    land_use_type TEXT,          -- field_groups → land_use_type
    purpose TEXT,                -- field_groups → purpose
    cadastral_status TEXT,       -- field_groups → cadastral_status
    foreign_eligible BOOLEAN,    -- "Yes"/"No" → boolean
    description TEXT             -- field_groups → description
);
CREATE INDEX ON core.cadastral_plot (district_id);
CREATE INDEX ON core.cadastral_plot (land_use_type);
CREATE INDEX ON core.cadastral_plot (purpose);

----------------------------------------------------------------------

----------------------------------------------------------------------

CREATE OR REPLACE FUNCTION core.load_cadastral_plots()
RETURNS VOID AS $$
BEGIN
    TRUNCATE core.cadastral_plot;

    INSERT INTO core.cadastral_plot (
        plot_id,
        district_id,
        district_name,
        plot_number,
        land_id,
        land_use_type,
        purpose,
        cadastral_status,
        foreign_eligible,
        description
    )
    SELECT
        payload->>'id' AS plot_id,
        district_id,
        district_name,
        payload->>'name' AS plot_number,

        -- land_id (tag-based)
        (
            SELECT (f->>'value')::numeric
            FROM jsonb_array_elements(payload->'field_groups') g,
                 jsonb_array_elements(g->'fields') f
            WHERE f->>'tag' = 'land_id'
            LIMIT 1
        ) AS land_id,

        -- land_use_type (tag-based)
        (
            SELECT f->>'value'
            FROM jsonb_array_elements(payload->'field_groups') g,
                 jsonb_array_elements(g->'fields') f
            WHERE f->>'tag' = 'land_use_type'
            LIMIT 1
        ) AS land_use_type,

        -- purpose (caption-based)
        (
            SELECT f->>'value'
            FROM jsonb_array_elements(payload->'field_groups') g,
                 jsonb_array_elements(g->'fields') f
            WHERE f->>'caption' = 'Purpose'
            LIMIT 1
        ) AS purpose,

        -- cadastral_status (tag-based)
        (
            SELECT f->>'value'
            FROM jsonb_array_elements(payload->'field_groups') g,
                 jsonb_array_elements(g->'fields') f
            WHERE f->>'tag' = 'cadastral_status'
            LIMIT 1
        ) AS cadastral_status,

        -- foreign_eligible (caption-based, Yes/No → boolean)
        (
            SELECT CASE
                WHEN f->>'value' ILIKE 'yes' THEN TRUE
                WHEN f->>'value' ILIKE 'no'  THEN FALSE
                ELSE NULL
            END
            FROM jsonb_array_elements(payload->'field_groups') g,
                 jsonb_array_elements(g->'fields') f
            WHERE f->>'caption' = 'Foreign eligible'
            LIMIT 1
        ) AS foreign_eligible,

        -- description (caption-based)
        (
            SELECT f->>'value'
            FROM jsonb_array_elements(payload->'field_groups') g,
                 jsonb_array_elements(g->'fields') f
            WHERE f->>'caption' = 'Description'
            LIMIT 1
        ) AS description

    FROM ingest.raw_urbi_cadastral_plots;
END;
$$ LANGUAGE plpgsql;

----------------------------------------------------------------------



-- Business domain layer
INSERT INTO core.building (building_id, name, building_type, status, osm_id)
SELECT
    gen_random_uuid(),
    properties->>'name',
    properties->>'fclass',
    'active',
    properties->>'osm_id'
FROM ingest.valid_features
WHERE feature_type = 'building';

INSERT INTO core.parcel (parcel_id, zoning)
SELECT
    gen_random_uuid(),
    properties->>'fclass'
FROM ingest.valid_features
WHERE feature_type = 'landuse';
