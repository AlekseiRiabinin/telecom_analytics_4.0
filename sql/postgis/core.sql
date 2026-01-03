CREATE TABLE core.building (
	building_id uuid NOT NULL,
	"name" text NULL,
	building_type text NULL,
	status text NULL,
	created_at timestamptz DEFAULT now() NULL,
	osm_id text NULL,
	CONSTRAINT building_pkey PRIMARY KEY (building_id)
);

CREATE TABLE core.parcel (
  parcel_id UUID PRIMARY KEY,
  zoning TEXT,
  created_at TIMESTAMPTZ DEFAULT now()
);


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
