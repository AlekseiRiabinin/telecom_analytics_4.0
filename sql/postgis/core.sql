CREATE TABLE core.building (
	building_id UUID NOT NULL,
	"name" TEXT NULL,
	building_type TEXT NULL,
	status TEXT NULL,
	created_at TIMESTAMPTZ DEFAULT now() NULL,
	osm_id TEXT NULL,
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
