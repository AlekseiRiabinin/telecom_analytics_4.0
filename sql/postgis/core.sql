CREATE TABLE core.building (
  building_id UUID PRIMARY KEY,
  name TEXT,
  building_type TEXT,
  status TEXT,
  created_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE core.parcel (
  parcel_id UUID PRIMARY KEY,
  zoning TEXT,
  created_at TIMESTAMPTZ DEFAULT now()
);
