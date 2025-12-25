CREATE TABLE core.building (
    building_id UUID PRIMARY KEY,
    name TEXT,
    building_type TEXT,
    created_at TIMESTAMP DEFAULT now()
);

CREATE TABLE core.parcel (
    parcel_id UUID PRIMARY KEY,
    zoning TEXT,
    created_at TIMESTAMP DEFAULT now()
);
