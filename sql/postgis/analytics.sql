CREATE TABLE analytics.building_metrics (
  building_id UUID PRIMARY KEY,
  risk_score DOUBLE PRECISION,
  accessibility_score DOUBLE PRECISION,
  updated_at TIMESTAMPTZ
);
