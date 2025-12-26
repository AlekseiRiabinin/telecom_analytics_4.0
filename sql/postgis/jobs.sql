CREATE TABLE jobs.job_definitions (
    job_name     TEXT PRIMARY KEY,
    job_type     TEXT,
    description  TEXT,
    enabled      BOOLEAN DEFAULT TRUE
);

CREATE TABLE jobs.job_runs (
    run_id       BIGSERIAL PRIMARY KEY,
    job_name     TEXT REFERENCES jobs.job_definitions,
    status       TEXT CHECK (status IN ('PENDING','RUNNING','FAILED','SUCCESS')),
    started_at   TIMESTAMPTZ,
    finished_at  TIMESTAMPTZ,
    parameters   JSONB
);

CREATE TABLE jobs.job_steps (
    step_id     BIGSERIAL PRIMARY KEY,
    run_id      BIGINT REFERENCES jobs.job_runs,
    step_name   TEXT,
    status      TEXT,
    output      JSONB,
    finished_at TIMESTAMPTZ
);


CREATE OR REPLACE FUNCTION jobs.start_job(p_job_name TEXT, p_params JSONB)
RETURNS BIGINT AS $$
DECLARE
    rid BIGINT;
BEGIN
    INSERT INTO jobs.job_runs (job_name, status, started_at, parameters)
    VALUES (p_job_name, 'RUNNING', now(), p_params)
    RETURNING run_id INTO rid;

    RETURN rid;
END;
$$ LANGUAGE plpgsql;
