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


CREATE OR REPLACE FUNCTION jobs.run_refresh_node_cache()
RETURNS VOID AS $$
DECLARE
    rid BIGINT;
BEGIN
    rid := jobs.start_job('refresh_node_cache', '{}'::jsonb);

    BEGIN
        -- Actual job logic
        PERFORM graph.refresh_node_spatial_cache(node_id)
        FROM graph.node;

        UPDATE jobs.job_runs
        SET status = 'SUCCESS', finished_at = now()
        WHERE run_id = rid;

    EXCEPTION WHEN OTHERS THEN
        UPDATE jobs.job_runs
        SET status = 'FAILED', finished_at = now()
        WHERE run_id = rid;
        RAISE;
    END;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION jobs.run_refresh_building_metrics()
RETURNS VOID AS $$
DECLARE
    rid BIGINT;
BEGIN
    rid := jobs.start_job('refresh_building_metrics', '{}'::jsonb);

    BEGIN
        UPDATE analytics.building_metrics
        SET
            risk_score = analytics.compute_risk_score(building_id),
            accessibility_score = analytics.compute_accessibility_score(building_id),
            updated_at = now();

        UPDATE jobs.job_runs
        SET status = 'SUCCESS', finished_at = now()
        WHERE run_id = rid;

    EXCEPTION WHEN OTHERS THEN
        UPDATE jobs.job_runs
        SET status = 'FAILED', finished_at = now()
        WHERE run_id = rid;
        RAISE;
    END;
END;
$$ LANGUAGE plpgsql;


-- Run nightly (computes ML features)
CREATE OR REPLACE FUNCTION jobs.run_refresh_ml_features()
RETURNS VOID AS $$
DECLARE
    rid BIGINT;
BEGIN
    rid := jobs.start_job('refresh_ml_features', '{}'::jsonb);

    BEGIN
        PERFORM ml.refresh_building_features();

        UPDATE jobs.job_runs
        SET status = 'SUCCESS', finished_at = now()
        WHERE run_id = rid;

    EXCEPTION WHEN OTHERS THEN
        UPDATE jobs.job_runs
        SET status = 'FAILED', finished_at = now()
        WHERE run_id = rid;
        RAISE;
    END;
END;
$$ LANGUAGE plpgsql;


-- Refresh all analytics materialized views
CREATE OR REPLACE FUNCTION jobs.run_refresh_materialized_views()
RETURNS VOID AS $$
DECLARE
    rid BIGINT;
BEGIN
    rid := jobs.start_job('refresh_materialized_views', '{}'::jsonb);

    BEGIN
        -- Add more views here later
        REFRESH MATERIALIZED VIEW CONCURRENTLY analytics.building_accessibility;

        UPDATE jobs.job_runs
        SET status = 'SUCCESS', finished_at = now()
        WHERE run_id = rid;

    EXCEPTION WHEN OTHERS THEN
        UPDATE jobs.job_runs
        SET status = 'FAILED', finished_at = now()
        WHERE run_id = rid;
        RAISE;
    END;
END;
$$ LANGUAGE plpgsql;


-- Prevent unbounded growth of jobs.job_runs and jobs.job_steps
CREATE OR REPLACE FUNCTION jobs.run_cleanup_job_logs()
RETURNS VOID AS $$
DECLARE
    rid BIGINT;
BEGIN
    rid := jobs.start_job('cleanup_job_logs', '{}'::jsonb);

    BEGIN
        DELETE FROM jobs.job_steps
        WHERE finished_at < now() - INTERVAL '90 days';

        DELETE FROM jobs.job_runs
        WHERE finished_at < now() - INTERVAL '90 days';

        UPDATE jobs.job_runs
        SET status = 'SUCCESS', finished_at = now()
        WHERE run_id = rid;

    EXCEPTION WHEN OTHERS THEN
        UPDATE jobs.job_runs
        SET status = 'FAILED', finished_at = now()
        WHERE run_id = rid;
        RAISE;
    END;
END;
$$ LANGUAGE plpgsql;


-- Will need this once road nodes + edges are implemented.
CREATE OR REPLACE FUNCTION jobs.run_rebuild_graph()
RETURNS VOID AS $$
DECLARE
    rid BIGINT;
BEGIN
    rid := jobs.start_job('rebuild_graph', '{}'::jsonb);

    BEGIN
        PERFORM graph.rebuild_road_nodes();
        PERFORM graph.rebuild_road_edges();
        PERFORM graph.recompute_edge_weights();

        UPDATE jobs.job_runs
        SET status = 'SUCCESS', finished_at = now()
        WHERE run_id = rid;

    EXCEPTION WHEN OTHERS THEN
        UPDATE jobs.job_runs
        SET status = 'FAILED', finished_at = now()
        WHERE run_id = rid;
        RAISE;
    END;
END;
$$ LANGUAGE plpgsql;


SELECT cron.schedule('refresh_node_cache_job', '0 * * * *', $$SELECT jobs.run_refresh_node_cache();$$);
