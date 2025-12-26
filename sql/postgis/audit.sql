CREATE TABLE audit.change_log (
    id           BIGSERIAL PRIMARY KEY,
    schema_name  TEXT,
    table_name   TEXT,
    operation    TEXT,
    record_id    TEXT,
    old_data     JSONB,
    new_data     JSONB,
    changed_at   TIMESTAMPTZ DEFAULT now(),
    changed_by   TEXT
);


CREATE OR REPLACE FUNCTION audit.audit_trigger()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO audit.change_log (
        schema_name,
        table_name,
        operation,
        record_id,
        old_data,
        new_data,
        changed_by
    )
    VALUES (
        TG_TABLE_SCHEMA,
        TG_TABLE_NAME,
        TG_OP,
        COALESCE(NEW.id::TEXT, OLD.id::TEXT),
        to_jsonb(OLD),
        to_jsonb(NEW),
        current_user
    );

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;


CREATE TRIGGER audit_core_building
AFTER INSERT OR UPDATE OR DELETE ON core.building
FOR EACH ROW
EXECUTE FUNCTION audit.audit_trigger();
