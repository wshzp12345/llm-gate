CREATE TABLE config_revision (
    revision bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    state text NOT NULL CHECK (state IN ('candidate', 'active', 'superseded', 'stale')),
    base_revision bigint REFERENCES config_revision(revision),
    snapshot_digest text NOT NULL CHECK (snapshot_digest ~ '^sha256:[0-9a-f]{64}$'),
    snapshot jsonb NOT NULL CHECK (jsonb_typeof(snapshot) = 'object'),
    change_set jsonb NOT NULL CHECK (jsonb_typeof(change_set) = 'object'),
    creation_description text,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    rollback_of bigint REFERENCES config_revision(revision)
);

CREATE UNIQUE INDEX config_one_active ON config_revision ((state)) WHERE state = 'active';

CREATE TABLE config_active (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    revision bigint REFERENCES config_revision(revision)
);
INSERT INTO config_active (singleton, revision) VALUES (true, NULL);

CREATE TABLE config_command_index (
    tenant_id text NOT NULL,
    subject text NOT NULL,
    operation text NOT NULL,
    command_id uuid NOT NULL,
    command_digest text NOT NULL,
    outcome jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, subject, operation, command_id)
);

CREATE TABLE config_command_audit (
    audit_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    tenant_id text NOT NULL,
    subject text NOT NULL,
    operation text NOT NULL,
    command_id uuid NOT NULL,
    revision bigint REFERENCES config_revision(revision),
    error_code text,
    description text,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE FUNCTION guard_config_revision() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'Configuration revisions are retained';
    END IF;
    IF (to_jsonb(NEW) - 'state') IS DISTINCT FROM (to_jsonb(OLD) - 'state') THEN
        RAISE EXCEPTION 'Configuration revision content is immutable';
    END IF;
    IF NOT ((OLD.state = 'candidate' AND NEW.state IN ('active', 'stale'))
        OR (OLD.state = 'active' AND NEW.state = 'superseded')) THEN
        RAISE EXCEPTION 'Invalid configuration lifecycle transition';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER config_revision_immutable BEFORE UPDATE OR DELETE ON config_revision
    FOR EACH ROW EXECUTE FUNCTION guard_config_revision();

CREATE FUNCTION guard_config_append_only() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'Configuration history is append-only';
END;
$$;
CREATE TRIGGER config_command_index_immutable BEFORE UPDATE OR DELETE ON config_command_index
    FOR EACH ROW EXECUTE FUNCTION guard_config_append_only();
CREATE TRIGGER config_command_audit_immutable BEFORE UPDATE OR DELETE ON config_command_audit
    FOR EACH ROW EXECUTE FUNCTION guard_config_append_only();
