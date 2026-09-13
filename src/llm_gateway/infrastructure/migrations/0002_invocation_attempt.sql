-- Initial unkeyed shell and Attempt checkpoints. Full routing, keyed admission
-- and terminal settlement are separate increments, not optional live gates.
CREATE TABLE model_invocation (
    call_id uuid PRIMARY KEY,
    trace_id text NOT NULL CHECK (trace_id ~ '^[0-9a-f]{32}$' AND trace_id <> repeat('0', 32)),
    configuration_revision bigint NOT NULL REFERENCES config_revision(revision),
    requested_model text NOT NULL,
    tenant_id text NOT NULL,
    subject text NOT NULL,
    scopes text[] NOT NULL,
    issuer text NOT NULL,
    audience text NOT NULL,
    authorization_expires_at timestamptz NOT NULL,
    authentication_method text NOT NULL CHECK (authentication_method IN ('dev_bypass','local_jwt','online_authority')),
    state text NOT NULL DEFAULT 'accepted' CHECK (state IN ('accepted','running','cancelling','completed','failed','cancelled','uncertain')),
    accepted_at timestamptz NOT NULL DEFAULT statement_timestamp()
);

CREATE TABLE provider_attempt (
    call_id uuid NOT NULL REFERENCES model_invocation(call_id),
    number smallint NOT NULL CHECK (number BETWEEN 1 AND 3),
    binding_id text NOT NULL,
    candidate_attempt smallint NOT NULL CHECK (candidate_attempt BETWEEN 1 AND 2),
    started_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    PRIMARY KEY (call_id, number),
    UNIQUE (call_id, binding_id, candidate_attempt)
);

CREATE TABLE provider_attempt_outcome (
    call_id uuid NOT NULL,
    number smallint NOT NULL,
    outcome text NOT NULL CHECK (outcome IN ('succeeded','failed','cancelled','uncertain')),
    error_code text,
    resolved_model text,
    finish_reason text,
    input_tokens bigint CHECK (input_tokens >= 0),
    output_tokens bigint CHECK (output_tokens >= 0),
    cached_tokens bigint CHECK (cached_tokens >= 0),
    reasoning_tokens bigint CHECK (reasoning_tokens >= 0),
    provider_reported_total bigint CHECK (provider_reported_total >= 0),
    recovery_action text NOT NULL CHECK (recovery_action IN ('retry','advance','stop')),
    recovery_delay_ms integer NOT NULL CHECK (recovery_delay_ms BETWEEN 0 AND 5000),
    finished_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    PRIMARY KEY (call_id, number),
    FOREIGN KEY (call_id, number) REFERENCES provider_attempt(call_id, number),
    CHECK ((outcome = 'succeeded' AND error_code IS NULL AND resolved_model IS NOT NULL
            AND finish_reason IS NOT NULL AND recovery_action = 'stop')
        OR (outcome <> 'succeeded' AND error_code IS NOT NULL)),
    CHECK (cached_tokens IS NULL OR (input_tokens IS NOT NULL AND cached_tokens <= input_tokens)),
    CHECK (reasoning_tokens IS NULL OR (output_tokens IS NOT NULL AND reasoning_tokens <= output_tokens)),
    CHECK (recovery_action = 'retry' OR recovery_delay_ms = 0)
);

CREATE FUNCTION guard_attempt_append_only() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'Attempt history is append-only';
END;
$$;
CREATE TRIGGER provider_attempt_immutable BEFORE UPDATE OR DELETE ON provider_attempt
    FOR EACH ROW EXECUTE FUNCTION guard_attempt_append_only();
CREATE TRIGGER provider_attempt_outcome_immutable BEFORE UPDATE OR DELETE ON provider_attempt_outcome
    FOR EACH ROW EXECUTE FUNCTION guard_attempt_append_only();

CREATE FUNCTION guard_invocation_identity() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'Invocation retention is not enabled';
    END IF;
    IF (to_jsonb(NEW) - 'state') IS DISTINCT FROM (to_jsonb(OLD) - 'state') THEN
        RAISE EXCEPTION 'Invocation identity is immutable';
    END IF;
    IF NOT ((OLD.state = 'accepted' AND NEW.state IN ('running','cancelling','failed','uncertain'))
        OR (OLD.state = 'running' AND NEW.state IN ('cancelling','completed','failed','uncertain'))
        OR (OLD.state = 'cancelling' AND NEW.state IN ('cancelled','uncertain'))) THEN
        RAISE EXCEPTION 'Invalid invocation transition';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER model_invocation_guard BEFORE UPDATE OR DELETE ON model_invocation
    FOR EACH ROW EXECUTE FUNCTION guard_invocation_identity();
