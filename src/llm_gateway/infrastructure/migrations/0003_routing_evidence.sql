CREATE TABLE routing_decision (
    call_id uuid PRIMARY KEY REFERENCES model_invocation(call_id),
    protocol_version text NOT NULL DEFAULT 'gateway.routing-decision/v1' CHECK (protocol_version='gateway.routing-decision/v1'),
    alias_digest text NOT NULL CHECK (alias_digest ~ '^sha256:[0-9a-f]{64}$'),
    routing_policy text NOT NULL,
    routing_policy_digest text NOT NULL CHECK (routing_policy_digest ~ '^sha256:[0-9a-f]{64}$'),
    requirement_digest text NOT NULL CHECK (requirement_digest ~ '^sha256:[0-9a-f]{64}$'),
    seed_hex text NOT NULL CHECK (seed_hex ~ '^[0-9a-f]{64}$'),
    subject_profile text NOT NULL DEFAULT 'gateway.routing-subject/v1' CHECK (subject_profile='gateway.routing-subject/v1'),
    requirement jsonb NOT NULL CHECK (jsonb_typeof(requirement)='object'),
    feature_ids text[] NOT NULL,
    created_at timestamptz NOT NULL DEFAULT statement_timestamp()
);

CREATE TABLE routing_candidate (
    call_id uuid NOT NULL REFERENCES routing_decision(call_id) ON DELETE CASCADE,
    binding_id text NOT NULL,
    service_level text NOT NULL CHECK (service_level IN ('full','reduced')),
    priority integer NOT NULL CHECK (priority BETWEEN 0 AND 65535),
    weight integer NOT NULL CHECK (weight BETWEEN 0 AND 10000),
    initial_order integer CHECK (initial_order >= 0),
    PRIMARY KEY (call_id,binding_id),
    UNIQUE (call_id,initial_order),
    CHECK (weight <> 0 OR initial_order IS NULL)
);

ALTER TABLE provider_attempt ADD CONSTRAINT attempt_binding_identity UNIQUE (call_id,number,binding_id);

CREATE TABLE routing_event (
    call_id uuid NOT NULL REFERENCES routing_decision(call_id) ON DELETE CASCADE,
    sequence bigint NOT NULL CHECK (sequence > 0),
    kind text NOT NULL CHECK (kind IN ('preselection','attempt_gate','attempt_started','attempt_finished',
                                     'candidate_skipped','degradation_entered','routing_terminal')),
    binding_id text,
    attempt_number smallint,
    detail jsonb NOT NULL CHECK (jsonb_typeof(detail)='object'),
    observed_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    PRIMARY KEY (call_id,sequence),
    UNIQUE (call_id,sequence,kind),
    FOREIGN KEY (call_id,binding_id) REFERENCES routing_candidate(call_id,binding_id),
    FOREIGN KEY (call_id,attempt_number,binding_id) REFERENCES provider_attempt(call_id,number,binding_id),
    CHECK ((kind IN ('attempt_started','attempt_finished') AND binding_id IS NOT NULL AND attempt_number IS NOT NULL)
        OR (kind IN ('preselection','attempt_gate','candidate_skipped') AND binding_id IS NOT NULL AND attempt_number IS NULL)
        OR (kind IN ('degradation_entered','routing_terminal') AND binding_id IS NULL AND attempt_number IS NULL))
);

CREATE TABLE routing_terminal (
    call_id uuid PRIMARY KEY REFERENCES routing_decision(call_id) ON DELETE CASCADE,
    sequence bigint NOT NULL,
    kind text NOT NULL DEFAULT 'routing_terminal' CHECK (kind='routing_terminal'),
    terminal_at timestamptz NOT NULL,
    expires_at timestamptz NOT NULL CHECK (expires_at > terminal_at),
    FOREIGN KEY (call_id,sequence,kind) REFERENCES routing_event(call_id,sequence,kind)
);
CREATE INDEX routing_terminal_expiry ON routing_terminal(expires_at);

CREATE FUNCTION guard_routing_history() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP='UPDATE' OR EXISTS (SELECT 1 FROM routing_decision WHERE call_id=OLD.call_id) THEN
        RAISE EXCEPTION 'Routing facts are immutable; delete only the complete aggregate';
    END IF;
    RETURN OLD;
END;
$$;
CREATE TRIGGER routing_candidate_immutable BEFORE UPDATE OR DELETE ON routing_candidate
    FOR EACH ROW EXECUTE FUNCTION guard_routing_history();
CREATE TRIGGER routing_event_immutable BEFORE UPDATE OR DELETE ON routing_event
    FOR EACH ROW EXECUTE FUNCTION guard_routing_history();
CREATE TRIGGER routing_terminal_immutable BEFORE UPDATE OR DELETE ON routing_terminal
    FOR EACH ROW EXECUTE FUNCTION guard_routing_history();
CREATE TRIGGER routing_decision_immutable BEFORE UPDATE ON routing_decision
    FOR EACH ROW EXECUTE FUNCTION guard_attempt_append_only();

CREATE FUNCTION guard_routing_event_sequence() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    PERFORM 1 FROM routing_decision WHERE call_id=NEW.call_id FOR UPDATE;
    IF EXISTS (SELECT 1 FROM routing_event WHERE call_id=NEW.call_id AND kind='routing_terminal')
        OR NEW.sequence <> (SELECT COALESCE(max(sequence),0)+1 FROM routing_event WHERE call_id=NEW.call_id) THEN
        RAISE EXCEPTION 'Invalid routing event sequence';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER routing_event_sequence BEFORE INSERT ON routing_event
    FOR EACH ROW EXECUTE FUNCTION guard_routing_event_sequence();
