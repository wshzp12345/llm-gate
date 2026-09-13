CREATE TABLE invocation_fingerprints (
    call_id uuid PRIMARY KEY REFERENCES model_invocation(call_id),
    key_id text NOT NULL,
    key_version text NOT NULL,
    invalidation_generation bigint NOT NULL CHECK (invalidation_generation >= 0),
    algorithm text NOT NULL CHECK (algorithm = 'HMAC-SHA-256'),
    request_profile text NOT NULL CHECK (request_profile <> ''),
    request_digest text NOT NULL CHECK (request_digest ~ '^[0-9a-f]{64}$'),
    execution_profile text NOT NULL CHECK (execution_profile <> ''),
    execution_digest text NOT NULL CHECK (execution_digest ~ '^[0-9a-f]{64}$'),
    routing_policy text NOT NULL,
    routing_seed text NOT NULL CHECK (routing_seed ~ '^[0-9a-f]{64}$'),
    expires_at timestamptz NOT NULL CHECK (isfinite(expires_at)),
    FOREIGN KEY (key_id,key_version) REFERENCES fingerprint_key_protection(key_id,key_version)
);
CREATE TRIGGER invocation_fingerprints_immutable BEFORE UPDATE OR DELETE ON invocation_fingerprints
    FOR EACH ROW EXECUTE FUNCTION guard_attempt_append_only();

CREATE FUNCTION guard_admitted_routing_seed() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE admitted invocation_fingerprints%ROWTYPE;
BEGIN
    SELECT * INTO admitted FROM invocation_fingerprints WHERE call_id=NEW.call_id;
    IF FOUND AND (NEW.seed_hex <> admitted.routing_seed OR NEW.routing_policy <> admitted.routing_policy) THEN
        RAISE EXCEPTION 'Routing identity differs from admission';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER routing_seed_admission BEFORE INSERT ON routing_decision
    FOR EACH ROW EXECUTE FUNCTION guard_admitted_routing_seed();
