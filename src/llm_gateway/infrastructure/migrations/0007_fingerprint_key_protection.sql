-- Non-content version protection only. v0.1 exposes no invalidation mutation.
CREATE TABLE fingerprint_key_protection (
    key_id text NOT NULL CHECK (key_id <> ''),
    key_version text NOT NULL CHECK (key_version <> ''),
    protected_until timestamptz NOT NULL CHECK (isfinite(protected_until)),
    invalidated_at timestamptz CHECK (isfinite(invalidated_at)),
    invalidation_generation bigint NOT NULL DEFAULT 0 CHECK (invalidation_generation >= 0),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (key_id, key_version)
);

CREATE FUNCTION guard_fingerprint_protection() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'Fingerprint protection rows cannot be deleted';
    END IF;
    IF NEW.key_id IS DISTINCT FROM OLD.key_id
        OR NEW.key_version IS DISTINCT FROM OLD.key_version
        OR NEW.protected_until < OLD.protected_until
        OR NEW.invalidated_at IS DISTINCT FROM OLD.invalidated_at
        OR NEW.invalidation_generation IS DISTINCT FROM OLD.invalidation_generation THEN
        RAISE EXCEPTION 'Fingerprint protection is monotonic and invalidation is unavailable';
    END IF;
    NEW.updated_at := clock_timestamp();
    RETURN NEW;
END;
$$;
CREATE TRIGGER fingerprint_protection_monotonic BEFORE UPDATE OR DELETE ON fingerprint_key_protection
    FOR EACH ROW EXECUTE FUNCTION guard_fingerprint_protection();
