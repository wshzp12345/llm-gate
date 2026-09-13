-- An irreversible delivery reservation, not proof of client receipt. Commit
-- this before handing the first business delta to the HTTP transport.
CREATE TABLE invocation_stream_commit (
    call_id uuid PRIMARY KEY REFERENCES model_invocation(call_id),
    number smallint NOT NULL,
    resolved_model text NOT NULL CHECK (length(resolved_model) BETWEEN 1 AND 256),
    delta_kind text NOT NULL CHECK (delta_kind IN ('text','refusal')),
    committed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    FOREIGN KEY (call_id,number) REFERENCES provider_attempt(call_id,number)
);
CREATE TRIGGER invocation_stream_commit_immutable BEFORE UPDATE OR DELETE ON invocation_stream_commit
    FOR EACH ROW EXECUTE FUNCTION guard_attempt_append_only();

CREATE FUNCTION guard_stream_commit() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE current_state text;
BEGIN
    SELECT state INTO current_state FROM model_invocation WHERE call_id=NEW.call_id FOR UPDATE;
    IF current_state IS DISTINCT FROM 'running'
        OR NEW.number IS DISTINCT FROM (SELECT max(number) FROM provider_attempt WHERE call_id=NEW.call_id)
        OR EXISTS (SELECT 1 FROM provider_attempt_outcome WHERE call_id=NEW.call_id AND number=NEW.number) THEN
        RAISE EXCEPTION 'Stream commitment requires the active Attempt';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER invocation_stream_commit_guard BEFORE INSERT ON invocation_stream_commit
    FOR EACH ROW EXECUTE FUNCTION guard_stream_commit();

CREATE FUNCTION guard_attempt_stream_boundary() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    -- Serialize with commit, cancellation and terminal settlement, including
    -- callers issuing SQL without the application journal.
    PERFORM 1 FROM model_invocation WHERE call_id=NEW.call_id FOR UPDATE;
    IF EXISTS (SELECT 1 FROM invocation_stream_commit WHERE call_id=NEW.call_id) THEN
        RAISE EXCEPTION 'Committed stream cannot start another Attempt';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER provider_attempt_stream_guard BEFORE INSERT ON provider_attempt
    FOR EACH ROW EXECUTE FUNCTION guard_attempt_stream_boundary();

CREATE FUNCTION guard_outcome_stream_boundary() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE committed invocation_stream_commit%ROWTYPE;
BEGIN
    PERFORM 1 FROM model_invocation WHERE call_id=NEW.call_id FOR UPDATE;
    SELECT * INTO committed FROM invocation_stream_commit WHERE call_id=NEW.call_id;
    IF FOUND AND (NEW.number <> committed.number OR NEW.recovery_action <> 'stop'
        OR (NEW.resolved_model IS NOT NULL AND NEW.resolved_model <> committed.resolved_model)) THEN
        RAISE EXCEPTION 'Committed stream requires same-model terminal outcome';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER provider_attempt_outcome_stream_guard BEFORE INSERT ON provider_attempt_outcome
    FOR EACH ROW EXECUTE FUNCTION guard_outcome_stream_boundary();
