-- Historical transition evidence only. Runtime Circuit state is never restored
-- from this table, and live samples/counters are not persisted as state rows.
CREATE TABLE circuit_transition_evidence (
    event_id uuid PRIMARY KEY,
    binding_id text NOT NULL,
    configuration_revision bigint NOT NULL REFERENCES config_revision(revision),
    occurred_at timestamptz NOT NULL,
    call_id uuid REFERENCES model_invocation(call_id),
    attempt_number smallint,
    transition jsonb NOT NULL CHECK (jsonb_typeof(transition)='object'),
    CHECK (attempt_number IS NULL OR (call_id IS NOT NULL AND attempt_number BETWEEN 1 AND 3)),
    FOREIGN KEY (call_id,attempt_number) REFERENCES provider_attempt(call_id,number)
);
CREATE INDEX circuit_transition_binding_time ON circuit_transition_evidence(binding_id,occurred_at,event_id);
CREATE TRIGGER circuit_transition_immutable BEFORE UPDATE OR DELETE ON circuit_transition_evidence
    FOR EACH ROW EXECUTE FUNCTION guard_attempt_append_only();
