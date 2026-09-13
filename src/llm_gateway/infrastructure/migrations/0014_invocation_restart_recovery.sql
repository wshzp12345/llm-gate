-- Content-free, immutable reconciliation evidence independent of routing TTL.
ALTER TABLE model_invocation ADD COLUMN execution_owner_id uuid
    REFERENCES model_execution_ownership_event(owner_id);

CREATE TABLE invocation_restart_recovery (
    call_id uuid PRIMARY KEY REFERENCES invocation_settlement(call_id),
    owner_id uuid NOT NULL REFERENCES model_execution_ownership_event(owner_id),
    prior_state text NOT NULL CHECK (prior_state IN ('accepted','running','cancelling')),
    event text NOT NULL DEFAULT 'restart_reconciliation' CHECK (event='restart_reconciliation'),
    uncertainty_reason text NOT NULL CHECK (uncertainty_reason='gateway_terminal_not_committed'),
    configuration_revision bigint NOT NULL REFERENCES config_revision(revision),
    schema_revision text NOT NULL,
    recovered_at timestamptz NOT NULL DEFAULT statement_timestamp()
);
CREATE TRIGGER invocation_restart_recovery_immutable
    BEFORE UPDATE OR DELETE ON invocation_restart_recovery
    FOR EACH ROW EXECUTE FUNCTION guard_attempt_append_only();
CREATE INDEX model_invocation_recovery_pending ON model_invocation(call_id)
    WHERE state IN ('accepted','running','cancelling');
