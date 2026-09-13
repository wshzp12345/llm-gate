CREATE TABLE invocation_cancellation_cleanup (
    call_id uuid PRIMARY KEY REFERENCES invocation_local_cancellation(call_id),
    dispatched boolean NOT NULL,
    provider_result text CHECK (provider_result IN
        ('acknowledged','already_finished','not_supported','unknown','failed')),
    downstream_writable boolean NOT NULL,
    recorded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (dispatched = (provider_result IS NOT NULL))
);
-- This records uncertainty of billing, separately from logical cancellation.
-- A trusted cancel acknowledgement does not prove that no tokens were billed.
CREATE TABLE attempt_cancellation_billing (
    call_id uuid NOT NULL REFERENCES invocation_cancellation_cleanup(call_id),
    number smallint NOT NULL,
    billing_outcome text NOT NULL CHECK (billing_outcome='uncertain'),
    PRIMARY KEY (call_id,number),
    FOREIGN KEY (call_id,number) REFERENCES provider_attempt(call_id,number)
);
CREATE TRIGGER invocation_cancellation_cleanup_immutable
    BEFORE UPDATE OR DELETE ON invocation_cancellation_cleanup
    FOR EACH ROW EXECUTE FUNCTION guard_attempt_append_only();
CREATE TRIGGER attempt_cancellation_billing_immutable
    BEFORE UPDATE OR DELETE ON attempt_cancellation_billing
    FOR EACH ROW EXECUTE FUNCTION guard_attempt_append_only();
