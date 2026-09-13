-- Internal context/disconnect/shutdown initiators only. Public cancellation
-- authorization, lifecycle lookup and final settlement are separate paths.
-- The immutable Invocation identity supplies tenant/subject/trace correlation;
-- local cancellation is system initiated, not a fabricated explicit caller act.
CREATE TABLE invocation_local_cancellation (
    call_id uuid PRIMARY KEY REFERENCES model_invocation(call_id),
    reason text NOT NULL CHECK (reason IN
        ('context_cancelled','client_disconnected','shutdown_drain_expired')),
    reserved_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TRIGGER invocation_local_cancellation_immutable
    BEFORE UPDATE OR DELETE ON invocation_local_cancellation
    FOR EACH ROW EXECUTE FUNCTION guard_attempt_append_only();
