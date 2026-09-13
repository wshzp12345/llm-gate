-- Coordination state, not configuration or Invocation identity. One active
-- Model process per database holds the separate session advisory lock.
CREATE TABLE model_execution_owner (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    owner_id uuid NOT NULL,
    acquired_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE model_execution_ownership_event (
    owner_id uuid PRIMARY KEY,
    previous_owner_id uuid,
    acquired_at timestamptz NOT NULL,
    source text NOT NULL DEFAULT 'model_process_start' CHECK (source='model_process_start')
);
CREATE TRIGGER model_execution_ownership_event_immutable
    BEFORE UPDATE OR DELETE ON model_execution_ownership_event
    FOR EACH ROW EXECUTE FUNCTION guard_attempt_append_only();
