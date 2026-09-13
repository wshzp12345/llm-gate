-- Legacy shells retain NULL: their absent admission-time retention cannot be
-- invented. New admission explicitly copies the immutable Snapshot's TTL.
ALTER TABLE model_invocation ADD COLUMN routing_evidence_ttl_seconds integer
    CHECK (routing_evidence_ttl_seconds BETWEEN 86400 AND 31536000);

CREATE TABLE invocation_settlement (
    call_id uuid PRIMARY KEY REFERENCES model_invocation(call_id),
    outcome text NOT NULL CHECK (outcome IN ('completed','failed','cancelled','uncertain')),
    error_code text,
    resolved_model text,
    finish_reason text,
    service_level text CHECK (service_level IN ('full','reduced')),
    attempt_count smallint NOT NULL CHECK (attempt_count BETWEEN 0 AND 3),
    input_tokens numeric(20,0) CHECK (input_tokens >= 0),
    output_tokens numeric(20,0) CHECK (output_tokens >= 0),
    cached_tokens numeric(20,0) CHECK (cached_tokens >= 0),
    reasoning_tokens numeric(20,0) CHECK (reasoning_tokens >= 0),
    validation_status text NOT NULL CHECK (validation_status IN ('not_requested','valid','invalid','unavailable')),
    terminal_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    CHECK ((outcome='completed' AND error_code IS NULL AND resolved_model IS NOT NULL
        AND finish_reason IS NOT NULL AND service_level IS NOT NULL)
        OR (outcome<>'completed' AND error_code IS NOT NULL AND service_level IS NULL))
);

CREATE TABLE invocation_cost_summary (
    call_id uuid NOT NULL REFERENCES invocation_settlement(call_id),
    currency text NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    attempt_count smallint NOT NULL CHECK (attempt_count BETWEEN 1 AND 3),
    input_cost numeric CHECK (input_cost >= 0),
    output_cost numeric CHECK (output_cost >= 0),
    cached_cost numeric CHECK (cached_cost >= 0),
    reasoning_cost numeric CHECK (reasoning_cost >= 0),
    total_cost numeric CHECK (total_cost >= 0),
    completeness text NOT NULL CHECK (completeness IN ('complete','partial','unavailable')),
    certainty text NOT NULL CHECK (certainty IN ('estimated','unavailable')),
    rounding text NOT NULL DEFAULT 'half_even_12dp' CHECK (rounding='half_even_12dp'),
    PRIMARY KEY (call_id,currency)
);
CREATE TRIGGER invocation_settlement_immutable BEFORE UPDATE OR DELETE ON invocation_settlement
    FOR EACH ROW EXECUTE FUNCTION guard_attempt_append_only();
CREATE TRIGGER invocation_cost_summary_immutable BEFORE UPDATE OR DELETE ON invocation_cost_summary
    FOR EACH ROW EXECUTE FUNCTION guard_attempt_append_only();
