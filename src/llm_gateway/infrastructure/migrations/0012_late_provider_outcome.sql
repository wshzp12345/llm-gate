-- Completion observations are not additive charges or reconciliation imports.
-- The original Attempt/pricing/accrual share (call_id,number). Accrual may be
-- created by cancellation settlement after an observation arrives; no FK to an
-- as-yet-uncommitted accrual and no dependency on expiring Routing aggregates.
CREATE TABLE late_provider_outcome (
    call_id uuid NOT NULL,
    number smallint NOT NULL,
    sequence bigint NOT NULL CHECK (sequence > 0),
    event_id uuid NOT NULL,
    observed_state text NOT NULL CHECK (observed_state IN ('cancelling','cancelled','uncertain')),
    outcome text NOT NULL CHECK (outcome IN ('succeeded','failed','uncertain')),
    error_code text CHECK (error_code IN ('invalid_request','rate_limited','provider_unavailable',
        'provider_credentials_unavailable','provider_protocol_error','upstream_timeout','uncertain')),
    requested_model text,
    resolved_model text,
    finish_reason text,
    input_tokens numeric CHECK (input_tokens >= 0 AND trunc(input_tokens)=input_tokens),
    output_tokens numeric CHECK (output_tokens >= 0 AND trunc(output_tokens)=output_tokens),
    cached_tokens numeric CHECK (cached_tokens >= 0 AND trunc(cached_tokens)=cached_tokens),
    reasoning_tokens numeric CHECK (reasoning_tokens >= 0 AND trunc(reasoning_tokens)=reasoning_tokens),
    provider_reported_total numeric CHECK (provider_reported_total >= 0 AND trunc(provider_reported_total)=provider_reported_total),
    safety_refused boolean NOT NULL,
    source text NOT NULL DEFAULT 'provider_completion' CHECK (source='provider_completion'),
    observed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (call_id,number,sequence),
    UNIQUE (call_id,event_id),
    FOREIGN KEY (call_id,number) REFERENCES attempt_pricing(call_id,number),
    CHECK ((outcome='succeeded' AND error_code IS NULL AND requested_model IS NOT NULL
            AND resolved_model IS NOT NULL AND finish_reason IN ('stop','length','content_filter'))
        OR (outcome<>'succeeded' AND error_code IS NOT NULL AND requested_model IS NULL
            AND resolved_model IS NULL AND finish_reason IS NULL AND NOT safety_refused)),
    CHECK (NOT safety_refused OR outcome='succeeded'),
    CHECK (finish_reason IS DISTINCT FROM 'content_filter' OR safety_refused),
    CHECK ((outcome='uncertain') = (error_code='uncertain')),
    CHECK (outcome='succeeded' OR (input_tokens IS NULL AND output_tokens IS NULL AND cached_tokens IS NULL
        AND reasoning_tokens IS NULL AND provider_reported_total IS NULL)),
    CHECK (cached_tokens IS NULL OR (input_tokens IS NOT NULL AND cached_tokens<=input_tokens)),
    CHECK (reasoning_tokens IS NULL OR (output_tokens IS NOT NULL AND reasoning_tokens<=output_tokens))
);
CREATE TRIGGER late_provider_outcome_immutable BEFORE UPDATE OR DELETE ON late_provider_outcome
    FOR EACH ROW EXECUTE FUNCTION guard_attempt_append_only();

CREATE FUNCTION guard_late_provider_outcome() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE current_state text;
BEGIN
    SELECT state INTO current_state FROM model_invocation WHERE call_id=NEW.call_id FOR UPDATE;
    IF current_state NOT IN ('cancelling','cancelled','uncertain')
        OR NEW.observed_state IS DISTINCT FROM current_state
        OR NOT EXISTS (SELECT 1 FROM invocation_local_cancellation WHERE call_id=NEW.call_id)
        OR NEW.sequence <> (SELECT COALESCE(max(sequence),0)+1 FROM late_provider_outcome
                            WHERE call_id=NEW.call_id AND number=NEW.number) THEN
        RAISE EXCEPTION 'Invalid late Provider observation';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER late_provider_outcome_guard BEFORE INSERT ON late_provider_outcome
    FOR EACH ROW EXECUTE FUNCTION guard_late_provider_outcome();
