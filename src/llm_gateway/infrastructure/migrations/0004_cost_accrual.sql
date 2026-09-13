CREATE TABLE attempt_pricing (
    call_id uuid NOT NULL,
    number smallint NOT NULL,
    configuration_revision bigint NOT NULL REFERENCES config_revision(revision),
    pricing_resource_id text NOT NULL,
    currency text NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    effective_from timestamptz NOT NULL,
    input_rate text NOT NULL,
    output_rate text NOT NULL,
    cached_input_rate text,
    reasoning_output_rate text,
    unit text NOT NULL DEFAULT 'per_million_tokens' CHECK (unit='per_million_tokens'),
    rounding text NOT NULL DEFAULT 'half_even_12dp' CHECK (rounding='half_even_12dp'),
    PRIMARY KEY (call_id,number),
    FOREIGN KEY (call_id,number) REFERENCES provider_attempt(call_id,number)
);

CREATE TABLE cost_accrual (
    call_id uuid NOT NULL,
    number smallint NOT NULL,
    usage_source text NOT NULL CHECK (usage_source IN ('provider_reported','locally_estimated','unavailable')),
    input_cost numeric CHECK (input_cost >= 0 AND trunc(input_cost,12)=input_cost),
    output_cost numeric CHECK (output_cost >= 0 AND trunc(output_cost,12)=output_cost),
    cached_cost numeric CHECK (cached_cost >= 0 AND trunc(cached_cost,12)=cached_cost),
    reasoning_cost numeric CHECK (reasoning_cost >= 0 AND trunc(reasoning_cost,12)=reasoning_cost),
    total_cost numeric CHECK (total_cost >= 0 AND trunc(total_cost,12)=total_cost),
    certainty text NOT NULL CHECK (certainty IN ('estimated','unavailable')),
    completeness text NOT NULL CHECK (completeness IN ('complete','partial','unavailable')),
    created_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    PRIMARY KEY (call_id,number),
    FOREIGN KEY (call_id,number) REFERENCES attempt_pricing(call_id,number),
    FOREIGN KEY (call_id,number) REFERENCES provider_attempt_outcome(call_id,number),
    CHECK (completeness <> 'complete' OR total_cost IS NOT NULL),
    CHECK ((completeness='unavailable' AND certainty='unavailable' AND total_cost IS NULL
        AND input_cost IS NULL AND output_cost IS NULL AND cached_cost IS NULL AND reasoning_cost IS NULL)
        OR (completeness <> 'unavailable' AND certainty='estimated'))
);
CREATE TRIGGER attempt_pricing_immutable BEFORE UPDATE OR DELETE ON attempt_pricing
    FOR EACH ROW EXECUTE FUNCTION guard_attempt_append_only();
CREATE TRIGGER cost_accrual_immutable BEFORE UPDATE OR DELETE ON cost_accrual
    FOR EACH ROW EXECUTE FUNCTION guard_attempt_append_only();
