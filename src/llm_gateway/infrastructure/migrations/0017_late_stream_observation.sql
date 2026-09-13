-- Preserve reported metadata from interrupted streams without rewriting the
-- terminal outcome or charging a second time. All other late guards remain.
ALTER TABLE late_provider_outcome DROP CONSTRAINT late_provider_outcome_check;
ALTER TABLE late_provider_outcome DROP CONSTRAINT late_provider_outcome_check4;
ALTER TABLE late_provider_outcome ADD CONSTRAINT late_provider_observation_shape CHECK (
    (outcome='succeeded' AND error_code IS NULL AND requested_model IS NOT NULL
        AND resolved_model IS NOT NULL AND finish_reason IN ('stop','length','content_filter'))
    OR (outcome<>'succeeded' AND error_code IS NOT NULL AND requested_model IS NULL
        AND finish_reason IS NULL AND NOT safety_refused
        AND (resolved_model IS NULL OR length(resolved_model) BETWEEN 1 AND 256))
);
