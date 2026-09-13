-- Missing historical correlations stay NULL. The existing identity guard
-- compares every column except state, so these become immutable at admission.
ALTER TABLE model_invocation
    ADD COLUMN task_id text CHECK (task_id IS NULL OR (octet_length(task_id) BETWEEN 1 AND 128 AND task_id !~ '[^A-Za-z0-9._:-]')),
    ADD COLUMN turn_id text CHECK (turn_id IS NULL OR (octet_length(turn_id) BETWEEN 1 AND 128 AND turn_id !~ '[^A-Za-z0-9._:-]')),
    ADD COLUMN step_id text CHECK (step_id IS NULL OR (octet_length(step_id) BETWEEN 1 AND 128 AND step_id !~ '[^A-Za-z0-9._:-]'));
