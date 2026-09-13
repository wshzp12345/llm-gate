-- No legacy policy backfill: earlier shells did not record this admission fact.
CREATE TABLE invocation_safety_policy (
    call_id uuid PRIMARY KEY REFERENCES model_invocation(call_id),
    configuration_revision bigint NOT NULL REFERENCES config_revision(revision),
    resource_id text NOT NULL,
    content_digest text NOT NULL CHECK (content_digest ~ '^sha256:[0-9a-f]{64}$'),
    enforcement_identity text NOT NULL CHECK (enforcement_identity='provider_refusal_terminal')
);
CREATE TRIGGER invocation_safety_policy_immutable BEFORE UPDATE OR DELETE ON invocation_safety_policy
    FOR EACH ROW EXECUTE FUNCTION guard_attempt_append_only();

CREATE FUNCTION guard_safety_policy_reference() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE locked_snapshot jsonb; alias_id text; revision_id bigint; current_state text;
BEGIN
    SELECT i.configuration_revision,i.requested_model,i.state,c.snapshot
    INTO revision_id,alias_id,current_state,locked_snapshot
    FROM model_invocation i JOIN config_revision c ON c.revision=i.configuration_revision
    WHERE i.call_id=NEW.call_id FOR UPDATE OF i;
    IF current_state IS DISTINCT FROM 'accepted' OR revision_id IS DISTINCT FROM NEW.configuration_revision
        OR locked_snapshot->'model_aliases'->alias_id->>'safety_policy' IS DISTINCT FROM NEW.resource_id
        OR locked_snapshot->'safety_policies'->NEW.resource_id IS DISTINCT FROM '{"mode":"provider_refusal_terminal"}'::jsonb
        OR NEW.content_digest <> 'sha256:' || encode(sha256(convert_to('{"mode":"provider_refusal_terminal"}', 'UTF8')), 'hex')
        OR EXISTS (SELECT 1 FROM provider_attempt WHERE call_id=NEW.call_id) THEN
        RAISE EXCEPTION 'Safety reference differs from admitted policy';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER invocation_safety_reference BEFORE INSERT ON invocation_safety_policy
    FOR EACH ROW EXECUTE FUNCTION guard_safety_policy_reference();

ALTER TABLE provider_attempt_outcome ADD COLUMN safety_refused boolean NOT NULL DEFAULT false;
ALTER TABLE provider_attempt_outcome ADD CHECK (NOT safety_refused OR
    (outcome='succeeded' AND recovery_action='stop' AND recovery_delay_ms=0));
ALTER TABLE invocation_settlement ADD COLUMN safety_refused boolean NOT NULL DEFAULT false;
ALTER TABLE invocation_settlement ADD CHECK (NOT safety_refused OR outcome='completed');

CREATE TABLE safety_refusal (
    call_id uuid PRIMARY KEY REFERENCES invocation_safety_policy(call_id),
    attempt_number smallint NOT NULL,
    category text NOT NULL DEFAULT 'unspecified' CHECK (category='unspecified'),
    code text NOT NULL DEFAULT 'safety_refused' CHECK (code='safety_refused'),
    stage text NOT NULL DEFAULT 'provider_output' CHECK (stage='provider_output'),
    enforcer text NOT NULL DEFAULT 'provider' CHECK (enforcer='provider'),
    signal text NOT NULL CHECK (signal IN ('message_refusal','content_filter','both')),
    provider_policy_identity text CHECK (provider_policy_identity IS NULL),
    provider_policy_identity_state text NOT NULL DEFAULT 'unavailable' CHECK (provider_policy_identity_state='unavailable'),
    provider_request_id text CHECK (provider_request_id IS NULL),
    provider_request_id_state text NOT NULL DEFAULT 'unavailable' CHECK (provider_request_id_state='unavailable'),
    observed_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    FOREIGN KEY (call_id,attempt_number) REFERENCES provider_attempt_outcome(call_id,number) DEFERRABLE INITIALLY DEFERRED
);
CREATE TRIGGER safety_refusal_immutable BEFORE UPDATE OR DELETE ON safety_refusal
    FOR EACH ROW EXECUTE FUNCTION guard_attempt_append_only();

CREATE FUNCTION guard_refusal_recovery() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE refused safety_refusal%ROWTYPE;
BEGIN
    PERFORM 1 FROM model_invocation WHERE call_id=NEW.call_id FOR UPDATE;
    SELECT * INTO refused FROM safety_refusal WHERE call_id=NEW.call_id;
    IF TG_TABLE_NAME='provider_attempt' THEN
        IF refused.call_id IS NOT NULL THEN RAISE EXCEPTION 'No Attempt after refusal'; END IF;
    ELSIF TG_TABLE_NAME='provider_attempt_outcome' THEN
        IF NEW.safety_refused IS DISTINCT FROM (refused.call_id IS NOT NULL AND refused.attempt_number=NEW.number) THEN
            RAISE EXCEPTION 'Refusal outcome requires matching evidence';
        END IF;
    ELSIF TG_TABLE_NAME='invocation_settlement' THEN
        IF NEW.safety_refused IS DISTINCT FROM (refused.call_id IS NOT NULL) THEN
            RAISE EXCEPTION 'Refusal terminal requires matching evidence';
        END IF;
    ELSIF TG_TABLE_NAME='routing_event' AND refused.call_id IS NOT NULL THEN
        IF NOT (NEW.kind='routing_terminal' OR NEW.kind='attempt_finished' AND NEW.attempt_number=refused.attempt_number) THEN
            RAISE EXCEPTION 'No routing recovery after refusal';
        END IF;
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER refusal_stops_attempt BEFORE INSERT ON provider_attempt FOR EACH ROW EXECUTE FUNCTION guard_refusal_recovery();
CREATE TRIGGER refusal_outcome_guard BEFORE INSERT ON provider_attempt_outcome FOR EACH ROW EXECUTE FUNCTION guard_refusal_recovery();
CREATE TRIGGER refusal_settlement_guard BEFORE INSERT ON invocation_settlement FOR EACH ROW EXECUTE FUNCTION guard_refusal_recovery();
CREATE TRIGGER refusal_routing_guard BEFORE INSERT ON routing_event FOR EACH ROW EXECUTE FUNCTION guard_refusal_recovery();

CREATE FUNCTION guard_refusal_insert() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    PERFORM 1 FROM model_invocation WHERE call_id=NEW.call_id AND state='running' FOR UPDATE;
    IF NOT FOUND OR NOT EXISTS (SELECT 1 FROM provider_attempt WHERE call_id=NEW.call_id AND number=NEW.attempt_number)
        OR EXISTS (SELECT 1 FROM provider_attempt WHERE call_id=NEW.call_id AND number>NEW.attempt_number)
        OR EXISTS (SELECT 1 FROM provider_attempt_outcome WHERE call_id=NEW.call_id AND number=NEW.attempt_number) THEN
        RAISE EXCEPTION 'Refusal requires current unfinished Attempt';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER safety_refusal_insert BEFORE INSERT ON safety_refusal FOR EACH ROW EXECUTE FUNCTION guard_refusal_insert();
