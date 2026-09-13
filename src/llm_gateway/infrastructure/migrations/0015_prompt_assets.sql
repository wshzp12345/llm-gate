-- Protected template assets are separate from ordinary Invocation Evidence.
CREATE TABLE prompt_asset (
    tenant_id text NOT NULL,
    asset_id uuid NOT NULL,
    generation bigint NOT NULL DEFAULT 0 CHECK (generation >= 0),
    published_version uuid,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, asset_id),
    CHECK ((generation = 0) = (published_version IS NULL))
);

CREATE TABLE prompt_version (
    tenant_id text NOT NULL,
    asset_id uuid NOT NULL,
    version_id uuid NOT NULL,
    messages jsonb NOT NULL CHECK (jsonb_typeof(messages) = 'array' AND jsonb_array_length(messages) BETWEEN 1 AND 100),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, asset_id, version_id),
    FOREIGN KEY (tenant_id, asset_id) REFERENCES prompt_asset (tenant_id, asset_id)
);

ALTER TABLE prompt_asset ADD FOREIGN KEY (tenant_id, asset_id, published_version)
    REFERENCES prompt_version (tenant_id, asset_id, version_id);

CREATE TABLE prompt_publication (
    tenant_id text NOT NULL,
    asset_id uuid NOT NULL,
    generation bigint NOT NULL CHECK (generation > 0),
    version_id uuid NOT NULL,
    subject text NOT NULL,
    published_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, asset_id, generation),
    FOREIGN KEY (tenant_id, asset_id, version_id) REFERENCES prompt_version (tenant_id, asset_id, version_id)
);

CREATE FUNCTION reject_prompt_mutation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'Immutable Prompt record';
END;
$$;

CREATE TRIGGER prompt_version_immutable BEFORE UPDATE OR DELETE ON prompt_version
    FOR EACH ROW EXECUTE FUNCTION reject_prompt_mutation();
CREATE TRIGGER prompt_publication_immutable BEFORE UPDATE OR DELETE ON prompt_publication
    FOR EACH ROW EXECUTE FUNCTION reject_prompt_mutation();

CREATE TABLE invocation_prompt (
    call_id uuid PRIMARY KEY REFERENCES model_invocation(call_id) ON DELETE CASCADE,
    tenant_id text NOT NULL,
    asset_id uuid NOT NULL,
    version_id uuid NOT NULL,
    FOREIGN KEY (tenant_id, asset_id, version_id) REFERENCES prompt_version(tenant_id, asset_id, version_id)
);
CREATE TRIGGER invocation_prompt_immutable BEFORE UPDATE ON invocation_prompt
    FOR EACH ROW EXECUTE FUNCTION reject_prompt_mutation();
