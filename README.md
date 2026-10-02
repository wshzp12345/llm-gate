# LLM Gateway

Implementation of the [approved v0.1 design](docs/README.md). A runnable development text Gateway supports synchronous JSON and SSE, assembling configuration, authorization, versioned Prompts, admission, routing, protected Provider transport, durable execution and bounded telemetry behind separate APIs. See the [six-capability acceptance audit](docs/six-capability-final-audit.md) for current evidence. The full frozen v0.1 surface and production deployment remain incomplete.

## Docker/API quick start

For streaming requests after Docker startup, see [HTTP SSE requests and verification (PowerShell)](docs/docker-quickstart.md#docker-启动后直接请求-http-ssepowershell).

With the published `general` model configured for streaming, a PowerShell client can call the Docker-hosted HTTP API directly. Each request may incur Provider usage:

```powershell
$body = '{"model":"general","stream":true,"max_completion_tokens":2048,"messages":[{"role":"user","content":"Count from 1 to 10."}]}'
$body | curl.exe --noproxy "*" -N -i "http://127.0.0.1:8000/v1/chat/completions" -H "Content-Type: application/json" -H "Accept: text/event-stream" --data-binary "@-"
```

Check for `Content-Type: text/event-stream`, a nonempty `choices[0].delta.content`, a terminal `finish_reason`, and `data: [DONE]`. An error frame or a truncated stream is a failed call even when HTTP status is 200. For configuration checks and a script that verifies the full frame sequence, use the [Docker/API quick start](docs/docker-quickstart.md#流式入口使用).

For a newly published `deploy/deepseek.bundle.json` configuration, synchronous JSON Object output can be requested directly (this also incurs Provider usage):

```powershell
$body = '{"model":"general","response_format":{"type":"json_object"},"max_completion_tokens":2048,"messages":[{"role":"user","content":"Reply with one JSON object containing an answer field."}]}'
$body | curl.exe --noproxy "*" -i "http://127.0.0.1:8000/v1/chat/completions" -H "Content-Type: application/json" --data-binary "@-"
```

The Gateway independently checks JSON syntax and the object root. A published Binding must declare `json_object`; an already-active `none` configuration is not changed by editing the example file. `json_schema` requires a Binding using the `anthropic_messages/v1` Adapter with `json_schema` capability and a separately configured credential; its object Schemas must explicitly use `additionalProperties: false`, including nested objects, or feature-aware routing rejects them before Provider I/O. Strict `json_object` SSE is supported when the published Binding also enables streaming; Gateway validates the terminal JSON before `[DONE]`. JSON Schema SSE and wrapper extraction from streaming text remain unsupported.

See [Docker setup and API smoke](docs/docker-quickstart.md). The unified `gateway --bootstrap ...` entrypoint uses a closed dev configuration; `docker compose up -d --build` starts PostgreSQL, an independent migration job and the two-listener Gateway. First configuration publication is explicit through the API; an empty database remains non-ready. Production configuration fails closed instead of falling back to bypass.

The API-only `python deploy/smoke.py` explicitly validates, creates and publishes the supplied DeepSeek example, then sends one bounded model request. It incurs Provider usage and never runs as part of startup or pytest. A real Docker/DeepSeek smoke returned HTTP 200 and `OK`; restart and migration replay were also verified. Secret files stay outside the image and source control. Read the quick start before running it against an existing installation.

## Run tests

Python 3.12 or newer:

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -e ".[test]"
.\.venv\Scripts\python.exe -m pytest -q
```

Tests never invoke a real model. Unit tests use synthetic transports; integration tests additionally require an isolated PostgreSQL database and exercise local HTTP/TCP. Real DeepSeek synchronous and SSE smoke results are recorded separately in the acceptance checklist; credentials are not stored in source or pasted into chat.

## Current boundary

The component inventory below records historical incremental boundaries, not a current completion checklist. For the assembled dev startup and SSE support, prefer the quick start and six-capability audit above. Full production delivery remains incomplete.

- Framework-free request/result/failure and Usage values.
- Application-owned completion port.
- Separate keyed Request/Execution fingerprints for the currently accepted synchronous text subset, with versioned canonicalization and behavior-only Snapshot projection. Full request-surface profiles and live Idempotency/admission/cache integration remain pending.
- Immutable Fingerprint key ring, closable raw-HMAC material and a dedicated bounded operation-lease service, with exact-version lookup, local failure fencing and Fault recovery. PostgreSQL protection helpers and cancellation-safe dev mounted-file resolution are implemented; canonical fingerprint derivation, production SecretSource and live readiness/admission/retention wiring remain pending.
- Bounded synchronous text response translation and safe transport error classification.
- FR-719: missing trustworthy Usage remains unavailable; no conservative estimator.
- Exact decimal Attempt cost calculation and append-only initial accruals with immutable pricing identity, unknown/partial accounting and half-even 12-place rounding; reconciliation remains pending.
- Deterministic integer-weight Candidate ordering by service level and priority, with Python/Node golden vectors; hard eligibility filtering and runtime Router integration remain pending.
- Snapshot-to-Alias routing projection and static capability/status/Provider-override/output-limit assessment with ordered rejection reasons. Static eligibility does not bypass remaining runtime security, health, capacity or Attempt gates.
- Process-local fail-fast concurrency pools with explicit leases and cancellation-safe context cleanup; complete live service and transport-pool wiring remain pending.
- Atomic shared Provider/Binding Attempt capacity, projected from the locked Snapshot's Provider limit and the default Binding ceiling of 20. Capacity rejection persists `concurrency_exhausted` before Candidate skip; success still requires the remaining runtime gates.
- Process-wide instance/tenant Admission Permits enforce the fixed 200/50 defaults. An authorization-dependent synchronous wrapper holds one permit from before Invocation persistence through routing, all Attempts/backoffs and terminal settlement, with cancellation-safe release and a drain gate. Local-JWT request authorization is now available for the owned text process; stream/replay lifetimes, online-Authority integration and complete production Bootstrap remain pending.
- Shared instance/tenant API token buckets use 100/50 QPS and one-second burst capacity, with atomic two-scope debit after concurrency admission. One API access consumes once across Provider retries; rejected capacity/QPS creates no Invocation. Fully refilled idle tenant state is removed. Replay integration remains pending.
- Provider-wide QPS/burst token buckets supplement the capacity stage, share credit across Bindings and revisions, and persist `qps_exhausted` before skip. Retry reevaluates the bucket; configuration changes cannot reset credit or retroactively expand refill/burst. Replay admission and Provider-removal cleanup remain pending.
- A process-local per-Binding Circuit core implements the fixed rolling-window/open/half-open policy, one concurrent trial, bounded reopen cooldown and availability-only sampling. Its runtime stage waits for durable transition evidence before trial dispatch and observes outcomes only after their checkpoint commits. Failed event acknowledgements block subsequent work with the same event identity retained. Full service composition, probes and Binding lifecycle cleanup remain pending.
- An optional operational probe Port, safe same-origin GET adapter and process-shared due-runner isolate credentials, interval, timeout and concurrency from user work. Probes read no response body and produce content-free observations, not Model Results or Usage. Live scheduling, transport security assembly, durable health transitions and Circuit trial integration remain pending.
- A DNS-pinned direct connection backend and lazy HTTPX client factory validate every returned address, refuse mixed allow/deny sets before TCP, verify the actual peer, and retain the original hostname for mandatory TLS checks. DNS uses bounded uncached workers; DNS/TCP/TLS share a connection deadline. The HTTPX bridge disables environment proxies and redirects and rejects origin/Host/SNI overrides. CA-only embedded Trust Bundles replace default roots and recheck whole-bundle dates before DNS/TLS; connection age limits retire idle connections without interrupting active responses. A snapshot-reconciled registry fences changed pools, immediately closes idle connections and drains active responses. The development management lifecycle now drives it at startup, after publication and during background recovery. Completion can acquire these clients lazily after credentials/gates; full Model service composition and expiry telemetry remain pending.
- Pure bounded retry/fallback decisions with Attempt ceilings, remaining deadline, injected full jitter and trusted Retry-After limits; no execution or sleeping in the planner.
- Synchronous Attempt executor with explicit runtime/journal ports: start checkpoint before I/O, outcome/recovery checkpoint before retry or fallback, one absolute deadline and cancellation propagation. Concrete runtime gates and PostgreSQL Attempt storage remain pending.
- PostgreSQL migration runner and atomic configuration create/publish effects, command result replay and audit.
- Initial unkeyed Invocation shell and append-only PostgreSQL Attempt start/outcome checkpoints; complete admission, routing evidence and terminal cost settlement are not yet wired.
- Normalized routing-evidence storage with immutable Candidate snapshots, registered rejection reasons and atomic Attempt events. An Attempt requires a fresh matching allowed gate; runtime checks and evidence read/retention workflows remain pending.
- Atomic synchronous Invocation settlement with per-currency cost/Usage summaries and routing terminal. The Application wrapper returns only after commit; cancellation/uncertainty and streaming terminal workflows remain pending.
- Zero-Attempt route settlement for supported static/capacity/credential/Circuit rejection families: verifies complete locked-Alias evidence and commits failed state before returning a safe error, without inventing Provider Usage or cost. Other lifecycle/failure families remain pending.
- Injectable dev-only `POST /v1/chat/completions` text API: bounded strict requests, safe errors, model identity pair and committed aggregate Usage. It has no default backend/listener and is not yet the full `chat-completions-v1` profile.
- Text parameter projection merges Alias defaults with caller `temperature`, `top_p` and output-token overrides, rejects raised output ceilings without clamping, and preserves `developer` message order. Parameter-source/digest metadata is computed but not yet persisted; model-specific parameter support gates remain pending.
- An explicit dev-only SecretSource reads versioned mounted-file or opted-in environment records without caching or dotenv discovery. A bounded off-loop resolver and credential-stage Candidate runtime acquire leases before Attempt start and close them before backoff; remaining gates are mandatory injected dependencies. Launcher wiring and process-wide revocation/refresh integration remain pending.
- Canonical whole-resource Change Set application/diff/rebase rules, separate from full configuration validation.
- Strict JSON Bundle/Change Set DTOs, bounded JSON/YAML parsing, deterministic YAML export, canonical Bundle preparation, local reference/capability/deployment checks.
- Local PEM canonicalization, content-identity/CA/validity checks and Trust Bundle resource ceilings.
- An injectable dev-only FastAPI management slice: validation, Candidate creation, Active read, YAML export, rollback Candidate creation and publication, exercised with ASGI and PostgreSQL tests.

The Adapters are internal components, not independently deployable clients: production egress enforcement, credential leases, deadlines, persistence, and application retry/fallback policy must be wired before live use. Ordinary text streaming, strict JSON Object SSE, synchronous JSON Object validation, and a restricted synchronous JSON Schema subset are available in the development Gateway. Full incremental structured streaming, Tool payloads, complete Schema-feature routing, and full structured recovery remain unsupported and must not be advertised as the full approved Adapter capability contract.

See [implementation progress](docs/implementation-progress.md) for remaining work. The development synchronous Docker/API-to-DeepSeek milestone has been achieved; this is not full v0.1 acceptance.

The management app also exposes status-only `GET /healthz` and `GET /readyz`. Liveness is independent of configuration and external dependencies. Readiness remains 503 until startup supplies all required execution dependencies; probe requests do not perform database or Provider I/O. Non-readiness does not block configuration recovery routes. These routes belong on the internal management listener, never the public model listener. The local launcher observes Active configuration and runs the management listener, but does not yet assemble Model execution or the complete readiness aggregate.

`ActiveConfigurationLoader` reads the PostgreSQL Active pointer and immutable Snapshot in one statement, verifies persisted content identity and materialized structure, and rechecks current deployment constraints. It performs no publication or Provider calls and does not cache a previous successful load. This is a startup/admission dependency component, not the complete readiness aggregate or a background monitor.

`GET /gateway/v1/config/revisions/{revision}/export` returns YAML with the original creation description and Snapshot Digest header. With a semantic validator supplied, `POST /gateway/v1/config/revisions/{revision}/rollback` copies a Superseded Revision into a new Candidate based on the expected Active Revision. Rollback never activates it automatically; publication remains explicit. Command replay returns the original result without creating another Revision.

`POST /gateway/v1/config/revisions/{revision}/rebase` replays a Stale Candidate's stored Change Set against the expected current Active Snapshot. Whole-resource conflicts return 409; full validation failures after rebase return 422. Success creates a new Candidate and does not publish it. Convergent edits become no-ops, and deterministic outcomes support command replay.

## Local management listener (partial)

Install/update with `python -m pip install -e ".[test]"`, then run:

```powershell
.\.venv\Scripts\gateway-dev-management.exe --env-file .env.local --port 8001
```

This development-only listener binds **127.0.0.1** and uses the approved `llm_gateway` PostgreSQL schema. It verifies migration history before listening, never creates tables or publishes automatically, and stops with Ctrl+C. Credentials and access logs are not printed; forwarded headers are not trusted. The launcher exposes health probes, Active read, Revision export, validation, Candidate creation, rebase, rollback and explicit publication. Model routes are absent and `/readyz` remains 503, including after publication. This is not the Docker or production launcher.

The partial launcher's explicit parser ceilings are 1 MiB body/string, depth 64, 100,000 nodes and 10,000 collection items; its configuration command timeout is five seconds. These are development harness settings, not new frozen Bootstrap defaults.

The dev validation policy uses the Secret Reference `deepseek-api-key` (an identity only, never the actual key), USD pricing, and the implemented `compatible/v1` text-only capability contract. Streaming, Tool Calling and Structured Output capabilities cannot yet be advertised. Local CA validation is wired, and importing/publishing makes no Provider or key-resolution call. Runtime secret resolution is still pending. Dev limits are 1 MiB request, 100 messages, 256 KiB content item, 64 KiB output Schema, 256 KiB SSE event and 300 seconds per invocation; Routing Evidence is capped at 90 days and replay at one day/1 MiB. These explicit local settings do not replace the full production Bootstrap policy.

## Local development preflight

After installing the package, run `gateway-dev-check --env-file .env.local` to load local credentials and verify the database Schema without migrations, configuration writes or Provider calls. This is a development-only utility, not the production Bootstrap loader or service launcher. Exit codes are 0 for verified Schema, 1 for database/check failure, and 2 for invalid local environment. A successful check still reports `gateway_ready: false`: remaining execution dependencies and service startup are not wired.

The loader accepts `AGENT_RUNTIME_POSTGRES_URL` or `GATEWAY_DATABASE_URL`, plus optional `DEEPSEEK_API_KEY`. Process environment takes precedence over file values, and the Gateway-specific database name wins within one source. The exact SQLAlchemy `postgresql+psycopg://` prefix is translated to `postgresql://` without decoding credentials or changing connection options. Dotenv interpolation and environment mutation are disabled. `.env.local` remains ignored; never paste its values into docs or commands. Output contains status only.

Both development commands override the connection search path to `llm_gateway` while retaining other connection options; neither changes `.env.local` nor database role defaults. The migration CLI remains explicit and separate.

## PostgreSQL component tests

Set `GATEWAY_TEST_DATABASE_URL` to a dedicated, disposable PostgreSQL database, then run the same test command. Without it, PostgreSQL tests are explicitly skipped. Fixtures create a random `gateway_test_<uuid>` schema and drop only that schema at teardown; do not point the test suite at a production database.

The migration CLI is `gateway-migrate` after package installation (or `python -m llm_gateway.infrastructure.migrate`). It reads `GATEWAY_DATABASE_URL`, applies bundled versioned SQL transactionally, and verifies checksums on subsequent runs. Database credentials belong in local environment/secret management, not source or docs.

The separate `verify_schema` startup component checks the exact packaged migration history in a deadline-bounded, read-only transaction. Its `expected_version` is the final packaged SQL filename (currently `0015_prompt_assets.sql`), not the domain Bundle schema identifier. Missing, changed or newer history fails closed; verification never runs migrations or repairs history. Development launchers and the owned text process use this check; production Bootstrap and the complete v0.1 startup surface remain pending. An existing database on an earlier migration requires the explicit migration step before these launchers can start.

`PostgresStartupRecovery` now reconciles pre-owner non-terminal Invocations under a held `PostgresModelExecutionOwner` and an ownership-bound store. Before admission opens, it verifies the packaged Schema, processes bounded selections in per-call transactions, and atomically appends missing uncertain Attempt outcomes, terminal Usage/cost/routing facts, and immutable recovery audit (prior state, owner, reason, configuration and Schema revision). It never contacts a Provider, replays work, rewrites known Attempt/cost facts, or changes committed terminals. A durable Provider success without the Gateway terminal checkpoint remains logical uncertainty, not fabricated Gateway completion. Recovery marks validation unavailable rather than assuming validation was unnecessary; missing legacy admission TTL fails closed rather than inventing retention. Admission locks the current execution-owner ID; recovery excludes that same epoch, so wall-clock changes cannot hide older calls or select this process's live calls. The owned text process described below binds its live Invocation writers to the owner, runs recovery before opening admission, and retains the lease until local work stops. No recovery scan is enabled in the development configuration-only launchers.

`FullServiceTextProcess` now owns the implemented synchronous full-service text backend's startup, admission gate, in-flight tasks, key-recovery task, and PostgreSQL process lease. It requires explicit validated configuration, SecretSource, transport, authorization and health dependencies; there are no permissive production defaults. Fingerprint protection probes remain read-only, while admission, routing, Attempt, cancellation and terminal writes share the owned store. A one-second owner watcher and per-invocation/checkpoint verification detect lease loss; observation permanently closes admission and aborts local Provider work without fabricating a cancellation terminal. `create_owned_text_app` binds this process to ASGI lifespan. `create_owned_text_server` adds an explicit loopback Uvicorn server that starts the same Model shutdown task while HTTP requests drain, rather than waiting until lifespan teardown. Shutdown reserves up to two seconds inside its at-most-30-second budget for system cancellation, records `shutdown_drain_expired`, clips new checkpoints to the remaining window, and joins cooperating work before releasing ownership. The supplied transport/configuration lifetimes remain deployment-owned, and arbitrary non-cooperating Adapter tasks are not forcibly killed by this component. This is not yet a production CLI/Compose assembly, complete readiness/authentication bootstrap, or the full streaming/Tools/Structured/cache/replay API. Real PostgreSQL and loopback HTTP tests cover startup recovery, successful execution, graceful completion, forced cancellation and actual owner-connection loss.

`create_local_jwt_text_process` adds an actual HTTP-to-Invocation production-identity binding for this text slice. Its `authorization` dictionary is closed: `mode: local_jwt`, an absolute local `key_file` path, `format: pem|jwks`, exact `issuer` and `audience`, and `kid` only for PEM. The deployment must mount that file read-only; this component reads it once (at most 256 KiB), never reloads it, and never fetches keys or reads inline/environment/SecretSource key material. PEM accepts exactly one RSA public SPKI/PKCS#1 object; JWKS accepts 1–32 uniquely identified RSA public keys with the fixed RS256 profile. Token validation enforces canonical Compact JWS, strict bounded JSON, signature/key/issuer/audience checks, fixed Claim normalization and the 30-second temporal allowance. Static key/branch defects fail process construction without a bypass fallback. The HTTP boundary reads original uncombined Authorization fields, authenticates before body parsing, checks exact `gateway.model.invoke`, and carries the immutable identity via request-local context into the owned backend. All HTTP 401 responses use only `WWW-Authenticate: Bearer`; 403 and safe authorization dependency failures do not echo credentials or create Invocation/Provider work. Online introspection, production profile resolution, mount enforcement, Control Plane authentication and complete deployment assembly are still pending. The local-JWT factory cannot be combined with a separate authorizer callback.

The configuration transaction component accepts a `CandidatePreparer` and performs preparation only after command-index lookup and the Active-state lock. `BundlePreparer` validates a typed Bundle and derives its Snapshot and Change Set against that locked base. Local validation requires explicit deployment resource ceilings, Secret Reference identities, currency codes, Adapter contracts and a certificate-validation port. `LocalTrustBundleValidator` implements local certificate checks with an injected clock; these dependencies still need production composition. Most configuration API routes and runtime TLS/egress wiring remain pending. Synthetic low-level transaction fixtures must not bypass the validation layer.

`create_development_management_app` registers `GET /gateway/v1/config/active` and `POST /gateway/v1/config/revisions/{revision}/publish`; supplying the required semantic validator also registers `POST /gateway/v1/config/validate` and `POST /gateway/v1/config/revisions`. JSON Bundle/Change Set and YAML Bundle share the preparation path. The factory requires injected configuration services and explicit parser limits. It is intentionally dev-only (fixed dev command identity, no production authentication) and must not be installed on a production ingress. The local management launcher has real HTTP/PostgreSQL coverage; a complete Gateway/Compose startup and mounted Data Plane route remain pending.
