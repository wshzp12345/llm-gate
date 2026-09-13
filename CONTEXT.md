# LLM Gateway

This context defines the shared language for the unified model entry point used by the Agent Engineering Platform.

## Language

**Agent Engineering Platform**:
The platform whose agents and engineering workflows consume model capabilities through the LLM Gateway.
_Avoid_: Agent Runtime

**LLM Gateway**:
The unified model entry point that exposes an OpenAI-compatible API and coordinates access to multiple OpenAI-compatible Providers.
_Avoid_: Thin proxy, Provider Adapter

**Provider**:
An external OpenAI-compatible model service connected below the LLM Gateway.
_Avoid_: Gateway, model

**Provider Egress Allowlist**:
The deployment-owned set of Provider hosts and network destinations that an Active Snapshot may authorize for outbound model traffic; production absence denies Provider egress.
_Avoid_: Caller URL, permissive HTTPS default, runtime routing preference

**Provider Trust Bundle**:
A Provider-scoped, content-addressed immutable certificate-authority set used instead of system roots for that Provider without disabling certificate or hostname verification.
_Avoid_: Insecure TLS, Data Plane certificate input, global trust bypass

**Model Adapter**:
An outer anti-corruption component that implements an Application-owned Provider Port by translating Gateway domain requests, events, results, and failures to and from one Provider protocol.
_Avoid_: Domain model, Provider SDK object

**Provider Port**:
The Application-owned model-execution interface with explicit synchronous `complete`, streaming `stream`, cancellation `cancel`, and optional evidence-based `resolve_uncertain` operations over Gateway-owned protocols.
_Avoid_: Provider SDK client, one method with a stream boolean

**Provider Event**:
An Adapter-to-Application streaming event representing content progress, Tool Call progress, Usage, or exactly one completed/failed/cancelled terminal state.
_Avoid_: Raw Provider chunk, external SSE frame

**Provider Cancel Result**:
A provider-independent cancellation acknowledgement of `acknowledged`, `already_finished`, `not_supported`, `unknown`, or `failed`, distinct from merely closing the local transport.
_Avoid_: Local socket close treated as remote cancellation

**Usage Measurement**:
A normalized Token-usage snapshot carrying separate count categories, authoritative/estimated/unavailable provenance, and complete/partial status.
_Avoid_: Missing counts represented as zero, unqualified estimate

**Usage Inconsistency**:
A non-terminal accounting anomaly where Provider-reported `total_tokens` differs from known `input_tokens + output_tokens`; Gateway recomputes the normalized total from the parent categories, retains the raw total only in safe Evidence, and emits a quality/operational warning outside availability circuits.
_Avoid_: Failed model result, Provider availability failure, trusting inconsistent total

**Partial Total Usage**:
A Usage observation containing Provider `total_tokens` without both input and output parent counts; Gateway preserves it in authorized Evidence with `partial` state but omits the standard OpenAI `usage` object rather than inventing a split or zeros.
_Avoid_: Estimated input/output split, zero-filled parents, incomplete standard Usage object

**Invocation Usage**:
The caller-visible category-wise sum of all known Usage Measurements from every Provider Attempt executed by one Model Invocation, including failed Attempts before a successful retry, with aggregate provenance and completeness that never fabricate unavailable counts as zero. `total_tokens` is `input_tokens + output_tokens`; cached Tokens are an input subset and reasoning Tokens are an output subset, so neither is added again.
_Avoid_: Final-Attempt-only usage, subset double counting, blended cost, hidden retry consumption

**Cost Accrual**:
The immutable initial estimated or authoritative charge recorded for one Provider Attempt using its locked pricing basis; locally estimated observable input/output may produce `estimated/partial` cost without assuming cache discount or hidden reasoning charges.
_Avoid_: Mutable current cost, invocation-wide blended estimate, guessed Provider-internal discount

**Pricing Table**:
The revision-scoped monetary rate schedule used to value one Provider Attempt's known Token categories in one settlement currency.
_Avoid_: Provider invoice, floating-point price, cross-currency report

**Outward Cost Boundary**:
The rule that ordinary OpenAI-compatible response bodies and headers expose no estimated, accrued, reconciled, or converted monetary cost; authorized status, Invocation Evidence, and Cost Ledger APIs own cost disclosure with pricing version, currency, certainty, and reconciliation context.
_Avoid_: Cost response header, OpenAI body extension, unqualified price estimate

**Reconciliation Adjustment**:
An append-only positive or negative Cost Ledger entry that reconciles a prior Cost Accrual against trusted Provider billing evidence without rewriting it.
_Avoid_: Editing the original accrual, unaudited correction

**Persistence Checkpoint**:
A required durable PostgreSQL transaction boundary before admission, Provider execution, recovery, external stream commitment, or successful terminal exposure may advance.
_Avoid_: Best-effort log write, per-delta content persistence

**Application Port**:
An interface owned by the Application use case that expresses an external capability it requires, implemented by an outer Adapter.
_Avoid_: Infrastructure-owned interface, Provider client

**Composition Root**:
The outermost startup component that validates Bootstrap Configuration, creates concrete Adapters, and injects them into Application use cases without containing domain policy.
_Avoid_: Service locator, business workflow

**Model Invocation**:
One logical model request observed by a caller. It can contain multiple internal Provider Attempts when Gateway policy permits retry or failover.
_Avoid_: Provider call, attempt

**Provider Attempt**:
One concrete attempt to invoke a selected model through one Provider as part of a Model Invocation.
_Avoid_: Model Invocation, request

**Model Alias**:
A caller-facing model name that resolves to one or more configured Provider and model candidates.
_Avoid_: Provider model ID

**Candidate Service Level**:
The Model Alias classification of a candidate as full requested service or an explicitly disclosable reduced service, independent of hard protocol-capability eligibility.
_Avoid_: Model size, capability bypass, routing priority

**Provider Model Binding**:
The configured association between one Provider and one opaque upstream model, declaring the capability and Token-limit subset that Gateway may use for routing.
_Avoid_: Model Alias, routing candidate order, Provider runtime health

**Service Requirement**:
A Gateway-internal, provider-independent description of the capabilities and service constraints required for one Model Invocation, derived from the requested Model Alias and validated request.
_Avoid_: Caller model name, configured Provider endpoint, concrete Provider model ID

**Provider Selection Override**:
A permission-controlled diagnostic or administrative request to prefer a configured Provider within the normal capability, health, circuit, security, and evidence policies.
_Avoid_: Arbitrary Provider binding, endpoint override

**Provider Override Mode**:
The explicit `prefer` or `require` routing constraint applied to a configured Provider ID without bypassing ordinary eligibility controls.
_Avoid_: Forced unhealthy call, implicit fallback semantics

**Requested Model**:
The caller-facing configured Model Alias supplied in the standard model field for a Model Invocation.
_Avoid_: Resolved Model

**Model Selection Source**:
The caller-supplied standard `model` field interpreted as a configured Model Alias and locked as the Requested Model.
_Avoid_: Prompt-defaulted model, implicit model substitution

**Resolved Model**:
The concrete Provider and Provider model selected by the Gateway for a Provider Attempt.
_Avoid_: Requested Model

**Model Result**:
The successful provider-independent outcome of a Model Invocation, retaining Gateway execution metadata and, when Structured Output is requested, a locally validated Model Output.
_Avoid_: Raw Provider response, application business result

**Model Failure**:
The provider-independent unsuccessful outcome carrying stable code, stage, disposition, safe details, commitment state, and evidence references, including exhausted Structured Output recovery when applicable.
_Avoid_: Python exception across a Port, raw Provider error

**Model Capability**:
A provider-independent feature that a model endpoint and its Adapter can support, such as streaming, Structured Output, or opaque Tool Calling transport.
_Avoid_: Model name, model size

**Capability Registry**:
The Gateway's authoritative catalog of the Model Capabilities declared by each configured Model Adapter.
_Avoid_: Model list, routing table

**Adapter Capability Contract**:
The versioned protocol-feature ceiling implemented by one Adapter type; configuration may intersect and narrow it but cannot claim behavior the Adapter does not implement.
_Avoid_: Provider marketing claim, mutable runtime health

**Effective Capability Set**:
The immutable intersection of Adapter Capability Contract, Provider Model Binding declaration, and locked published policy used for routing one invocation.
_Avoid_: Adapter maximum alone, runtime guess

**Capability Revocation**:
An audited emergency marker that immediately makes one declared capability or property ineligible without silently rewriting its published historical snapshot.
_Avoid_: Automatic database mutation from one failed request

**Routing Decision**:
The explainable selection outcome for a Model Invocation, including evaluated candidates and the reason each non-selected candidate was rejected.
_Avoid_: Model name lookup

**Routing Decision Event**:
One ordered fact in a Routing Decision describing candidate evaluation, Attempt progression, degradation entry, or terminal selection without rewriting an earlier fact.
_Avoid_: Mutable candidate status, free-form routing log

**Routing Reason**:
A stable, stage-specific explanation for a Candidate rejection, skip, or degradation transition, optionally qualified by a canonical capability, limit, parameter, or Provider-override subject.
_Avoid_: Raw exception, Provider error text, lower-score placeholder

**Routing Evidence Retention Boundary**:
The maximum period during which a complete Routing Decision aggregate remains available for internal Eval or audit, independent from the Invocation shell and Cost Ledger lifetimes.
_Avoid_: Cost retention, partial event deletion, request-selected TTL

**Routing Evidence Effective Expiry**:
The non-extendable end of one Routing Decision's availability, determined by its terminal-time retention promise and any later stricter deployment compliance ceiling.
_Avoid_: Sliding TTL, admission timestamp, cleanup execution time

**Routing Evidence Ceiling History**:
The durable history of effective retention ceilings that preserves an earlier tightening for existing Routing Decisions even after a later ceiling increase.
_Avoid_: Current ceiling only, expiry renewal, routing-event mutation

**Routing Evidence Availability**:
The Status-visible distinction between a retained Routing Decision, an expired one, and one that was never durably established, without disclosing retention or persistence details.
_Avoid_: Invocation lifecycle state, dangling Evidence Reference, raw persistence error

**Routing Policy**:
The published rule set that fixes candidate selection, bounded retry, pre-commit fallback, and the ordered Degradation Policy for a Model Invocation.
_Avoid_: Provider preference alone, caller-defined retry plan, dynamic best effort

**Weighted Candidate Order**:
A deterministic, seeded, weighted-without-replacement permutation of eligible candidates inside one priority tier, reused for initial choice and fallback.
_Avoid_: Hidden composite score, re-randomization after failure

**Candidate Blacklist**:
The derived view of Provider model Candidates currently excluded by invocation-local rejection, Provider Circuit state, or published configuration; it is not an independent health state or network-address list.
_Avoid_: Separate blacklist state machine, IP blacklist, Provider Egress Denylist, dynamic DNS denylist

**Projected Cost Estimate**:
The pre-Attempt estimate calculated from known input, effective maximum output, applicable Token categories, and the locked pricing table for routing evidence without acting as a first-phase spend limit.
_Avoid_: Final billed cost, Cost Budget, unqualified average price

**Provider Health State**:
The runtime eligibility state of a configured Provider, derived primarily from real Provider Attempt outcomes and optionally supplemented by configured active probes.
_Avoid_: Provider availability claim, synthetic model result

**Provider Circuit**:
The single-instance runtime `closed`, `open`, or `half_open` eligibility state for one `provider_binding_id`, driven only by classified Provider-attributable health signals under a published policy.
_Avoid_: Global Provider status, configuration validation

**Stream Event**:
One ordered, provider-independent event in a streaming Model Result, representing a content delta, opaque compatible payload fragment, Usage, or termination.
_Avoid_: Provider chunk, raw SSE frame

**Terminal Outcome**:
The single immutable reason a Model Invocation stream ended: completed, failed, or cancelled.
_Avoid_: Finish reason

**Cancelling**:
A durable non-terminal Model Invocation state that reserves cancellation ownership before best-effort Provider cancellation and final `cancelled` or recovery-time `uncertain` resolution.
_Avoid_: Successful Provider cancellation guarantee, terminal outcome

**Structured Output**:
A Model Output requested against JSON Object or JSON Schema constraints and accepted only after Gateway extraction, parsing, and final local validation succeed.
_Avoid_: Provider conformance claim, opaque JSON-looking text

**Output Schema**:
The caller-supplied JSON Schema Draft 2020-12 document used as a Structured Output contract, restricted to the Gateway's versioned portable keyword profile, an acyclic graph of same-document JSON Pointer references, and the active Output Schema Resource Profile.
_Avoid_: Provider-specific Schema dialect, remote Schema, Prompt template

**Output Schema Resource Profile**:
The deployment-owned structural safety limits applied before routing to an Output Schema: 256 KiB encoded size, 64 JSON nesting levels, 32 edges in any local-reference chain, and 4096 total Schema nodes by default; subordinate policies may only tighten them and callers cannot override them.
_Avoid_: Business Budget, Provider-advertised Schema limit, validation-error collection limit

**Structured Output Resource Profile**:
The deployment-owned hard limits applied incrementally to a model-generated JSON instance: 4 MiB normalized bytes, depth 64, 1024 members per object, 1024 elements per array, 65,536 total value nodes, 262,144 code points and 1 MiB UTF-8 per string, and 256 bytes per number token by default.
_Avoid_: Output Schema Resource Profile, Token or cost budget, caller-provided `max_tokens`

**Pattern Profile**:
The versioned, linear-time regular-expression contract used by the Output Schema `pattern` keyword; first phase uses `re2-portable-v1`, Unicode code-point and JSON Schema substring-search semantics, a 2 KiB encoded pattern ceiling, and no lookaround, backreference, conditional expression, Provider-dialect rewrite, or other unsupported construct.
_Avoid_: Python `re` semantics, Provider-specific regex, full-string match

**Numeric Profile**:
The versioned exact-decimal contract for Schema literals and Structured Output numbers; first phase uses `decimal-exact-v1`, mathematical numeric equality, exact `multipleOf`, preserved output lexemes, a 256-byte number-token ceiling, and absolute exponent at most 1000 without binary-float conversion, rounding, truncation, or coercion.
_Avoid_: IEEE-754 comparison, epsilon tolerance, Provider-normalized number text

**String Profile**:
The versioned Unicode contract for Schema and Structured Output strings; first phase uses `unicode-scalar-v1`, counts decoded Unicode code points, rejects unpaired surrogates, preserves valid output lexemes, and performs no normalization, case folding, trimming, or grapheme-based counting.
_Avoid_: UTF-8 byte count as `maxLength`, locale-sensitive comparison, silent Unicode normalization

**JSON Literal Equality**:
The structural equality used by `enum` and `const`: object member order is irrelevant, array order is significant, strings use exact decoded Unicode scalar sequences, numbers use `decimal-exact-v1` mathematical equality, and JSON primitive types remain distinct; literal contents are data rather than nested Schemas.
_Avoid_: Serialized-text equality, type coercion, Schema-keyword traversal inside literal data

**Object Schema Profile**:
The bounded object-validation contract distinguishing open objects, closed objects, and Schema-constrained additional properties while preserving caller field optionality and output member order; first phase permits only object-form Schema nodes and forbids Provider-driven Schema rewriting.
_Avoid_: JSON object instance, Provider-required all-fields-required rewrite, field defaulting or removal

**Array Schema Profile**:
The bounded homogeneous-array contract using at most one object-form `items` Schema, preserving element order and allowing at most 1024 elements per output array by default without padding, truncation, deduplication, reordering, tuple semantics, or boolean item Schemas.
_Avoid_: `prefixItems`, `uniqueItems`, list repair, heterogeneous tuple Schema

**Schema Type Set**:
The one-to-seven-member unordered set declared by a Schema `type`, drawn from null, boolean, object, array, number, integer, and string; omission permits any JSON type subject to applicable assertions, and no type coercion is performed.
_Avoid_: Ordered union, Provider-specific nullable encoding, runtime value conversion

**Output Schema Identity**:
The three-hash identity of one accepted Output Schema: SHA-256 of original UTF-8 bytes for provenance, SHA-256 of the deterministic canonical contract for replay/cache/fingerprints, and SHA-256 of the routing capability descriptor for candidate matching; raw Schema content is not ordinary telemetry.
_Avoid_: One overloaded Schema hash, raw Schema log field, Provider payload hash

**Adapter Contract Violation**:
A deterministic pre-I/O failure in which an Adapter cannot exactly translate a request that its published Effective Capability Set declared representable; it is a Gateway correctness defect, maps to `internal`, creates no Provider Attempt or fallback, and degrades the affected readiness detail.
_Avoid_: Provider rejection, unsupported caller capability, transient transport failure

**Provider Capability Mismatch**:
A sent Provider Attempt rejected because the Provider does not actually support a capability declared for that binding; it forbids same-candidate retry, permits only pre-commit ordered fallback, affects capability-drift evidence rather than the availability circuit, and never mutates the registry automatically.
_Avoid_: Adapter Contract Violation, ordinary 429/5xx, model-output Schema mismatch

**Compiled Schema Cache**:
A bounded process-local optimization keyed by canonical Schema identity plus validator/profile/resource versions, storing reusable validation plans and sensitive literal-bearing artifacts without changing preflight, authorization, routing, persistence, or result-cache semantics.
_Avoid_: Cached Degraded Result, Redis cache, durable Schema registry, hash-only trust

**Conclusive Incremental Failure**:
A Structured Output violation proven irreversible from the normalized prefix already parsed, allowing immediate failure and upstream cancellation without waiting for unseen output; it never permits early success and records incomplete validation/error enumeration explicitly.
_Avoid_: Speculative Schema failure, successful partial JSON, final validation

**Retryability Decision**:
A policy result classifying one failure as retryable or non-retryable under commitment, cause, Attempt, delay, and deadline limits; it authorizes only an equivalent-request Attempt after bounded backoff and never authorizes Prompt, Schema, model-parameter, or output-mode modification.
_Avoid_: Request repair, semantic degradation, unlimited retry, fallback eligibility

**Schema Evaluation Ceiling**:
The deployment-owned bound on combinator validation work: at most 32 branches per `allOf`/`anyOf`/`oneOf`, 8 nested combinator levels, and 8192 instance-node-by-Schema-node evaluations per final validation by default; it is a resource limit rather than a Business Budget.
_Avoid_: Token budget, Provider Attempt Budget, short-circuit heuristic

**Structured Validation Failure**:
A 4xx contract/input-class failure showing that requested Structured Output could not be extracted, parsed, or validated after the single permitted Local Structured Repair pass; only an eligible pre-commit JSON Structure Failure may authorize one bounded equivalent-request retry, while the failure never affects Provider Health State or circuit counters and never authorizes Provider/model fallback.
_Avoid_: Provider availability failure, circuit input, silent text downgrade, automatic higher-tier-model recovery

**JSON Structure Failure**:
A model-generated Structured Validation Failure whose stable reason is explicitly listed in the versioned structural-retry allowlist; before external commitment it may authorize at most one unchanged-request retry under the ordinary Attempt, deadline, and exponential-backoff limits. New reason codes never become retryable merely because their names begin with `json_`.
_Avoid_: Caller-invalid Schema, Schema mismatch, structured resource limit, post-commit retry, request repair

**Structured Failure Reason**:
A stable safe reason identifying the first caller-actionable Structured Validation Failure category without exposing model content or Provider-specific validator text.
_Avoid_: Raw parser message, complete validation error list, HTTP status

**Presented Structured Failure**:
The caller-visible Structured Failure Reason selected from the first failed Provider Attempt that triggered JSON recovery; if the bounded request retry also fails, its different reason remains internal Evidence and does not replace the originally presented reason.
_Avoid_: Last-Attempt reason, cross-Attempt error merging, retry-dependent public error drift

**Failed Invocation Usage Boundary**:
The rule that an OpenAI-compatible error body carries no `usage`; failed-invocation aggregate and per-Attempt Usage, retry, and repair details remain accessible only through authorized Gateway-native status and Evidence surfaces.
_Avoid_: Usage in error JSON, Usage SSE after terminal error, retry-count response header, hidden internal accounting

**Local Structured Repair**:
A single deterministic pipeline pass over one Provider output that removes a BOM, unwraps one Markdown fence, and selects one unambiguous complete JSON value in that order without rewriting JSON syntax or data.
_Avoid_: Syntax correction, field inference, value coercion, unlimited repair loop, new model generation

**Advanced Structured Recovery**:
A future, explicitly configured custom workflow that may ask a separately selected higher-tier model to repair or regenerate invalid Structured Output; it is outside the first phase and must never appear as an implicit Gateway retry or fallback.
_Avoid_: Current-version behavior, hidden model escalation, Local Structured Repair

**Normalized Structured Output**:
The pure JSON representation returned synchronously or emitted incrementally after deterministic extraction, preserving parsed keys and values without surrounding prose, fences, default insertion, type coercion, or semantic rewriting; final validity is known only at completion.
_Avoid_: Raw model text, canonical JSON with reordered data, repaired business object

**Opaque Tool Payload**:
A bounded OpenAI-compatible Tool-related request, response, or stream fragment carried through API and Adapter boundaries without Gateway semantic parsing, normalization, validation, repair, storage, or execution.
_Avoid_: Tool Calls Output, Gateway Tool model, Loop Tool state

**Effective Generation Parameters**:
The final provider-independent generation settings resolved from Model Alias defaults, caller-supplied supported values, and non-overridable Gateway/model/policy constraints.
_Avoid_: Raw caller parameters, Provider-specific options

**Failure Disposition**:
The Gateway's classified instruction for handling a Gateway/transport failure, distinguishing terminal rejection, caller correction, configuration change, bounded automatic retry, and failover without model-content repair.
_Avoid_: Error message, HTTP status

**Gateway Resource Ceiling**:
A deployment-owned hard upper bound for request/input, opaque field, stream-event, Token, or execution resources; output-format and Tool fields consume generic limits rather than semantic-specific limits.
_Avoid_: Provider-advertised limit, caller preference

**Business Budget**:
A configurable Token, cost, spend, or tenant quota used to admit, reject, or stop model work for commercial allocation purposes; it is deferred beyond the first phase.
_Avoid_: Invocation Deadline Budget, Provider Attempt Budget, Gateway Resource Ceiling

**Invocation Deadline Budget**:
The single monotonic remaining-time budget shared by admission, queueing, outer validation, Provider Attempts, retry/backoff, fallback, and termination for one Model Invocation.
_Avoid_: Per-Attempt reset, client socket timeout

**Provider Attempt Budget**:
The finite invocation-level allowance shared by the initial Provider call, same-candidate retries, and Provider fallbacks; the first-phase maximum is three Attempts.
_Avoid_: Retry count per layer, unlimited fallback list

**Admission Permit**:
A scoped runtime concurrency lease held by a Model Invocation at instance/tenant levels or by its active Provider Attempt at `provider_binding_id` level until the corresponding lifecycle ends.
_Avoid_: QPS token, queued request

**Degraded Outcome**:
An explicitly identified result or failure produced when the requested model service cannot be delivered at its full policy level after bounded recovery options are exhausted.
_Avoid_: Silent fallback, fake success

**Degradation Policy**:
The versioned Routing Policy projection that orders and permits fallback, reduced-capability results, cached/stale results, or explicit failure for a Service Requirement.
_Avoid_: Independent configuration registry, implicit fallback, best effort

**Cached Degraded Result**:
A previously produced Model Result reused under an explicit Degradation Policy after its source, age, freshness limit, and deviation from the requested service are disclosed.
_Avoid_: Silent cache hit, fresh result

**Cache Write Candidate**:
A normally completed, non-exception Model Result that may be stored only after the generic cache policy accepts it; successful Local Structured Repair or bounded request retry does not by itself make the result ineligible.
_Avoid_: Every successful response is cached, recovery history forbids caching

**Cache Result Variant**:
A cached result distinguished by whether its source Candidate delivered full or reduced service, allowing both qualities to coexist without overwriting or hiding one another.
_Avoid_: Last-write-wins service quality, unlabeled cached result

**Cache Write Contention**:
The benign race in which another complete result already occupies the same Cache Result Variant identity, causing a later non-authoritative population attempt to be skipped without refreshing or replacing it.
_Avoid_: Cache failure, result conflict, TTL renewal

**Cache Freshness Window**:
The configured fresh period plus an optional additional stale period that determine whether a cached entry is fresh, explicitly stale-eligible, or expired.
_Avoid_: Redis eviction time, indefinite cache lifetime

**Cache Revocation**:
An immediately enforced security marker that overrides an Invocation Cache Policy Snapshot and makes matching cached entries ineligible before their asynchronous physical deletion completes.
_Avoid_: TTL expiry, best-effort purge

**Cache Security Invalidation Fence**:
The single point at which a Cache Revocation or cache-encryption key invalidation takes effect: an uncommitted cache hit is discarded, later writes are suppressed, and prior data is logically ineligible, while an already emitted cache result remains unchanged; an exhausted live path keeps its existing terminal result.
_Avoid_: Provider cancellation, live-result rollback, best-effort race handling

**Optional Cache Compose Profile**:
An explicitly enabled Docker Compose profile that starts the Redis cache component for policy-controlled Cached Degraded Results; the default Compose startup does not start Redis, and enabling the profile does not enable cache policy by itself.
_Avoid_: Redis as configuration authority, implicit cache dependency, automatic cache enablement

**Cache Backend Connection Profile**:
A deployment-owned description of the Redis endpoint, transport protection, and Secret Reference used by the optional cache stage; it is infrastructure connectivity, not cache behavior or a caller-controlled setting.
_Avoid_: Cache policy, caller Redis URL, domain configuration override

**Cache Backend Unavailable**:
A runtime condition in which an enabled cache policy has no usable connection profile or Redis operation; it makes the cache stage ineligible while leaving the live invocation outcome and Gateway readiness unchanged, and is not caller-visible.
_Avoid_: Gateway not ready, Provider failure, cache hit

**Cache Backend Recovery**:
The automatic re-eligibility of the cache stage on a later eligible invocation after Redis connectivity becomes usable again; it requires no Gateway restart or configuration publication and does not retry the failed cache operation in place.
_Avoid_: Provider failover, readiness recovery, cache replay

**Cache Encryption Key Rotation Boundary**:
The security transition at which a rotated or revoked cache-encryption key overrides Invocation Cache Policy Snapshots and makes existing cached entries immediately ineligible; v0.1 does not use old keys for dual-read or online re-encryption.
_Avoid_: Key rollover migration, dual-key cache read, live cache re-encryption

**Cache Profile Namespace Boundary**:
The logical separation created when a Cache Backend Connection Profile changes; later invocations use a new cache namespace without migrating or reading entries from the prior profile.
_Avoid_: Cross-profile cache reuse, cache migration, Redis data merge

**Cache Restart Semantics**:
The rule that cached results are disposable across Gateway or Redis restarts: a deployment may retain them, but Gateway must operate correctly with an empty cache and never treat restoration as an availability requirement.
_Avoid_: Durable source of truth, replay guarantee, readiness dependency

**Cache Background Work**:
Process-local, best-effort cache population or physical-cleanup work that never delays or changes the live Model Invocation result and may be dropped at capacity, shutdown, or restart.
_Avoid_: Durable cache outbox, live-result checkpoint, cache authority

**Cache Policy Disablement**:
A published cache policy state that makes the cache stage ineligible for Model Invocations admitted under that policy and pauses its logical freshness clock; already admitted invocations retain their locked cache-policy rules. Re-enabling the same policy content may reuse an entry that still exists and satisfies its semantic, isolation, freshness, revocation, retention, and current-key conditions, while explicit revocation or a material policy-content change forces invalidation.
_Avoid_: Synchronous Redis purge, automatic cache revocation, live-path disablement

**Cache Policy Clock State**:
The authoritative logical elapsed-time state associated with one Routing Policy and one cache-material identity, paused by cache disablement and resumable only by the same material identity.
_Avoid_: Redis TTL, wall-clock entry age, disabled cache configuration fields

**Invocation Cache Policy Snapshot**:
The immutable revision-scoped cache policy reference and status, logical freshness-clock state, and cache-connection profile selected when one Model Invocation is admitted; later policy publication does not change it.
_Avoid_: Agent Session, caller session ID, live cache-policy lookup

**Invocation Replay Policy Snapshot**:
The immutable exact-replay mode and effective retention, encryption-key identity, eligibility, ceiling, and access-scope values selected when one Model Invocation is admitted; security invalidation may still override its replay eligibility.
_Avoid_: Live replay-policy lookup, caller retention override, Agent Session

**Disposable Redis Storage**:
The default cache storage mode using temporary Docker storage; an operator may attach a volume, but retained Redis data remains best-effort and outside Gateway continuity guarantees.
_Avoid_: Authoritative persistence, required Redis volume, PostgreSQL backup substitute

**Bootstrap Reload Boundary**:
The explicit lifecycle point at which deployment-owned connection settings become active; a restart or deliberate reload is required, mutable configuration files are not watched automatically, and already admitted invocations retain the profile locked at admission.
_Avoid_: Active Snapshot publication, file hot reload, request override

**Idempotency Key**:
A caller-supplied, optional identifier for one logical Model Invocation, bound to a canonical request fingerprint and its recorded Gateway outcome; a same-key, same-fingerprint duplicate resolves to that original invocation and its admission-locked snapshots rather than starting a new execution, even after later configuration or Provider-state changes. When omitted, the invocation has no idempotency binding.
_Avoid_: Provider request ID, retry counter

**Idempotency Key Namespace**:
The isolation boundary formed by tenant identity, the Stable Authorization Subject, API operation, and the opaque Idempotency Key; equality outside that complete boundary never identifies the same Model Invocation.
_Avoid_: Deployment-global key, tenant-only key, bearer-token namespace

**Stable Authorization Subject**:
The non-empty, durable principal identity whose stability across credential refreshes is guaranteed by the selected Authorization Adapter; it is carried in `Authorization Context.subject` and forms the subject component of an Idempotency Key Namespace.
_Avoid_: Gateway-inferred identity mapping, Bearer Token hash, transient session ID

**Idempotency Index Digest**:
The versioned keyed digest of the unambiguously encoded Idempotency Key Namespace used for PostgreSQL lookup without retaining the caller's raw key.
_Avoid_: Raw Idempotency Key column, unkeyed hash, Request Fingerprint

**Idempotency Index Key Ring**:
The deployment-scoped, bounded set of exactly one active key and at most eight verification-only keys that preserves Idempotency Index lookup across key rotation until every protected Idempotency Binding and Idempotency Tombstone expires.
_Avoid_: Single replace-in-place key, Fingerprint key ring, content-encryption key

**Idempotency Index Key Ring Snapshot**:
The immutable Key Ring membership, roles, and Secret References used for one Gateway process lifetime and replaced only by a complete restart.
_Avoid_: Live key reload, Active Configuration Snapshot, per-request key selection

**Idempotency Index Key Ring Member**:
One declared key identity and immutable version with an active or verification-only role and one exact Secret Reference in an Idempotency Index Key Ring.
_Avoid_: Multi-key Secret bundle, mutable key alias, implicit latest version

**Idempotency Index Key Material Lease**:
The operation-scoped, all-or-nothing set of trusted key bytes resolved for every required Idempotency Index Key Ring Member.
_Avoid_: Process-wide plaintext key cache, partial-key lookup, Provider Credential Lease

**Idempotency Index Security Fence**:
The monotonic boundary that orders an observed Index-key revocation or exact restoration against an uncommitted keyed access so stale material cannot create, disclose, replay, or cancel through that access.
_Avoid_: Provider Circuit, Key Ring rotation, retroactive invocation cancellation

**Index Key Revocation Observation**:
The moment Gateway learns that a required Idempotency Index key can no longer be trusted through a just-in-time resolution result or a supported SecretSource notification; external revocation before that observation is not claimed to be instantaneous Gateway knowledge.
_Avoid_: Secret-manager event time, inferred revocation, retrospective cancellation

**Idempotency Key Lockout**:
The fail-closed state in which every Idempotency-Key-dependent access is unavailable because a still-required Index key cannot be trusted or resolved, cleared only by a newer complete observation of the exact Ring while unkeyed invocation and `call_id` lifecycle access remain available.
_Avoid_: Gateway not-ready state, new-key miss, Provider outage

**Idempotency Protection Horizon**:
The PostgreSQL-time instant `protected_until` through which an Idempotency Binding or Idempotency Tombstone still requires its Idempotency Index key version to remain verifiable.
_Avoid_: Replay expiry, status retention, process-clock deadline

**Idempotency Protection Record**:
The durable non-content record generation that carries one Idempotency Binding Window and its following Key-Reuse Quarantine without a protection gap; after its horizon a single atomic successor may bind the same namespace to a new invocation.
_Avoid_: Asynchronously created Tombstone row, Replay Artifact, invocation result

**Canonical Invocation Operation**:
The stable, versioned identity of the original model-invocation operation within an Idempotency Key Namespace; lifecycle access by key reuses it instead of identifying itself as a new operation.
_Avoid_: HTTP method/path string, Status operation, Cancel operation

**Idempotency Conflict Disclosure**:
The content-free `409 conflict` response for a fingerprint mismatch or quarantined key that reveals only the current access identity and no historical invocation facts.
_Avoid_: Original call disclosure, Artifact existence hint, conflict diff

**Idempotency Key Profile**:
The case-sensitive, non-normalizing header contract for exactly one opaque visible-ASCII value of 1–255 characters.
_Avoid_: Trimmed key, Unicode-normalized key, repeated header values

**Idempotency Binding Window**:
The bounded period in which one Idempotency Key Namespace remains bound to its original Model Invocation and Request Fingerprint.
_Avoid_: Replay Artifact TTL, permanent key reservation, Provider timeout

**Key-Reuse Quarantine**:
The additional non-content Tombstone period after an Idempotency Binding Window ends, during which the old key still cannot create a new invocation.
_Avoid_: Replay retention, status TTL, silent key release

**Idempotent Invocation Reuse**:
Resolution of a same-key, same-Request-Fingerprint duplicate or replay to the original Model Invocation and its admission-locked snapshots, without fresh routing or a new Provider Attempt; a Cache Security Invalidation may still make a cache artifact ineligible.
_Avoid_: Fresh route, current-policy lookup, Provider replay

**Replay Access**:
An HTTP access created by repeating the original Chat Completions request with the same Idempotency Key and Request Fingerprint; it resolves the existing invocation's replay outcome or stored response but is not a new Model Invocation or Provider Attempt.
_Avoid_: New invocation, live stream join, Provider retry

**Replay Entry Point**:
The original `POST /v1/chat/completions` route used with the same Idempotency Key and Request Fingerprint; Replay Artifact identifiers are internal and are never caller-addressable download handles.
_Avoid_: Replay download endpoint, Artifact URL, Control Plane content fetch

**Replay Protocol Binding**:
The immutable synchronous or SSE response mode, including usage-event intent, captured by the original Request Fingerprint and preserved by every exact replay.
_Avoid_: Sync-to-stream conversion, stream-to-sync conversion, replay format override

**Invocation Lifecycle Access**:
The authorized status or cancellation operation for an existing Model Invocation; it exposes lifecycle metadata or requests cancellation but never decrypts or returns Replay Artifact content.
_Avoid_: Replay content endpoint, Artifact download, status-triggered decryption

**Unknown Lifecycle Target**:
The non-disclosing `unknown_or_expired` lifecycle result for a target that is absent, expired, or outside the caller's Idempotency Key Namespace; it contains no historical invocation identity or state.
_Avoid_: Unknown or Expired Invocation, existence-revealing not-found result, Idempotency conflict

**Replay Access Cancellation**:
The termination of one Replay Access transfer caused only by its client disconnect or HTTP request-context cancellation; it stops that delivery and leaves the original invocation and Replay Artifact unchanged.
_Avoid_: Invocation Cancel API, Provider cancellation, Artifact revocation

**Replay Response Cache Suppression**:
The mandatory `Cache-Control: no-store` behavior for exact-replay responses so intermediaries do not retain another plaintext copy; it does not alter the stored Artifact.
_Avoid_: Redis cache policy, Artifact deletion, browser cache opt-in

**Replay Stream Pacing**:
The rule that replayed SSE preserves normalized event order and terminal semantics but not original inter-event timing, while observing ordinary downstream backpressure limits.
_Avoid_: Timing replay, burst bypass, Provider stream reuse

**Idempotency Tombstone**:
The quarantine phase of an Idempotency Protection Record after its Binding Window expires, during which the same namespaced key cannot silently create a new logical invocation.
_Avoid_: Asynchronously inserted marker, Replay Artifact, permanent key reservation

**Request Fingerprint**:
A keyed, versioned digest of normalized caller intent before mutable Gateway configuration or routing resolution, used for Idempotency conflict detection.
_Avoid_: Raw request hash, resolved execution identity

**Execution Fingerprint**:
A keyed, versioned digest of the fully resolved and locked invocation behavior, used for cache identity, replay evidence, and execution audit.
_Avoid_: Caller-intent-only fingerprint, mutable latest configuration

**Fingerprint Key Ring**:
The deployment-scoped, bounded set of one current key and retained verification-only keys shared by Request and Execution Fingerprints while remaining separate from Idempotency Index and content-encryption keys.
_Avoid_: Idempotency Index Key Ring, cache key, Replay key

**Fingerprint Key Protection Horizon**:
The latest dependency expiry through which a Fingerprint key version must remain available for an Idempotency, cache, or Replay operation.
_Avoid_: Routing seed retention, Redis scan result, SecretSource availability

**Fingerprint Key Version Invalidation**:
The irreversible security decision that makes every historical identity, cache, and Replay dependency of one Fingerprint key version unusable and requires a distinct version for future work.
_Avoid_: Temporary SecretSource outage, dependency-specific release, same-version recovery

**Fingerprint Security Fence**:
The ordering boundary that prevents a revoked Fingerprint version from authorizing a later identity comparison, admission, or retention handoff while preserving already admitted execution.
_Avoid_: Provider cancellation, unknown external revocation time

**Routing Evidence Reference**:
An opaque identity linking an authorized Status response to its available Routing Decision without granting permission to read that decision.
_Avoid_: Download URL, access token

**Fingerprint Security Fence**:
The ordering boundary that prevents a revoked Fingerprint version from authorizing a later identity comparison, admission, or retention handoff while preserving already admitted execution.
_Avoid_: Provider cancellation, unknown external revocation time

**Routing Evidence Reference**:
An opaque identity linking an authorized Status response to its available Routing Decision without granting permission to read that decision.
_Avoid_: Download URL, access token

**Fingerprint Key Lease**:
The short-lived exclusive use of one exact Fingerprint key version for deriving or verifying invocation identity, ending before Provider execution.
_Avoid_: Process-wide key cache, Ring fan-out, Provider credential lease

**Fingerprint Key Fault**:
A process-global failure to obtain or validate the current Execution Fingerprint key, which prevents new routing from deriving trustworthy execution identity and makes Data Plane admission non-ready until isolated current-key validation succeeds.
_Avoid_: Idempotency Index Key Lockout, Provider outage, verification-only key fallback

**Idempotency Replay Mode**:
The published policy choosing either execution deduplication without raw-output retention or encrypted exact response/event replay for one Idempotency Key scope; deduplication-only cannot reproduce a successful content response and therefore exposes replay-unavailable rather than fabricated content.
_Avoid_: Automatic Provider replay, caller-selected retention bypass

**Replay Artifact**:
A short-lived, encrypted, authorization-bound synchronous response or normalized SSE event sequence retained only when encrypted exact replay is enabled; reuse also requires current authorization and the original invocation's scope binding.
_Avoid_: Ordinary Invocation Evidence, plaintext response log

**Replay Artifact Wire Form**:
The deterministic UTF-8 Gateway replay envelope containing protocol version, response mode, normalized result or SSE events, and safe lifecycle metadata; Provider raw bytes, headers, and errors are not part of it.
_Avoid_: Provider payload capture, raw HTTP replay, telemetry blob

**Replay Artifact Encoding Profile**:
The v0.1 deterministic, uncompressed serialization profile used for Artifact size accounting and replay; its canonical bytes are measured before encryption together with fixed envelope overhead.
_Avoid_: Compression-dependent size, Provider-native encoding, lossy serialization

**Replay Artifact Encryption Envelope**:
A versioned authenticated-encryption container for a Replay Artifact, carrying ciphertext, nonce, key identity, AAD bindings, and integrity metadata without exposing plaintext content.
_Avoid_: Plaintext retention, Provider encryption, unbound ciphertext

**Replay Artifact Content Hash**:
The digest of the canonical uncompressed Replay Artifact Wire Form computed before encryption and verified after decryption to detect corruption or key/binding mismatch.
_Avoid_: Ciphertext-only hash, Provider payload hash, semantic re-generation

**Replay Key Resolution**:
The just-in-time retrieval of the exact-replay encryption key from SecretSource for one Artifact read or write; key material exists only for that bounded operation and is never persisted or retained in a process-wide cache.
_Avoid_: Key prefetch, durable key storage, long-lived key cache

**Replay Key Material Lifetime**:
The single Artifact operation's ephemeral memory lifetime for resolved key bytes, ending when encryption, decryption, integrity verification, or the operation's failure path completes.
_Avoid_: Request-session key retention, Redis key cache, PostgreSQL key copy

**Replay Key Rotation Fence**:
The security-fence advancement caused by key rotation or revocation that immediately invalidates Artifacts and queued writes using the older key identity without re-encryption or dual-key reads.
_Avoid_: Graceful dual-key migration, old-key replay, Provider credential rotation

**Replay Key Format Contract**:
The fixed SecretSource output contract for exact replay: an immutable key identity/version paired with exactly 32 raw bytes for AES-256; invalid length, algorithm, or encoding is a persistence failure and is never guessed or derived.
_Avoid_: Auto-decoded Base64, variable-length key, implicit key derivation

**Replay Artifact Store**:
The PostgreSQL-owned durable store for encrypted Replay Artifacts and their non-content lifecycle metadata; it is separate from ordinary Evidence, Logs, Trace, Cost Ledger, and disposable Redis cache data.
_Avoid_: Redis cache, object-store authority, response log

**Replay Artifact Commit Boundary**:
The lifecycle boundary at which the original successful result and its Idempotency binding are already durable while exact-replay content remains optional; failure to retain the content cannot revise or regenerate the original result.
_Avoid_: Provider commitment point, transactional response rollback, automatic replay

**Replay Artifact Async Persistence**:
The bounded retention operation that starts after the terminal commit and does not delay a synchronous response or normal SSE completion; its failure affects only later exact replay availability.
_Avoid_: Live response gate, replay-pending join, background regeneration

**Replay Artifact Write Queue**:
A bounded, process-local handoff for eligible Replay Artifact persistence after terminal commit; saturation or process restart may discard pending retention work without changing live execution.
_Avoid_: Durable outbox, Provider work queue, replay-pending state

**Replay Artifact Queue Limits**:
The independent maximums on pending Artifact task count and estimated serialized bytes; either limit can reject a new retention task without applying backpressure to live execution.
_Avoid_: Model concurrency limit, API body limit, unbounded backlog

**Replay Artifact Queue Order**:
The FIFO order of pending Artifact retention tasks by terminal-commit handoff time; retries remain part of their original task and do not displace or evict another task.
_Avoid_: Priority replay, retry reordering, queue eviction

**Replay Artifact Byte Reservation**:
The enqueue-time reservation for normalized retained content plus fixed encryption and metadata overhead; reservation is released when the task is committed or dropped and cannot grow the queue later.
_Avoid_: Actual-size overrun, unbounded allocation, content truncation

**Replay Artifact Persistence Worker**:
A bounded asynchronous worker dedicated to writing Replay Artifacts, separate from Model/Provider concurrency and readiness dependencies.
_Avoid_: Provider worker, invocation executor, durable outbox worker

**Replay Artifact Persistence Pool**:
The dedicated bounded PostgreSQL connection and transaction capacity reserved for Replay Artifact Persistence Worker operations, isolated from live invocation checkpoints.
_Avoid_: Shared live-write pool, Provider connection pool, readiness dependency

**Replay Artifact Queue Saturation**:
A condition in which the process-local Replay Artifact Write Queue has no capacity for a new retention task; the task is dropped with safe evidence and later exact replay remains unavailable.
_Avoid_: API rate limit, Provider overload, live-response backpressure

**Replay Artifact Fixed Expiry**:
The non-sliding retention deadline assigned from the original successful terminal completion; replay access, delayed retention, and restarts cannot extend it.
_Avoid_: Sliding TTL, access-based renewal, indefinite replay

**Replay Artifact Integrity Failure**:
A condition where a referenced Replay Artifact is present but its completeness, authenticity, or decryptability cannot be established; the content is unavailable without exposing it or starting a new invocation.
_Avoid_: Confirmed artifact absence, ordinary expiry, structured-output failure

**Replay Artifact Revocation**:
An immediate policy or security marker, including exact-replay policy withdrawal or encryption-key rotation/revocation, that makes existing Replay Artifacts ineligible for reuse, including by already admitted invocations; physical deletion may complete asynchronously and Provider work is unaffected.
_Avoid_: Replay Artifact expiry, Cache Revocation, Provider cancellation

**Replay Artifact Eligibility**:
The set of complete successful results whose content-retention policy permits encrypted exact replay; partial, interrupted, failed, cancelled, uncertain, validation-invalid, and Safety Refusal outcomes are outside it.
_Avoid_: Every terminal result, raw error retention, automatic regeneration

**Replay Artifact Retention Ceiling**:
The published per-Artifact maximum and deployment-owned aggregate retained-content maximum; exceeding either makes the artifact unwritable without changing the completed live result and leaves later exact replay unavailable.
_Avoid_: Truncated replay, unbounded retention, cache TTL

**Replay Capture Cutoff**:
The point where normalized SSE capture stops because a Replay Artifact or retained-content ceiling would be exceeded, while live forwarding continues to its normal terminal event without truncation or semantic rewriting.
_Avoid_: Stream cancellation, partial replay artifact, live-output truncation

**Replay Artifact Persistence Retry**:
The single bounded retry permitted for a transient PostgreSQL or network failure during asynchronous Artifact persistence; policy, authorization, serialization, and capacity failures are not retried.
_Avoid_: Provider Attempt retry, regeneration, unbounded store retry

**Replay Persistence Retry Gate**:
The eligibility check immediately before a persistence retry that requires the Artifact to remain unexpired, non-revoked, policy-compliant, and within the current security fence; it never extends retention.
_Avoid_: Retry override, stale Artifact write, TTL renewal

**Replay Worker Shutdown Boundary**:
The process-exit point after which uncommitted queued Artifact tasks are discarded; already committed Artifacts remain authoritative and no separate replay drain is promised.
_Avoid_: Live-invocation drain, durable outbox recovery, shutdown TTL extension

**Replay Artifact Task Deadline**:
The bounded time allowed for one queued Artifact persistence task, including its permitted retry, after which the reservation is released and the task is dropped.
_Avoid_: Invocation Deadline Budget, sliding Artifact TTL, unbounded queue wait

**Replay Task Revalidation**:
The Worker-time recheck of policy, Resource Ceiling, fixed expiry, and security Fence for a queued Artifact before its first write; a stricter or revoked state drops the task without altering the live result.
_Avoid_: Admission-only validation, stale-policy write, live-result rollback

**Replay Artifact Write Idempotency**:
The invariant that an Artifact write keyed by immutable `artifact_id`/`call_id` can only confirm or commit the same content, including after an unknown outcome, and cannot duplicate or overwrite it.
_Avoid_: At-least-once mutation, content replacement, Provider idempotency

**Replay Artifact Key Collision**:
A write attempt using an existing immutable Artifact key with a different content hash; the original record remains unchanged and the conflicting write is a persistence failure.
_Avoid_: Idempotent confirmation, record merge, overwrite update

**Replay Artifact Retention Cleanup**:
The non-authoritative removal of expired or revoked Replay Artifacts after their logical ineligibility has already taken effect; cleanup delay never restores replay eligibility or changes a live invocation.
_Avoid_: Revocation decision, live-result deletion, cache eviction

**Replay Artifact Tombstone**:
The minimal non-content lifecycle marker retained after an Artifact's ciphertext becomes expired or revoked, preserving its immutable identity and status at least until the original Idempotency Binding expires; an explicit longer key-reuse quarantine may extend it.
_Avoid_: Deleted response body, plaintext audit record, replayable archive

**Replay Access Fence Check**:
The final Replay Security Invalidation Fence check after Artifact decryption and before the first downstream byte; a pre-emission invalidation discards the access, while an already emitted access is not withdrawn.
_Avoid_: Mid-stream rollback, post-send revocation, Provider cancellation

**Replay Key Failure Isolation**:
The boundary that confines SecretSource key lookup or validation failures to Artifact persistence or Replay Access, leaving Gateway readiness, Provider Health, Circuit state, and live invocation execution unchanged.
_Avoid_: Readiness dependency, Provider outage, automatic regeneration

**Replay Policy Tightening**:
A published restriction that makes previously retained Replay Artifacts no longer satisfy the active exact-replay policy; it changes replay eligibility without changing the completed Model Result or live execution.
_Avoid_: Metadata relabeling, live-result revocation, Provider fallback

**Replay Policy Expansion Boundary**:
A less restrictive exact-replay policy that applies to future retention and still-valid compliant Artifacts but never resurrects expired, revoked, deleted, or skipped content or extends an existing fixed expiry.
_Avoid_: Revocation reversal, TTL extension, artifact resurrection

**Exact Replay Policy Completeness**:
The requirement that an enabled exact-replay policy explicitly state its retention, encryption, eligibility, size, and access constraints before it can govern Artifact retention.
_Avoid_: Implicit retention default, caller-selected replay scope, partial policy

**Replay Policy Section**:
The optional top-level Gateway configuration object that declares the deployment's idempotency replay mode and its exact-replay constraints through the normal revisioned policy workflow.
_Avoid_: Provider setting, Alias override, request flag

**Replay Policy Mode**:
The mutually exclusive choice between `execution_dedup_only` and `encrypted_exact_replay` that determines whether a Model Invocation may retain content for exact replay.
_Avoid_: Cache enable flag, caller response mode, Provider mode

**Replay Policy Numeric Profile**:
The bounded unit contract for published Exact Replay retention: positive integer seconds for TTL and positive integer bytes for the per-Artifact ceiling; aggregate retained-content capacity remains deployment-owned.
_Avoid_: Infinite TTL, zero-as-disable, implicit unit conversion

**Deployment Replay Policy Scope**:
The single Gateway-wide scope of the effective replay policy in the first phase; Provider, Model Alias, Service Requirement, tenant, and caller-specific overrides are outside it.
_Avoid_: Per-model replay policy, tenant override, caller retention choice

**Replay Policy Omission Default**:
The explicit `execution_dedup_only` mode selected when no Replay Policy Section is present; it retains no raw response content and cannot be upgraded for an already admitted invocation.
_Avoid_: Implicit exact replay, cache-derived replay, retroactive enablement

**Replay Policy Validation Boundary**:
The point at which an exact-replay policy is accepted using its declared constraints and Secret Reference without requiring live access to secret material; key availability is a runtime Artifact concern.
_Avoid_: Secret prefetch, online key validation, readiness dependency

**Replay Policy Publication Epoch**:
The versioned, atomically published pairing of an exact-replay policy and its revocation state that orders replay eligibility decisions.
_Avoid_: Mixed policy state, process-local toggle, caller policy version

**Replay Security Invalidation Fence**:
The single point at which Replay Artifact Revocation takes effect: an uncommitted replay is discarded, later artifact use is blocked, and an already emitted replay remains unchanged.
_Avoid_: Cache Security Invalidation Fence, response rollback, Provider retry

**External Commitment Point**:
The moment the first business-meaningful output fragment is emitted to a caller; for Structured Output this is the first normalized JSON byte, after which the Gateway must not transparently regenerate or replace the Model Result.
_Avoid_: Provider connection, first Provider chunk

**Stream Commitment Barrier**:
The API-layer barrier crossed immediately before the first downstream flush of HTTP 200/SSE headers and a valid first business or normal-terminal event; before it, ordinary JSON errors remain possible, and after it only the streaming terminal protocol is possible.
_Avoid_: Provider connection success, early SSE heartbeat

**Downstream Backpressure**:
Bounded waiting caused by a streaming caller consuming SSE more slowly than the Gateway receives or serializes Provider Events.
_Avoid_: Provider stream idle, unlimited response buffering

**Safety Refusal**:
A Provider or Gateway outcome that declines content because of an applicable safety policy and must not trigger model switching intended to bypass that policy.
_Avoid_: Provider failure, routing failure

**Safety Policy Reference**:
The immutable policy identity and enforcement evidence attached to a Safety Refusal, including Gateway policy Resource ID, Configuration Revision, content digest, enforcer, normalized refusal reason, Provider policy identity when reported, and Provider Attempt correlation.
_Avoid_: Free-text refusal message, inferred Provider policy version

**Uncertain Provider Outcome**:
A Provider Attempt for which the Gateway cannot determine whether the Provider processed, completed, or billed the request, typically after a timeout or connection loss.
_Avoid_: Failed attempt, cancelled attempt

**Execution Correlation**:
The hierarchy of `task_id` for a task, `turn_id` for a Loop Step, `step_id` for an operation within that step, `call_id` for the Model Invocation, and W3C `trace_id` for the distributed call chain. Non-Agent callers require only `call_id` and `trace_id`; optional business IDs remain absent rather than fabricated.
_Avoid_: Provider request ID

**Time to First Token (TTFT)**:
The latency from Gateway receipt of a Model Invocation until the first externally emitted, business-meaningful delta; connection establishment, heartbeats, and protocol-only events do not satisfy it.
_Avoid_: Connection time, time to first SSE event

**Gateway Added Latency**:
The time attributable to Gateway admission, routing, policy, translation, persistence, and forwarding work, measured separately from Provider queueing and generation latency.
_Avoid_: End-to-end TTFT, Provider generation time

**Acceptance SLO Profile**:
A reproducible hardware, transport, concurrency, QPS, and duration profile used to verify Gateway performance without claiming a Provider or production availability SLA.
_Avoid_: Production SLA, Provider benchmark

**Invocation Evidence**:
The durable, structured facts describing one Model Invocation and its Provider Attempts, routing, usage, latency, resilience, result, errors, estimated cost and safe Prompt-version provenance; it is not a store of raw Prompt variables or rendered messages.
_Avoid_: Log line, Trace span

**Agent Loop**:
The Agent Engineering Platform component that owns Tool behavior/execution, iteration control and agent state while using Gateway for model execution and optionally Gateway-managed Prompt assets.
_Avoid_: LLM Gateway, Model Adapter

**Prompt Asset**:
A Gateway-managed named template asset for producing model messages from declared variables.
_Avoid_: Provider request, Agent memory, arbitrary executable template

**Prompt Version**:
An immutable edition of a Prompt Asset, distinct from a model configuration revision and from the variable values of a particular call.
_Avoid_: Mutable template, latest at every Attempt

**Prompt Publication**:
The explicit selection of a Prompt Version for subsequent resolution, without changing the version already selected by an in-flight Model Invocation.
_Avoid_: Implicit model configuration publication, retroactive instruction replacement

**Loop Step**:
One unit of Agent Loop progression identified by a `turn_id` and capable of causing multiple Model Invocations.
_Avoid_: Model Invocation, Provider Attempt

**Call ID**:
The opaque, Gateway-issued canonical lowercase UUIDv4 identifier of one Model Invocation within a Loop Step.
_Avoid_: Caller-selected identifier, Provider request ID, turn ID

**Data Plane**:
The Gateway model-invocation surface that accepts Model Invocation requests, returns Model Results, and executes Provider Attempts.
_Avoid_: Control Plane, management API

**Compatibility Profile**:
A versioned explicit subset and behavior contract for an external OpenAI-compatible API family; upstream additions do not become Gateway features until deliberately adopted.
_Avoid_: Pass-through of every current Provider/OpenAI field, unversioned compatibility claim

**Control Plane**:
The privileged Gateway management surface for Providers, Model Aliases, routing, resilience, pricing, resource, safety, health policy and Prompt assets; it does not own Tool execution.
_Avoid_: OpenAI-compatible invocation API, Data Plane

**Configuration Revision**:
A complete immutable validated configuration snapshot identified by one positive monotonic revision number and classified as Candidate, Active, Superseded, or Stale; distinct revisions may intentionally share one content digest.
_Avoid_: Mutable configuration row, revision zero, validation failure

**Candidate Configuration Revision**:
A complete, immutable, validated-but-not-active configuration snapshot created from a Control Plane mutation or explicit YAML import and based on one declared Active revision, or on the Initial Configuration Base while no Active revision exists.
_Avoid_: Active configuration, partial patch in memory

**Superseded Configuration Revision**:
A Configuration Revision that was once Active and became historical when a later Candidate was published.
_Avoid_: Stale Candidate, deleted revision, rolled-back revision number

**Initial Configuration Base**:
The synthetic empty configuration base from which one or more initial Candidate Configuration Revisions may be created while no Active revision exists; it is not itself an active or publishable snapshot.
_Avoid_: Default Provider configuration, implicit Active Snapshot, database seed

**Active Revision Precondition**:
The operator's expected current Active revision, checked atomically when publishing, rebasing, or creating a rollback Candidate so the command cannot silently act against a newer base.
_Avoid_: Candidate base revision, runtime model version, published configuration value

**Configuration Resource ID**:
The stable identity of a named resource within its configuration registry, used by explicit references across configuration content; changing that identity constitutes removal and creation of a resource.
_Avoid_: Configuration Revision, display label, Provider model name

**Configuration Change Description**:
The operator's explanation of a configuration command, retained with its creation or publication history independently of executable configuration content.
_Avoid_: Prompt content, runtime policy, Snapshot identity

**Configuration Bundle**:
The canonical full-snapshot configuration representation whose transport envelope carries one declared base and whose domain content contains explicit stable identities, references, policy registries, and Gateway-wide policy singletons without runtime state or secret values.
_Avoid_: Environment-substituted template, partial patch file

**Resolved Configuration Snapshot**:
The complete immutable, default-materialized, non-secret Gateway domain configuration used as configuration content identity and publication input; transport metadata, revision history, deployment hard ceilings, and runtime observations are outside it.
_Avoid_: Configuration Bundle envelope, Bootstrap Configuration, runtime health snapshot

**Configuration Submission**:
The closed input union by which an operator proposes either one complete Configuration Bundle or one typed Configuration Change Set against a declared base.
_Avoid_: Arbitrary JSON Patch, database mutation payload, unversioned YAML document

**Minimal Configuration Bundle**:
A complete Configuration Bundle with the smallest live topology needed for a first Smoke: one Provider, one Provider Model Binding, one Model Alias, and explicit applicable policy sections.
_Avoid_: Partial configuration, implicit default snapshot, Provider-only bootstrap

**Configuration Change Set**:
The canonical order-independent set of whole-resource operations stored with every Candidate and replayed during rebase; it may be empty when the resulting configuration content is unchanged.
_Avoid_: Database-row CRUD, arbitrary JSON Patch

**Configuration Operation**:
One base-relative intent to add, replace, or remove a complete registry resource, or to replace one Gateway-wide policy singleton.
_Avoid_: Field patch, ordered script statement, database-row operation

**Rebase Conflict**:
A divergence at one Configuration Operation target where current Active content matches neither the operation's original Base state nor its intended result.
_Avoid_: Any concurrent publication, field-level merge conflict, dangling reference

**Configuration Diagnostic**:
A bounded, safe, machine-readable reason and logical location describing one configuration structure, semantic, or rebase problem without exposing protected configuration values.
_Avoid_: Raw parser exception, Provider error payload, free-form debug message

**Configuration Impact**:
The deterministic, resource-bounded summary of registry resources and policy singletons that a valid Configuration Submission would add, replace, or remove.
_Avoid_: Raw configuration diff, runtime blast-radius prediction, audit event

**Configuration Validation Result**:
The no-write determination that a Configuration Submission is valid with a prospective Snapshot identity and Change Set, or invalid with safe diagnostics.
_Avoid_: Candidate Configuration Revision, reusable validation token, mutation response

**Configuration Resource Version**:
The immutable content of one Configuration Resource ID within one Configuration Revision; it has no separately assigned operator version number.
_Avoid_: Adapter protocol version, Schema version, independent resource counter

**Administrative Status**:
The explicit enabled or disabled state of a Provider or Provider Model Binding in a Configuration Snapshot; a disabled target remains referentially valid but is ineligible for routing.
_Avoid_: Provider Health State, Provider Circuit State, deleted resource

**Configuration Semantic Failure**:
A structurally valid configuration whose references, capabilities, policies, or other domain relationships cannot form a valid Candidate.
_Avoid_: Malformed request, concurrent configuration conflict, Provider runtime failure

**Configuration Snapshot Digest**:
The versioned content identity shared by prospective validation, Candidate publication, Active configuration, and deterministic export for the same resolved non-secret configuration; it identifies content rather than configuration history.
_Avoid_: Revision number, audit-event hash, hash of secret values

**Configuration Command ID**:
A caller-issued canonical UUIDv4 that identifies one state-changing Control Plane command independently of model-invocation idempotency.
_Avoid_: Idempotency Key, Call ID, Trace ID

**Configuration Command Digest**:
The versioned content identity of one normalized state-changing Control Plane command, used with its Configuration Command ID to distinguish a safe repeat from conflicting reuse.
_Avoid_: Configuration Snapshot Digest, raw HTTP-body hash, Model Request Fingerprint

**Configuration Command Index**:
The durable record of a schema-valid state-changing Configuration Command's identity, digest, and replayable deterministic outcome.
_Avoid_: Model Invocation Idempotency Index, transient failure cache, authentication failure log

**Configuration Read Projection**:
The authorized, secret-safe metadata and semantic-difference view of Configuration Revision history; full configuration content remains an explicit export concern.
_Avoid_: Active runtime snapshot, audit log, configuration mutation API

**Stale Candidate**:
A valid Candidate Configuration Revision whose base no longer matches the Active Snapshot after another publication and therefore cannot publish without explicit rebase.
_Avoid_: Automatically merged candidate, last-write-wins update

**YAML Synchronization View**:
A revision-specific, secret-free import/export representation of canonical PostgreSQL configuration; an Active view may serve as an apply baseline, while a non-Active view is archival until an operator explicitly selects a valid current base.
_Avoid_: Runtime source of truth, live watched file

**Bootstrap Configuration**:
The minimal deployment infrastructure configuration required before PostgreSQL and the Active Configuration Snapshot can be loaded, limited to environment identity, Authorization Integration Mode, process binding, database and SecretSource access, the Idempotency Index Key Ring, an optional Cache Backend Connection Profile, telemetry export, Schema compatibility, and optional first initialization input.
_Avoid_: Provider, routing, cost, resilience, or degradation policy

**Migration Job**:
The independent deployment component that applies ordered PostgreSQL Schema migration artifacts before Gateway admission; it is separate from Gateway startup and does not publish domain configuration.
_Avoid_: Gateway auto-migration, configuration publication, database seed script

**Provider Credential Lease**:
The short-lived in-memory result of resolving one Secret Reference for one Provider Attempt, carrying a non-secret version identity and validity/revocation state.
_Avoid_: Persisted plaintext credential, snapshot-embedded secret value

**Authorization Authority**:
An external identity and authorization system that authenticates callers and supplies the claims or decisions the Gateway enforces.
_Avoid_: Gateway permission store, built-in role manager

**Online Authorization Introspection Adapter**:
The versioned integration that exchanges a caller credential for a normalized Authorization Context without importing an external system's user, role, or policy model into Gateway.
_Avoid_: Vendor role object, Gateway user directory, per-request authentication fallback

**Authorization Introspection Decision**:
The closed online-Authority result that either authenticates a caller with the exact fields needed to form an Authorization Context or declares the credential unauthenticated without returning an identity.
_Avoid_: HTTP transport status, Gateway permission decision, external user record

**Local JWT Verification Profile**:
The deployment-fixed cryptographic, key-selection, claim, temporal, and parsing contract by which a locally verified credential may form an Authorization Context.
_Avoid_: Caller-selected algorithm, remote JWKS policy, generic JWT feature set

**Authorization Identity String**:
A bounded opaque identity value whose exact UTF-8 form is preserved after validation rather than trimmed, case-folded, or Unicode-normalized.
_Avoid_: Display name, normalized username, credential-derived identity

**Matched Authorization Audience**:
The single configured logical audience that an Authorization Adapter proved applicable to an accepted credential and placed in the Authorization Context.
_Avoid_: Unchecked audience claim array, caller-selected audience, audience wildcard

**Authorization Scope Set**:
The required, possibly empty, normalized set of exact permission strings carried by an Authorization Context.
_Avoid_: Role hierarchy, wildcard grant, missing authorization field

**Authorization Scope Token**:
A bounded syntactically valid ASCII member of an Authorization Scope Set; an unknown token may be retained but grants no Gateway permission.
_Avoid_: Role name, wildcard expression, malformed claim

**Gateway Permission Scope**:
A closed, case-sensitive permission string required for one Gateway operation or configuration domain and matched exactly against the Authorization Scope Set.
_Avoid_: Implicit admin, prefix grant, external role name


**Authorization Context**:
The Gateway-owned normalized identity context containing exact subject, tenant, issuer, Matched Authorization Audience, UTC expiry, a possibly empty Authorization Scope Set, and the closed verification method used for one request.
_Avoid_: External role object, raw JWT claims

**Effective Authorization Expiry**:
The strict UTC upper boundary before which a verified credential may enter the Model Invocation admission checkpoint after applying the selected Authorization Adapter's fixed temporal profile; equality is already expired.
_Avoid_: In-flight cancellation deadline, Provider timeout, status-record retention

**Authorization Verification Method**:
The closed Context value `dev_bypass`, `local_jwt`, or `online_authority` that records which verification path produced the Authorization Context rather than guessing the credential's encoding.
_Avoid_: Token type inference, fallback chain, request-selected mode

**Authorization Service Credential**:
The SecretSource-resolved credential by which Gateway authenticates itself to an online Authorization Authority, distinct from both the caller credential and the dedicated Probe credential.
_Avoid_: Forwarded caller Bearer token, Provider credential, shared probe identity

**Authorization Credential Lease**:
The operation-scoped in-memory result of resolving either the live or Probe Authorization Service Credential, discarded after that Authority operation.
_Avoid_: Process-lifetime credential cache, caller credential, Provider Credential Lease

**Authorization Dependency Failure**:
The inability of a required online Authorization Authority to produce a decision because that external dependency is unavailable.
_Avoid_: Rejected credential, insufficient permission, malformed Authorization Context

**Authorization Context Contract Failure**:
An individual result for which the selected Authorization Adapter accepted authentication but cannot form its promised Gateway Authorization Context because a mandatory normalized field is missing or invalid.
_Avoid_: Authority outage, invalid caller credential, authorization denial

**Authorization Integration Mode**:
The single deployment-selected authentication integration used by a Gateway instance: development bypass, local JWT validation, or an online Authorization Authority.
_Avoid_: Startup Mode, per-request auth choice, fallback authentication chain

**Authorization Dependency State**:
The process-local `unknown`, `available`, or `unavailable` aggregate of the Authorization Probe Gate and any unresolved Live Authorization Fault that controls authorization-dependent readiness without becoming a Provider Circuit.
_Avoid_: Provider Health State, persisted authority status, per-request authorization result

**Authorization Readiness Probe**:
A bounded, caller-independent, versioned contract check that proves the dedicated Probe path is usable but cannot by itself clear a failure observed on live introspection.
_Avoid_: Replayed caller authentication, readiness-time network request, Provider health probe

**Authorization Probe Gate**:
The process-local readiness condition that records whether a current-generation Authorization Readiness Probe has proved the dedicated Probe path usable.
_Avoid_: Live introspection health, Provider Circuit, persistent dependency record

**Live Authorization Fault**:
A latched observation of an eligible online-Authority or service-credential dependency failure that only a later protocol-valid live decision can clear.
_Avoid_: Probe failure, caller cancellation, local capacity exhaustion, unauthenticated decision, Authorization Context Contract Failure

**Startup Mode**:
The deployment-selected `dev` or `production` operating context that determines whether Gateway API authorization is enforced; it is fixed outside request handling.
_Avoid_: Request-level auth switch, caller-selected environment

**Development Authorization Context**:
The deterministic context used when `Startup Mode=dev` bypasses JWT and permission checks, with fixed development identity, issuer, audience, expiry, scope marker, and verification method values.
_Avoid_: Anonymous request, production identity, missing correlation context

**Development SecretSource**:
The explicitly selected dev-only credential resolver that reads a Docker Secret file by `secret_ref`, or an opted-in named environment variable, without placing the value in Gateway configuration or caller-visible data.
_Avoid_: Caller-supplied credential, plaintext configuration value, production Secret Manager
