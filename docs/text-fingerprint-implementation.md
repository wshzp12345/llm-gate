# Text fingerprint implementation profiles

These profiles implement identity for the currently accepted synchronous text subset, not the full frozen v0.1 request surface. The HTTP adapter rejects Tools, output Schema, Streaming, Provider overrides, caller deadline and semantic Gateway extensions before creating this query. Supporting those features requires their normalized inputs and a deliberate profile update, not silently dropping them. No Idempotency comparison, cache lookup or live admission is wired by this increment.

Both profiles use HMAC-SHA-256 with the same operation-scoped Fingerprint lease used for Routing seed derivation. The message is the ASCII profile name with no terminator, followed by RFC 8785 canonical JSON bytes. The result is an immutable non-content identity containing canonicalization profile, algorithm, key ID/version and 64 lowercase hex digest. Neither the canonical message nor key bytes are returned or persisted. Request and Execution domain separators differ.

## Request: gateway.request-fingerprint/text-v1

Projection contains fixed `POST /v1/chat/completions`, normalized tenant/subject/issuer/audience/sorted scopes, requested Alias, ordered role/content messages and caller-provided output limit/temperature/top-p. Omitted generation values remain null; they are not resolved against current configuration. Numeric spellings follow RFC 8785 and message strings are not Unicode-normalized or interpreted. The supported subset fixes stream/include-usage to false, opaque Tools and output Schema to null, extensions to an empty object and caller invocation deadline to null.

Authorization expiry and authentication mechanism are not scope identity; credentials, request/Trace IDs, transport context and Idempotency Key are absent. Raw body byte count is retained separately for configured admission limits but is excluded from semantic identity. Deprecated output-limit spelling and explicit `store:false` warnings do not change normalized intent. The API rejects `store:true` and unsupported alternatives before this projection. Request calculation accepts no Active Snapshot and therefore cannot change when operator configuration changes.

## Execution: gateway.execution-fingerprint/text-v1

Projection contains requested Alias, canonical message hash, effective generation values, Alias output ceiling, all locked Candidates sorted by Binding ID, referenced Binding/Provider behavior, routing and safety policy values and fully resolved resource limits. It describes the complete original routing contract, not whichever Attempt later happens to succeed. No selected Provider response or dynamic Circuit state enters it. This text profile fixes Schema/Tools absent and extraction disabled with no pipeline.

Binding pricing references/tables, unrelated resources, raw Snapshot revision/digest, evidence retention and Trust Bundle labels are excluded. TLS behavior retains the content-addressed bundle identity instead of copying PEM. Effective generation omits provenance, so an explicit value and an identical resolved default have the same Execution identity even when their Request identities differ. Provider configuration includes exact Secret References but never resolved material. All six resource limit values must already be resolved from deployment inheritance; no `inherit` fallback is invented.

## Verification boundary

Python tests cover caller intent, authorization scope, generation normalization, request/execution separation, behavioral changes, revision/price/label exclusions, candidate-order independence and unresolved-limit rejection. An independent Node canonical-JSON/HMAC vector is executable with `node tests/reference/text_request_fingerprint_v1.cjs`. A query-field coverage test forces future additions to make an explicit profile decision. These are component checks, not proof of complete v0.1 Idempotency or semantic-cache acceptance.
