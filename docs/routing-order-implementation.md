# Routing order implementation profile

This records the byte encoding used by the seed-derivation and weighted-order implementation of FR-986. The pure order function still accepts an already derived/persisted canonical 64-lowercase-hex seed. Derivation uses the existing active Fingerprint operation lease, without another secret read or runtime randomness. These primitives do not themselves assemble admission, hard eligibility, dynamic Attempt gates or durable routing Evidence.

## Seed derivation

The HMAC-SHA-256 message starts with ASCII `gateway.routing-seed/v1`, with no terminator or length prefix, followed by exactly three fields. Each field is `uint32-big-endian(byte_length) || value`: canonical lowercase hyphenated Invocation UUIDv4 ASCII, Routing Policy Resource ID UTF-8, and positive Configuration Revision decimal ASCII without leading zeros. The key is the same 32 raw bytes held for Request and Execution Fingerprints. The returned seed is the 32-byte HMAC encoded as 64 lowercase hex characters. Historical comparison leases cannot derive it; persisted seeds remain usable by historical Eval without material resolution or key-retention extension.

For synthetic key bytes `00` through `1f`, call `12345678-1234-4234-9234-123456789abc`, policy `routing.primary`, and revision `9223372036854775807`, the seed is `baa5364da88093b83a64f41f324fe1ef9683c0e9859d10ada585a3c494716e90`. Python and `node tests/reference/routing_seed_v1.cjs` independently verify this vector.

## Counter block

For each pick, decode the seed into 32 bytes and use it as the HMAC-SHA-256 key. The message is the ASCII bytes `gateway.routing-seed/v1/order` followed by four fields, in order: service level, priority, pick index, rejection counter. Each field is `uint32-big-endian(byte_length) || ASCII(value)`. Service level is exactly `full` or `reduced`; integers use unsigned decimal text without leading zeros. Pick and rejection counters start at zero; the rejection counter resets for every pick. The domain separator has no terminator or length prefix.

Interpret the 32-byte digest as an unsigned big-endian integer. Let `S = 2^256`, `W` be the remaining integer-weight sum, and `cutoff = S - (S mod W)`. Reject values greater than or equal to cutoff and increment the rejection counter. Otherwise use `value mod W` in cumulative weight intervals, select the Candidate and remove it.

Before drawing, separate full/reduced service and ascending priority tiers. Inside each tier, positive-weight Candidates start in Binding Resource ID UTF-8 byte order. Zero-weight Candidates receive no order. Each tier has its own zero-based pick sequence. The returned full and reduced orders are separate: computing a reduced order does not authorize entering degradation.

## Golden vectors

The seed is `0123456789abcdef` repeated four times. Counter block inputs `(full, 0, 0, 0)` produce `adc35212dcfac7f6e66041c8427ff0f21510eec0824311d7d58956f87228d8a9`; `(reduced, 65535, 2, 1)` produce `19b7f72d1d7142dc27778b32725854f3bf76228daf8e5a7fdef17051b860a3f3`.

For full-service priority-zero Candidates `binding-a:1`, `binding-b:7`, `binding-c:3`, the order is `binding-b`, `binding-c`, `binding-a`, independent of submission order. Python tests and an independent Node BigInt/HMAC reference verify these vectors. Run the latter with `node tests/reference/routing_order_v1.cjs`; Node is not a Gateway runtime dependency.

Changing this encoding or integer sampling changes replay behavior and requires an explicit profile/version decision and new golden vectors. End-to-end admission and replay persistence integration remain pending.
