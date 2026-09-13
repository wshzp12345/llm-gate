# LLM Gateway Design Documentation

This directory is the source of truth for the LLM Gateway design process.

## Documents

| Document | Purpose | Status |
|---|---|---|
| [Requirements](requirements.md) | Goals, scope, use cases, constraints, and success criteria | Approved |
| [Architecture](architecture.md) | Module responsibilities, deployment boundaries, state ownership, and major decisions | Approved |
| [Detailed Design](detailed-design.md) | DTOs, protocols, lifecycle, failure semantics, security, and migration | Approved |
| [Test and Acceptance](test-acceptance.md) | Test seams, cases, negative evidence, and completion gates | Approved |
| [Initial Scope v0.1](scope-v0.1.md) | Frozen first-phase delivery boundary and v1 deferrals | Approved |

## v0.1 approval baseline

2026-09-13 amendment: the user explicitly moved basic Prompt template/version/publication/rendering ownership into Gateway while prioritizing [six basic capabilities](basic-capabilities.md). [ADR-0148](adr/0148-gateway-owned-basic-prompt-management.md) supersedes the corresponding older Prompt exclusions. This scope approval is not implementation acceptance and does not automatically restore previously superseded Prompt protocol designs.

The user approved freezing v0.1 on 2026-09-07. Approved status applies to the scope in `scope-v0.1.md` and its supporting requirements, architecture, detailed design, and acceptance criteria; v1 candidates remain deferred and are not implementation scope. Approval authorizes implementation, not a claim that implementation or acceptance testing is complete. Subsequent scope changes require an explicit recorded decision.

FR-719 governs Usage estimation. FR-964 and its design/test projections are aligned: an omitted or null tokenizer does not enable a conservative estimator. Without trustworthy Provider Usage or a successful compatible version-locked tokenizer estimate, Usage is `unavailable`, never zero-filled.

The first delivery milestone is Docker startup, API configuration publication, and one synchronous real-Provider model invocation through the public API. This milestone does not replace the remaining v0.1 acceptance gates.

## Implementation workflow

1. Confirm requirements and exclusions.
2. Review and approve the architecture.
3. Review and approve the detailed design.
4. Confirm test seams and acceptance criteria.
5. Start implementation only after the preceding documents are consistent.

Document status uses `Draft`, `Approved`, `Superseded`, or `Completed`. A draft does not authorize implementation.
