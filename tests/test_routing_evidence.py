from dataclasses import replace

import pytest

from llm_gateway.domain.routing_evidence import EvidenceCandidate, RoutingPreselection, ordered_reasons
from llm_gateway.domain.routing_eligibility import StaticRoutingReason, StaticServiceRequirement
from llm_gateway.domain.routing_order import WeightedCandidate


DIGEST = "sha256:" + "1" * 64
REQUIREMENT = StaticServiceRequirement(False, False, "none", 32)


def preselection():
    return RoutingPreselection(DIGEST, "default", DIGEST, DIGEST, "a" * 64, REQUIREMENT,
        tuple(EvidenceCandidate(WeightedCandidate(name, "full", index, 1), index)
              for index, name in enumerate(("a", "b", "c"))))


@pytest.mark.parametrize("reason", [
    StaticRoutingReason("unregistered"), StaticRoutingReason("health_unavailable", "raw provider text"),
    StaticRoutingReason("capability_mismatch", "raw Schema"), StaticRoutingReason("limit_exceeded", "999"),
    StaticRoutingReason("parameter_unsupported", "max_tokens"),
    StaticRoutingReason("provider_override_mismatch", "other"),
    StaticRoutingReason("capability_mismatch", "structured_output.feature.unregistered"),
])
def test_only_registered_content_free_reasons_are_accepted(reason):
    with pytest.raises(ValueError):
        ordered_reasons((reason,), requirement=REQUIREMENT)


def test_reason_order_and_locked_subject_registry():
    reasons = (StaticRoutingReason("provider_override_mismatch", "provider"),
               StaticRoutingReason("capability_mismatch", "structured_output.feature.enum"),
               StaticRoutingReason("weight_zero"))
    assert ordered_reasons(reasons, requirement=replace(REQUIREMENT, provider_override="provider"),
                           feature_ids=frozenset({"enum"})) == tuple(reversed(reasons))


def test_duplicate_reasons_are_rejected():
    with pytest.raises(ValueError):
        ordered_reasons((StaticRoutingReason("weight_zero"),) * 2, requirement=REQUIREMENT)


def test_initial_order_matches_seed_not_submitted_order():
    snapshot = preselection()
    assert replace(snapshot, candidates=tuple(reversed(snapshot.candidates)))
    wrong = (replace(snapshot.candidates[0], initial_order=1),) + snapshot.candidates[1:]
    with pytest.raises(ValueError):
        replace(snapshot, candidates=wrong)


def test_rejected_candidate_has_reasons_and_no_initial_order():
    snapshot = preselection()
    rejected = EvidenceCandidate(WeightedCandidate("d", "full", 0, 0), None, (StaticRoutingReason("weight_zero"),))
    assert replace(snapshot, candidates=snapshot.candidates + (rejected,))
    with pytest.raises(ValueError):
        replace(rejected, initial_order=3)
    with pytest.raises(ValueError):
        replace(rejected, reasons=())


@pytest.mark.parametrize("changes", [
    {"alias_digest": "raw text"}, {"seed_hex": "invalid"}, {"routing_policy": "raw policy text"},
    {"feature_ids": frozenset({"raw feature"})},
])
def test_preselection_identity_is_closed(changes):
    with pytest.raises(ValueError):
        replace(preselection(), **changes)
