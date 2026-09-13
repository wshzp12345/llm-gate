from dataclasses import replace
from itertools import permutations

import pytest

from llm_gateway.domain import routing_order as routing
from llm_gateway.domain.routing_order import WeightedCandidate as Candidate, weighted_candidate_order


SEED = "0123456789abcdef" * 4
CANDIDATES = (Candidate("binding-a", "full", 0, 1), Candidate("binding-b", "full", 0, 7),
              Candidate("binding-c", "full", 0, 3))


def test_v1_order_golden_vector():
    assert routing._block(bytes.fromhex(SEED), "full", 0, 0, 0).hex() == "adc35212dcfac7f6e66041c8427ff0f21510eec0824311d7d58956f87228d8a9"
    assert weighted_candidate_order(CANDIDATES, SEED).full == ("binding-b", "binding-c", "binding-a")
    assert routing._block(bytes.fromhex(SEED), "reduced", 65535, 2, 1).hex() == "19b7f72d1d7142dc27778b32725854f3bf76228daf8e5a7fdef17051b860a3f3"


def test_order_is_independent_of_submitted_candidate_order():
    expected = weighted_candidate_order(CANDIDATES, SEED)
    for variation in permutations(CANDIDATES):
        assert weighted_candidate_order(variation, SEED) == expected
    assert set(expected.full) == {candidate.binding for candidate in CANDIDATES}


def test_priority_and_service_levels_are_separate():
    candidates = (*CANDIDATES, Candidate("later", "full", 1, 10000), Candidate("reduced", "reduced", 0, 10000),
                  Candidate("disabled", "full", 0, 0))
    result = weighted_candidate_order(candidates, SEED)
    assert result.full[:-1] == weighted_candidate_order(CANDIDATES, SEED).full
    assert result.full[-1] == "later" and result.reduced == ("reduced",)
    assert "disabled" not in result.full


def test_empty_or_disabled_candidates_have_no_order():
    assert weighted_candidate_order((), SEED).full == ()
    assert weighted_candidate_order((Candidate("disabled", "full", 0, 0),), SEED).full == ()


@pytest.mark.parametrize("seed", [None, "", "a" * 63, "A" * 64, "g" * 64, "a" * 65])
def test_invalid_seed_is_not_replaced_with_randomness(seed):
    with pytest.raises(ValueError):
        weighted_candidate_order(CANDIDATES, seed)


@pytest.mark.parametrize("changes", [{"weight": -1}, {"weight": True}, {"weight": 1.5}, {"weight": 10001},
                                     {"priority": -1}, {"priority": True}, {"binding": "../bad"}, {"service_level": "other"}])
def test_invalid_candidate_is_rejected(changes):
    with pytest.raises(ValueError):
        replace(CANDIDATES[0], **changes)


def test_duplicate_binding_is_rejected_across_service_levels():
    with pytest.raises(ValueError):
        weighted_candidate_order((CANDIDATES[0], replace(CANDIDATES[0], service_level="reduced")), SEED)


def test_rejection_sampling_discards_biased_tail(monkeypatch):
    calls = []

    def blocks(seed, level, priority, pick, counter):
        calls.append(counter)
        return b"\xff" * 32 if counter == 0 else b"\x00" * 32

    monkeypatch.setattr(routing, "_block", blocks)
    assert routing._draw(bytes.fromhex(SEED), "full", 0, 0, 3) == 0
    assert calls == [0, 1]


def test_weight_intervals_and_removal(monkeypatch):
    calls = []

    def draw(seed, level, priority, pick, bound):
        calls.append((pick, bound))
        return 1 if pick == 0 else 0

    monkeypatch.setattr(routing, "_draw", draw)
    assert weighted_candidate_order(CANDIDATES, SEED).full == ("binding-b", "binding-a", "binding-c")
    assert calls == [(0, 11), (1, 4), (2, 3)]
