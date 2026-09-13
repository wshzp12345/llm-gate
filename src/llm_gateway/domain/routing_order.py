"""Deterministic ordering of already hard-filtered routing candidates.

No admission, Provider I/O, dynamic eligibility, cost ceiling or persistence.
The seed is the recorded 32-byte routing seed, not a secret key or runtime RNG.
"""

import hashlib
import hmac
import re
from dataclasses import dataclass


_ORDER_DOMAIN = b"gateway.routing-seed/v1/order"


def _field(value: bytes) -> bytes:
    return len(value).to_bytes(4, "big") + value


def _block(seed: bytes, service_level: str, priority: int, pick: int, counter: int) -> bytes:
    message = _ORDER_DOMAIN + b"".join(_field(value) for value in (
        service_level.encode("ascii"), str(priority).encode("ascii"),
        str(pick).encode("ascii"), str(counter).encode("ascii"),
    ))
    return hmac.new(seed, message, hashlib.sha256).digest()


def _draw(seed: bytes, service_level: str, priority: int, pick: int, bound: int) -> int:
    space = 1 << 256
    if not 1 <= bound <= space:
        raise ValueError("Invalid sampling bound")
    cutoff = space - space % bound
    counter = 0
    while True:
        value = int.from_bytes(_block(seed, service_level, priority, pick, counter), "big")
        if value < cutoff:
            return value % bound
        counter += 1


@dataclass(frozen=True)
class WeightedCandidate:
    binding: str
    service_level: str
    priority: int
    weight: int

    def __post_init__(self):
        if not isinstance(self.binding, str) or len(self.binding) > 128 or not re.fullmatch(r"[a-z][a-z0-9]*(?:[._-][a-z0-9]+)*", self.binding):
            raise ValueError("Invalid Binding Resource ID")
        if self.service_level not in {"full", "reduced"}:
            raise ValueError("Invalid service level")
        if type(self.priority) is not int or not 0 <= self.priority <= 65535:
            raise ValueError("Invalid candidate priority")
        if type(self.weight) is not int or not 0 <= self.weight <= 10000:
            raise ValueError("Invalid candidate weight")


@dataclass(frozen=True)
class CandidateOrder:
    full: tuple[str, ...]
    reduced: tuple[str, ...]


def weighted_candidate_order(candidates: tuple[WeightedCandidate, ...], seed_hex: str) -> CandidateOrder:
    if not isinstance(seed_hex, str) or not re.fullmatch(r"[0-9a-f]{64}", seed_hex):
        raise ValueError("A stored canonical routing seed is required")
    if len({candidate.binding for candidate in candidates}) != len(candidates):
        raise ValueError("Duplicate candidate Binding")
    seed = bytes.fromhex(seed_hex)
    orders = {}
    for level in ("full", "reduced"):
        eligible = [candidate for candidate in candidates if candidate.service_level == level and candidate.weight > 0]
        ordered = []
        for priority in sorted({candidate.priority for candidate in eligible}):
            remaining = sorted((candidate for candidate in eligible if candidate.priority == priority),
                               key=lambda candidate: candidate.binding.encode("utf-8"))
            pick = 0
            while remaining:
                position = _draw(seed, level, priority, pick, sum(candidate.weight for candidate in remaining))
                cumulative = 0
                for index, candidate in enumerate(remaining):
                    cumulative += candidate.weight
                    if position < cumulative:
                        ordered.append(candidate.binding)
                        remaining.pop(index)
                        break
                pick += 1
        orders[level] = tuple(ordered)
    return CandidateOrder(orders["full"], orders["reduced"])
