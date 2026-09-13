"""Failures before any Provider Attempt; never a fabricated Provider outcome."""

from dataclasses import dataclass


@dataclass(frozen=True)
class UnattemptedRoutingFailure:
    code: str

    def __post_init__(self):
        if self.code not in {"no_route", "rate_limited", "provider_credentials_unavailable", "unsupported_capability", "provider_unavailable"}:
            raise ValueError("Unsupported pre-Attempt routing failure")


def classify_unattempted_rejections(*, static: tuple[frozenset[str], ...],
                                    runtime: tuple[frozenset[str], ...]) -> UnattemptedRoutingFailure:
    # These are the currently implemented gate families. Other families require
    # their specific failure policy, not a guessed generic result here.
    static_codes = {"weight_zero", "service_level_disabled", "provider_disabled", "binding_disabled",
                    "capability_mismatch", "limit_exceeded", "parameter_unsupported", "provider_override_mismatch"}
    unavailable_codes = {"circuit_open", "circuit_half_open_busy", "health_unavailable",
                         "egress_policy_violation", "trust_bundle_expired", "security_invalidated"}
    runtime_codes = {"concurrency_exhausted", "qps_exhausted", "provider_credentials_unavailable"} | unavailable_codes
    for groups, allowed in ((static, static_codes | runtime_codes), (runtime, runtime_codes)):
        if not isinstance(groups, tuple) or any(not isinstance(group, frozenset) or not group or not group <= allowed for group in groups):
            raise ValueError("Incomplete or unsupported routing rejection facts")
    # Dynamic eligibility can reject a Candidate before its initial order exists.
    # Its terminal classification must not depend on when the same gate ran.
    runtime = runtime + tuple(group for group in static if group <= runtime_codes)
    static = tuple(group for group in static if not group <= runtime_codes)
    if runtime:
        reasons = frozenset().union(*runtime)
        if reasons <= {"concurrency_exhausted", "qps_exhausted"}:
            return UnattemptedRoutingFailure("rate_limited")
        if reasons == {"provider_credentials_unavailable"}:
            return UnattemptedRoutingFailure("provider_credentials_unavailable")
        if reasons <= unavailable_codes:
            return UnattemptedRoutingFailure("provider_unavailable")
    elif static and frozenset().union(*static) <= {"capability_mismatch", "limit_exceeded", "parameter_unsupported"}:
        return UnattemptedRoutingFailure("unsupported_capability")
    return UnattemptedRoutingFailure("no_route")
