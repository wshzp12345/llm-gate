"""Pre-commit recovery classification for the normalized synchronous text Port.

Refusals are typed results and never reach this classifier. Local faults and
cancellation propagate as exceptions; they are not Provider failure values.
This does not authorize degraded/cache selection or credential rotation retry.
"""

from llm_gateway.application.attempt_execution import FailureRecovery
from llm_gateway.domain.model import FailureCode, ProviderFailure


def classify_text_failure(failure: ProviderFailure) -> FailureRecovery:
    if not isinstance(failure, ProviderFailure) or not isinstance(failure.code, FailureCode) or type(failure.retryable) is not bool:
        raise TypeError("Normalized text Provider failure required")
    if failure.code in {FailureCode.INVALID_REQUEST, FailureCode.UNCERTAIN}:
        return FailureRecovery(False, False)
    if failure.code in {FailureCode.PROVIDER_CREDENTIALS_UNAVAILABLE, FailureCode.PROVIDER_PROTOCOL_ERROR}:
        return FailureRecovery(False, True)
    if failure.code in {FailureCode.RATE_LIMITED, FailureCode.UPSTREAM_TIMEOUT, FailureCode.PROVIDER_UNAVAILABLE}:
        return FailureRecovery(failure.retryable, True, failure.retry_after_ms if failure.retryable else None)
    raise ValueError("Unsupported text failure classification")
