import asyncio
from datetime import datetime, timedelta, timezone
from email.utils import format_datetime

import httpx
import pytest

from llm_gateway.adapters.retry_after import provider_retry_after_ms
from llm_gateway.application.text_failure_recovery import classify_text_failure
from llm_gateway.domain.model import FailureCode, ProviderFailure
from tests.test_completion import invoke
from tests.test_attempt_execution import Harness, SUCCESS, CANDIDATES
from llm_gateway.domain.recovery import RetryPolicy


NOW = datetime(2026, 9, 11, tzinfo=timezone.utc)


@pytest.mark.parametrize("value,expected", [("0", 0), (" 5\t", 5000), ("6", 6000),
    ("999999999999999999999", 999999999999999999999000), ("-1", None), ("1.5", None),
    ("", None), ("+2", None), ("５", None), ("bogus", None), ("1, 2", None)])
def test_retry_after_delta_is_bounded_in_parsing_but_never_clamped_to_retry_budget(value, expected):
    assert provider_retry_after_ms(httpx.Headers({"retry-after": value.encode("utf-8")}), now=lambda: NOW) == expected


def test_http_date_duration_rounds_up_and_duplicate_fields_are_not_trusted():
    target = format_datetime(NOW + timedelta(seconds=4), usegmt=True)
    assert provider_retry_after_ms(httpx.Headers({"retry-after": target}), now=lambda: NOW + timedelta(microseconds=1)) == 4000
    assert provider_retry_after_ms(httpx.Headers({"retry-after": target}), now=lambda: NOW + timedelta(seconds=5)) == 0
    assert provider_retry_after_ms(httpx.Headers([("retry-after", "1"), ("retry-after", "2")]), now=lambda: NOW) is None


@pytest.mark.parametrize("status,retry,fallback,hint", [(400, False, False, None),
    (401, False, True, None), (403, False, True, None), (302, False, True, None),
    (429, True, True, 3000), (408, True, True, 3000), (503, True, True, 3000), (501, False, True, None)])
def test_adapter_and_production_classifier_agree_without_reading_error_body(status, retry, fallback, hint):
    result = invoke(lambda request: httpx.Response(status, content=b"private provider failure",
        headers={"retry-after": "3"}))
    decision = classify_text_failure(result)
    assert (decision.retryable, decision.failover_eligible, decision.retry_after_ms) == (retry, fallback, hint)
    assert "private" not in repr(result)


@pytest.mark.parametrize("value,expected_binding,expected_sleep", [("3", "a", 3), ("6", "b", None)])
def test_real_executor_honors_short_wait_and_skips_long_wait_without_early_retry(value, expected_binding, expected_sleep):
    failure = invoke(lambda request: httpx.Response(429, headers={"retry-after": value}))
    harness = Harness([failure, SUCCESS])
    async def run():
        executor = harness.executor()
        executor._classify = classify_text_failure
        return await executor.execute(CANDIDATES, policy=RetryPolicy(), deadline=asyncio.get_running_loop().time() + 10)
    assert asyncio.run(run()) is SUCCESS
    starts = [event for event in harness.events if event[0] == "start"]
    assert starts[1][2] == expected_binding
    sleeps = [event[1] for event in harness.events if event[0] == "sleep"]
    assert sleeps == ([] if expected_sleep is None else [expected_sleep])


def test_nonretryable_and_invalid_failures_cannot_gain_retry_from_a_header():
    value = ProviderFailure(FailureCode.PROVIDER_CREDENTIALS_UNAVAILABLE, True, 1000)
    decision = classify_text_failure(value)
    assert not decision.retryable and decision.retry_after_ms is None
    with pytest.raises(TypeError):
        classify_text_failure(ProviderFailure("unknown", True))
    with pytest.raises(TypeError):
        classify_text_failure(SUCCESS)


@pytest.mark.parametrize("invalid", [-1, True, 1.5])
def test_retry_after_duration_is_typed(invalid):
    with pytest.raises(ValueError):
        ProviderFailure(FailureCode.RATE_LIMITED, True, invalid)
