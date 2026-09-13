from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace

import pytest

from llm_gateway.application.api_rate import ApiRate
from tests.test_invocation import authorization


TOKEN = 1_000_000_000


def test_tenant_denial_does_not_spend_instance_credit_and_subjects_share_tenant():
    rate = ApiRate(clock=lambda: 0)
    for n in range(50):
        assert rate.try_consume(authorization(subject=f"s{n}"))
    assert not rate.try_consume(authorization(subject="new"))
    assert rate._instance.credit == 50 * TOKEN
    for _ in range(50):
        assert rate.try_consume(authorization(tenant_id="other"))
    assert not rate.try_consume(authorization(tenant_id="third"))
    assert "third" not in rate._tenants


def test_instance_denial_does_not_spend_tenant_credit():
    now = [0]
    rate = ApiRate(clock=lambda: now[0])
    for n in range(100):
        assert rate.try_consume(authorization(tenant_id=f"t{n}"))
    assert not rate.try_consume(authorization(tenant_id="t0"))
    assert rate._tenants["t0"].credit == 49 * TOKEN
    now[0] = 9_999_999
    assert not rate.try_consume(authorization(tenant_id="fresh"))
    now[0] += 1
    assert rate.try_consume(authorization(tenant_id="fresh"))
    assert not rate.try_consume(authorization(tenant_id="fresh"))


def test_exact_tenant_refill_boundary_and_idle_cleanup_preserve_credit():
    now = [0]
    rate = ApiRate(clock=lambda: now[0])
    context = authorization()
    for _ in range(50):
        assert rate.try_consume(context)
    now[0] = 19_999_999
    assert not rate.try_consume(context)
    now[0] += 1
    assert rate.try_consume(context)
    assert not rate.try_consume(context)
    now[0] += TOKEN
    assert rate.try_consume(authorization(tenant_id="other"))
    assert "tenant" not in rate._tenants
    for _ in range(50):
        assert rate.try_consume(context)
    assert not rate.try_consume(context)


def test_atomic_thread_contention_cannot_overspend_either_scope():
    rate = ApiRate(clock=lambda: 0)
    contexts = [authorization(tenant_id=f"tenant-{n % 3}") for n in range(300)]
    with ThreadPoolExecutor(max_workers=12) as workers:
        assert sum(workers.map(rate.try_consume, contexts)) == 100
    assert rate._instance.credit == 0
    assert all(bucket.credit >= 0 for bucket in rate._tenants.values())


def test_unbounded_rejected_tenant_ids_create_no_bucket_entries():
    rate = ApiRate(clock=lambda: 0)
    context = authorization()
    for n in range(100):
        assert rate.try_consume(replace(context, tenant_id=f"t{n}"))
    for n in range(1000):
        assert not rate.try_consume(replace(context, tenant_id=f"rejected-{n}"))
    assert len(rate._tenants) == 100


@pytest.mark.parametrize("invalid", [-1, True, 1.5])
def test_invalid_clock_creates_no_state(invalid):
    rate = ApiRate(clock=lambda: invalid)
    with pytest.raises(ValueError):
        rate.try_consume(authorization())
    assert rate._instance is None and not rate._tenants


def test_clock_regression_and_untrusted_context_do_not_mutate_counters():
    now = [10]
    rate = ApiRate(clock=lambda: now[0])
    assert rate.try_consume(authorization())
    previous = rate._instance
    now[0] = 9
    with pytest.raises(ValueError):
        rate.try_consume(authorization())
    with pytest.raises(ValueError):
        rate.try_consume("caller-tenant")
    assert rate._instance is previous
    assert rate._instance.credit == 99 * TOKEN
