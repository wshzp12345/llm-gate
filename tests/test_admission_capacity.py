import copy
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack

import pytest

from llm_gateway.application.admission_capacity import AdmissionCapacity
from llm_gateway.application.model_api import ModelInvocationRejected
from tests.test_invocation import authorization


def test_tenant_limit_spans_subjects_and_instance_limit_spans_tenants():
    capacity = AdmissionCapacity()
    with ExitStack() as leases:
        for tenant in range(4):
            for subject in range(50):
                leases.enter_context(capacity.acquire(authorization(tenant_id=f"t{tenant}", subject=f"s{subject}")))
            with pytest.raises(ModelInvocationRejected) as error:
                capacity.acquire(authorization(tenant_id=f"t{tenant}", subject="new-subject"))
            assert error.value.code == "rate_limited"
        assert capacity.in_use == 200
        with pytest.raises(ModelInvocationRejected):
            capacity.acquire(authorization(tenant_id="new-tenant"))
        assert "new-tenant" not in capacity._tenants
    assert capacity.in_use == 0 and not capacity._tenants


def test_concurrent_tenant_acquisition_is_atomic_with_no_partial_permits():
    capacity = AdmissionCapacity()
    context = authorization()
    def acquire(_):
        try:
            return capacity.acquire(context)
        except ModelInvocationRejected:
            return None
    with ThreadPoolExecutor(max_workers=12) as workers:
        leases = list(workers.map(acquire, range(120)))
    assert sum(lease is not None for lease in leases) == 50
    assert capacity.in_use == 50
    for lease in leases:
        if lease is not None:
            lease.release()
    assert capacity.in_use == 0 and not capacity._tenants


def test_identity_checked_release_non_reentry_and_drain_preserve_active_ownership():
    capacity = AdmissionCapacity()
    lease = capacity.acquire(authorization())
    copy.copy(lease).release()
    assert capacity.in_use == 1
    capacity.stop_admission()
    capacity.stop_admission()
    with pytest.raises(ModelInvocationRejected) as error:
        capacity.acquire(authorization())
    assert error.value.code == "gateway_not_ready"
    with lease:
        assert capacity.in_use == 1
        with pytest.raises(RuntimeError):
            with lease:
                pass
    lease.release()
    with pytest.raises(RuntimeError):
        with lease:
            pass
    assert capacity.in_use == 0 and not capacity._tenants


def test_untrusted_tenant_string_cannot_be_used_as_authorization():
    capacity = AdmissionCapacity()
    with pytest.raises(ValueError):
        capacity.acquire("caller-tenant")
    assert capacity.in_use == 0
