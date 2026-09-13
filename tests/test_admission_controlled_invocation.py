import asyncio
from contextlib import ExitStack
from datetime import datetime, timezone

import pytest

from llm_gateway.application.admission_capacity import AdmissionCapacity
from llm_gateway.application.api_rate import ApiRate
from llm_gateway.application.admission_controlled_invocation import AdmissionControlledInvocation
from llm_gateway.application.model_api import ModelInvocationRejected
from llm_gateway.domain.invocation import InvocationAuthorizationExpired, InvocationPersistenceUnavailable
from tests.test_invocation import authorization
from tests.test_model_http import request


@pytest.mark.parametrize("failure", [None, RuntimeError, InvocationPersistenceUnavailable, TimeoutError, asyncio.CancelledError])
def test_one_permit_covers_authorized_backend_until_return_or_failure(failure):
    async def run():
        capacity = AdmissionCapacity()
        context = authorization()
        async def authorize(query):
            assert capacity.in_use == 0
            return context
        class Backend:
            async def invoke_authorized(self, query, received):
                assert received is context
                lease = next(iter(capacity._leases))
                for _ in range(3):
                    await asyncio.sleep(0)
                    assert capacity.in_use == 1 and lease in capacity._leases
                if failure:
                    raise failure()
                return "committed-result"
        service = AdmissionControlledInvocation(capacity=capacity, api_rate=ApiRate(), backend=Backend(), authorize=authorize)
        if failure:
            with pytest.raises(failure):
                await service.invoke(None)
        else:
            assert await service.invoke(None) == "committed-result"
        assert capacity.in_use == 0 and not capacity._tenants
    asyncio.run(run())


def test_external_cancel_during_backend_releases_permit_while_drain_refuses_new_work():
    async def run():
        capacity, entered = AdmissionCapacity(), asyncio.Event()
        async def authorize(query):
            return authorization()
        class Backend:
            async def invoke_authorized(self, *args):
                entered.set()
                await asyncio.Future()
        service = AdmissionControlledInvocation(capacity=capacity, api_rate=ApiRate(), backend=Backend(), authorize=authorize)
        task = asyncio.create_task(service.invoke(None))
        await entered.wait()
        capacity.stop_admission()
        assert capacity.in_use == 1
        with pytest.raises(ModelInvocationRejected):
            await service.invoke(None)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert capacity.in_use == 0
    asyncio.run(run())


def test_real_deadline_cancellation_releases_admission_without_an_async_cleanup_gap():
    async def run():
        capacity = AdmissionCapacity()
        async def authorize(query):
            return authorization()
        class Backend:
            async def invoke_authorized(self, *args):
                assert capacity.in_use == 1
                await asyncio.Future()
        service = AdmissionControlledInvocation(capacity=capacity, api_rate=ApiRate(), backend=Backend(), authorize=authorize)
        with pytest.raises(TimeoutError):
            async with asyncio.timeout(0):
                await service.invoke(None)
        assert capacity.in_use == 0 and not capacity._tenants
    asyncio.run(run())


@pytest.mark.parametrize("kind", ["expired", "forbidden", "invalid", "full"])
def test_pre_admission_failure_never_invokes_backend(kind):
    async def run():
        capacity = AdmissionCapacity()
        context = authorization(expires_at=datetime(2000, 1, 1, tzinfo=timezone.utc)) if kind == "expired" else authorization()
        async def authorize(query):
            if kind == "forbidden":
                raise ModelInvocationRejected("forbidden")
            return "caller-tenant" if kind == "invalid" else context
        class Backend:
            async def invoke_authorized(self, *args):
                pytest.fail("Rejected access cannot create an Invocation")
        service = AdmissionControlledInvocation(capacity=capacity, api_rate=ApiRate(), backend=Backend(), authorize=authorize)
        with ExitStack() as leases:
            if kind == "full":
                for _ in range(50):
                    leases.enter_context(capacity.acquire(context))
            expected = InvocationAuthorizationExpired if kind == "expired" else TypeError if kind == "invalid" else ModelInvocationRejected
            with pytest.raises(expected):
                await service.invoke(None)
            assert capacity.in_use == (50 if kind == "full" else 0)
        assert capacity.in_use == 0
    asyncio.run(run())


def test_expired_authorization_has_safe_http_401_without_invocation_identity():
    async def authorize(query):
        return authorization(expires_at=datetime(2000, 1, 1, tzinfo=timezone.utc))
    service = AdmissionControlledInvocation(capacity=AdmissionCapacity(), api_rate=ApiRate(), backend=None, authorize=authorize)
    response = request(service)
    assert response.status_code == 401 and response.json()["error"]["code"] == "unauthorized"
    assert "X-Gateway-Call-Id" not in response.headers


def test_concurrency_precedes_api_rate_and_qps_rejection_releases_permits_without_backend_work():
    async def run():
        capacity, rate = AdmissionCapacity(), ApiRate(clock=lambda: 0)
        context = authorization()
        async def authorize(query):
            return context
        class Backend:
            async def invoke_authorized(self, *args):
                pytest.fail("Rejected admission must not start backend work")
        service = AdmissionControlledInvocation(capacity=capacity, api_rate=rate, backend=Backend(), authorize=authorize)
        with ExitStack() as leases:
            for _ in range(50):
                leases.enter_context(capacity.acquire(context))
            with pytest.raises(ModelInvocationRejected):
                await service.invoke(None)
            assert rate._instance is None
        for _ in range(50):
            assert rate.try_consume(context)
        with pytest.raises(ModelInvocationRejected) as error:
            await service.invoke(None)
        assert error.value.code == "rate_limited"
        assert rate._instance.credit == 50_000_000_000
        assert capacity.in_use == 0
    asyncio.run(run())


@pytest.mark.parametrize("failure", [RuntimeError, asyncio.CancelledError])
def test_admitted_access_spends_api_token_once_and_does_not_refund_backend_failure(failure):
    async def run():
        capacity, rate = AdmissionCapacity(), ApiRate(clock=lambda: 0)
        context = authorization()
        async def authorize(query):
            return context
        class Backend:
            async def invoke_authorized(self, *args):
                for _ in range(3):
                    await asyncio.sleep(0)
                    assert rate._instance.credit == 99_000_000_000
                raise failure()
        service = AdmissionControlledInvocation(capacity=capacity, api_rate=rate, backend=Backend(), authorize=authorize)
        with pytest.raises(failure):
            await service.invoke(None)
        assert rate._instance.credit == 99_000_000_000
        assert rate._tenants[context.tenant_id].credit == 49_000_000_000
        assert capacity.in_use == 0
    asyncio.run(run())
