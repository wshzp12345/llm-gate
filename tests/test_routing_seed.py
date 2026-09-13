import asyncio
from uuid import UUID

import pytest

from llm_gateway.adapters.fingerprint_material import FingerprintMaterial, FingerprintMaterialMetadata
from llm_gateway.application.fingerprint_keys import FingerprintKeys
from llm_gateway.domain.invocation import InvocationPersistenceUnavailable
from llm_gateway.domain.routing_seed import routing_seed_message
from tests.test_fingerprint_leases import RING, OLD, Protection, deadline


CALL = UUID("12345678-1234-4234-9234-123456789abc")
GOLDEN = "baa5364da88093b83a64f41f324fe1ef9683c0e9859d10ada585a3c494716e90"


def test_seed_golden_and_same_operation_material_without_another_read():
    async def run():
        calls, materials = [], []
        class Source:
            async def resolve(self, member):
                calls.append(member)
                material = FingerprintMaterial(member, FingerprintMaterialMetadata(
                    member.key_id, member.key_version, member.secret_ref), bytes(range(32)))
                materials.append(material)
                return material
        keys = FingerprintKeys(RING, Source(), Protection())
        assert await keys.validate_active()
        async with keys.active(deadline=deadline()) as lease:
            lease.digest(b"request-test-projection", purpose="request")
            lease.digest(b"execution-test-projection", purpose="execution")
            assert lease.routing_seed(CALL, "routing.primary", "9223372036854775807") == GOLDEN
            assert len(calls) == 2  # one startup probe and one execution, not per hash
            alternatives = [lease.routing_seed(CALL, "routing.secondary", "9223372036854775807"),
                            lease.routing_seed(CALL, "routing.primary", "1"),
                            lease.routing_seed(UUID("12345678-1234-4234-9234-123456789abd"),
                                               "routing.primary", "9223372036854775807")]
            assert len(set([GOLDEN, *alternatives])) == 4
        assert all(material._closed for material in materials)
        with pytest.raises(InvocationPersistenceUnavailable):
            lease.routing_seed(CALL, "routing.primary", "1")
        async with keys.historical(*OLD.identity, deadline=deadline()) as historical:
            with pytest.raises(InvocationPersistenceUnavailable):
                historical.routing_seed(CALL, "routing.primary", "1")
    asyncio.run(run())


def test_seed_encoding_has_independent_prefix_and_length_framing():
    expected = (b"gateway.routing-seed/v1" + (36).to_bytes(4, "big") + str(CALL).encode("ascii")
                + b"\x00\x00\x00\x02p1\x00\x00\x00\x0223")
    assert routing_seed_message(CALL, "p1", "23") == expected
    assert routing_seed_message(CALL, "p1", "23") != routing_seed_message(CALL, "p12", "3")


@pytest.mark.parametrize("call,policy,revision", [
    (str(CALL), "p", "1"), (UUID(int=0), "p", "1"),
    (CALL, "P", "1"), (CALL, "", "1"), (CALL, "a" * 129, "1"),
    (CALL, "p", "01"), (CALL, "p", "0"), (CALL, "p", 1),
    (CALL, "p", "9223372036854775808"),
])
def test_noncanonical_identity_is_rejected_before_hmac(call, policy, revision):
    with pytest.raises(ValueError):
        routing_seed_message(call, policy, revision)
