import asyncio
from dataclasses import replace

import pytest

from llm_gateway.application.configuration_validation import ConfigurationDeploymentPolicy, LocalConfigurationValidator
from tests.test_configuration_preparation import CONTRACTS, draft
from tests.config_fixtures import bundle_submission


POLICY = ConfigurationDeploymentPolicy(
    True, {"max_request_bytes": 1000000, "max_messages": 100, "max_content_item_bytes": 100000,
           "max_output_schema_bytes": 100000, "max_sse_event_bytes": 100000, "max_invocation_seconds": 300},
    7776000, 86400, 1000000, frozenset({"test-provider-reference"}), frozenset({"USD"}), CONTRACTS,
)


class NoTrustBundleExpected:
    async def valid(self, identity, pem):
        raise AssertionError("System TLS must not call certificate validator")


def validate(body, policy=POLICY):
    return asyncio.run(LocalConfigurationValidator(policy, NoTrustBundleExpected()).validate(draft(body)))


def test_system_trust_configuration_passes_local_checks_without_network():
    assert validate(bundle_submission()) == ()


@pytest.mark.parametrize("url", [
    "http://provider.invalid/v1", "https://user:pass@provider.invalid/v1",
    "https://other.invalid/v1", "https://provider.invalid/v1/", "https://provider.invalid/v1?",
    "https://provider.invalid/v1#", "https://provider.invalid:99999/v1", "https://provider.invalid\\evil/v1",
])
def test_endpoint_constraints(url):
    body = bundle_submission()
    body["bundle"]["providers"]["provider-a"]["endpoint"]["base_url"] = url
    assert validate(body)[0].path.endswith("endpoint/base_url")


def test_dev_http_still_requires_explicit_hostname_membership():
    body = bundle_submission()
    body["bundle"]["providers"]["provider-a"]["endpoint"]["base_url"] = "http://provider.invalid/v1"
    assert validate(body, replace(POLICY, production=False)) == ()
    body["bundle"]["providers"]["provider-a"]["egress"]["allowed_hosts"] = ["other.invalid"]
    assert validate(body, replace(POLICY, production=False))


@pytest.mark.parametrize("network", ["127.0.0.1/8", "10.0.0.0/08", "PUBLIC", "anything"])
def test_noncanonical_networks_rejected(network):
    body = bundle_submission()
    body["bundle"]["providers"]["provider-a"]["egress"]["allowed_networks"] = [network]
    assert validate(body)[0].path.endswith("egress/allowed_networks")


def test_unknown_secret_identity_rejected_without_material_read():
    errors = validate(bundle_submission(), replace(POLICY, known_secret_references=frozenset()))
    assert errors[0].reason == "secret_reference_invalid"
    assert "test-provider-reference" not in repr(errors)


@pytest.mark.parametrize("currency,timestamp", [("ZZZ", "2026-09-07T00:00:00Z"), ("USD", "2026-02-30T00:00:00Z"), ("USD", "2026-09-07T00:00:00+08:00")])
def test_price_metadata_validation(currency, timestamp):
    body = bundle_submission()
    body["bundle"]["pricing_tables"]["price-a"].update(currency=currency, effective_from=timestamp)
    assert validate(body)[0].reason == "pricing_invalid"


def test_resource_ceiling_is_external_and_not_copied_into_snapshot():
    body = bundle_submission()
    assert validate(body) == ()
    body["bundle"]["resource_policies"]["limits"]["max_messages"] = 101
    assert validate(body)[0].reason == "resource_policy_invalid"
    assert draft().snapshot_digest == draft(bundle_submission()).snapshot_digest


def test_invalid_trust_bundle_fails_closed():
    class RejectBundle:
        async def valid(self, identity, pem):
            return False
    body = bundle_submission()
    body["bundle"]["providers"]["provider-a"]["transport"]["tls"] = {
        "trust": "bundle", "trust_bundle": {"identity": "sha256:" + "0" * 64, "pem": "untrusted-content"},
    }
    errors = asyncio.run(LocalConfigurationValidator(POLICY, RejectBundle()).validate(draft(body)))
    assert errors[0].reason == "trust_bundle_invalid"
