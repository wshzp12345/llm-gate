import asyncio

import pytest

from llm_gateway.infrastructure.development_policy import development_validator, DEEPSEEK_SECRET_REFERENCE
from tests.config_fixtures import bundle_submission
from tests.test_configuration_preparation import draft


def local_bundle():
    body = bundle_submission()
    body["bundle"]["providers"]["provider-a"]["credential"]["secret_ref"] = DEEPSEEK_SECRET_REFERENCE
    return body


def test_development_policy_validates_without_provider_credentials(monkeypatch):
    monkeypatch.delenv("DEEPSEEK_API_KEY", raising=False)
    assert asyncio.run(development_validator().validate(draft(local_bundle()))) == ()


@pytest.mark.parametrize("capability,value", [("streaming", True), ("tool_calling", True), ("structured_output", "json_schema")])
def test_unimplemented_adapter_capabilities_cannot_be_published(capability, value):
    body = local_bundle()
    body["bundle"]["provider_model_bindings"]["binding-a"]["capabilities"][capability] = value
    assert asyncio.run(development_validator().validate(draft(body)))


def test_unknown_secret_reference_is_not_accepted():
    errors = asyncio.run(development_validator().validate(draft(bundle_submission())))
    assert any(item.reason == "secret_reference_invalid" for item in errors)


def test_dev_bypass_does_not_bypass_resource_ceilings():
    body = local_bundle()
    body["bundle"]["resource_policies"]["limits"] = {"max_messages": 101}
    errors = asyncio.run(development_validator().validate(draft(body)))
    assert any(item.reason == "resource_policy_invalid" for item in errors)
