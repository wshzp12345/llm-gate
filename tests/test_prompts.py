from dataclasses import FrozenInstanceError
from uuid import uuid4

import pytest

from llm_gateway.domain.model import Message
from llm_gateway.domain.prompts import MAX_MESSAGE_BYTES, PromptInvalid, PromptVersion


def version(text="Hello {{name}}", **changes):
    return PromptVersion(**({"tenant_id": "tenant-a", "asset_id": uuid4(),
                            "version_id": uuid4(), "messages": (Message("user", text),)} | changes))


def test_render_preserves_roles_and_pins_version_without_mutating_template():
    original = version(messages=(Message("system", "Be helpful"), Message("user", "你好 {{name}} {{name}}")))
    values = {"name": "世界"}
    result = original.render(values)
    values["name"] = "changed"
    assert result.messages == (Message("system", "Be helpful"), Message("user", "你好 世界 世界"))
    assert result.version_id == original.version_id
    assert result.asset_id == original.asset_id
    assert result.tenant_id == "tenant-a"
    assert original.messages[1].text == "你好 {{name}} {{name}}"
    with pytest.raises(FrozenInstanceError):
        original.messages = ()


def test_variables_are_literal_not_recursively_evaluated():
    value = '{{other}} \\1 ${HOME} <script>example</script>'
    assert version().render({"name": value}).messages[0].text == "Hello " + value
    assert version('{"key": "value"}').render({}).messages[0].text == '{"key": "value"}'


@pytest.mark.parametrize("text", ["{{x.y}}", "{{x()}}", "{{ x }}", "{{x", "x}}", "{{}}", "{{x[0]}}"])
def test_expressions_and_malformed_placeholders_rejected(text):
    with pytest.raises(PromptInvalid, match="^invalid_template$"):
        version(text)


@pytest.mark.parametrize("values", [{}, {"name": "ok", "extra": "no"}, {"name": 1}, {"name": "\ud800"}, {1: "bad"}, []])
def test_variable_contract(values):
    with pytest.raises(PromptInvalid, match="^invalid_variables$"):
        version().render(values)


def test_utf8_limits_and_expansion_are_bounded():
    with pytest.raises(PromptInvalid, match="prompt_too_large"):
        version("你" * (MAX_MESSAGE_BYTES // 3 + 1))
    with pytest.raises(PromptInvalid, match="prompt_too_large"):
        version("{{name}}" * 100).render({"name": "a" * 1000})
    assert len(version("{{name}}").render({"name": "a" * MAX_MESSAGE_BYTES}).messages[0].text) == MAX_MESSAGE_BYTES


def test_sensitive_content_not_in_repr_or_errors():
    original = version("secret-template {{name}}")
    result = original.render({"name": "secret-variable"})
    assert "secret-template" not in repr(original)
    assert "secret-variable" not in repr(result)
    with pytest.raises(PromptInvalid) as error:
        original.render({"secret-variable": "secret-template"})
    assert str(error.value) == "invalid_variables"


def test_message_count_total_size_and_variable_count():
    for messages in [(), [Message("user", "x")], (Message("user", "x"),) * 101]:
        with pytest.raises(PromptInvalid):
            version(messages=messages)
    with pytest.raises(PromptInvalid, match="prompt_too_large"):
        version(messages=(Message("user", "a" * MAX_MESSAGE_BYTES),) * 5)
    with pytest.raises(PromptInvalid, match="prompt_too_large"):
        version(" ".join("{{v" + str(i) + "}}" for i in range(65)))
