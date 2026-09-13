"""Bounded, non-executable Prompt versions; no storage or Provider dependencies."""

from dataclasses import dataclass, field
import re
from uuid import UUID
from types import MappingProxyType
from collections.abc import Mapping

from llm_gateway.domain.model import Message


_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]{0,63}\Z")
_TOKEN = re.compile(r"\{\{([A-Za-z_][A-Za-z0-9_]{0,63})\}\}")
MAX_MESSAGES = 100
MAX_MESSAGE_BYTES = 64 * 1024
MAX_TOTAL_BYTES = 256 * 1024
MAX_VARIABLES = 64


class PromptInvalid(ValueError):
    """Only closed reason codes; never include template or variable content."""

    def __init__(self, code: str):
        if code not in {"invalid_identity", "invalid_template", "invalid_variables", "prompt_too_large"}:
            raise ValueError("Unregistered Prompt error")
        self.code = code
        super().__init__(code)


@dataclass(frozen=True)
class PromptReference:
    asset_id: UUID
    version_id: UUID

    def __post_init__(self):
        if type(self.asset_id) is not UUID or type(self.version_id) is not UUID:
            raise PromptInvalid("invalid_identity")


@dataclass(frozen=True)
class PromptSelection:
    reference: PromptReference
    variables: Mapping[str, str] = field(repr=False)

    def __post_init__(self):
        if type(self.reference) is not PromptReference or not isinstance(self.variables, Mapping):
            raise PromptInvalid("invalid_variables")
        if len(self.variables) > MAX_VARIABLES:
            raise PromptInvalid("prompt_too_large")
        total = 0
        for name, value in self.variables.items():
            if type(name) is not str or not _NAME.fullmatch(name):
                raise PromptInvalid("invalid_variables")
            size = _size(value)
            total += size
            if size > MAX_MESSAGE_BYTES or total > MAX_TOTAL_BYTES:
                raise PromptInvalid("prompt_too_large")
        object.__setattr__(self, "variables", MappingProxyType(dict(self.variables)))


def _size(value: str) -> int:
    if type(value) is not str or "\x00" in value:
        raise PromptInvalid("invalid_variables")
    try:
        return len(value.encode("utf-8"))
    except UnicodeEncodeError:
        raise PromptInvalid("invalid_variables") from None


def _check_messages(messages: tuple[Message, ...]) -> None:
    if type(messages) is not tuple or not messages or len(messages) > MAX_MESSAGES:
        raise PromptInvalid("invalid_template")
    total = 0
    for message in messages:
        if type(message) is not Message:
            raise PromptInvalid("invalid_template")
        size = _size(message.text)
        total += size
        if size > MAX_MESSAGE_BYTES or total > MAX_TOTAL_BYTES:
            raise PromptInvalid("prompt_too_large")


@dataclass(frozen=True)
class PromptVersion:
    tenant_id: str
    asset_id: UUID
    version_id: UUID
    messages: tuple[Message, ...] = field(repr=False)
    variables: tuple[str, ...] = field(init=False, repr=False)

    def __post_init__(self):
        if (type(self.tenant_id) is not str or not self.tenant_id or len(self.tenant_id) > 256
                or type(self.asset_id) is not UUID or type(self.version_id) is not UUID):
            raise PromptInvalid("invalid_identity")
        _check_messages(self.messages)
        names = set()
        for message in self.messages:
            # Single braces (e.g. JSON) remain literal. Every double brace must
            # belong to a plain identifier placeholder; no expression evaluator.
            remainder = _TOKEN.sub("", message.text)
            if "{{" in remainder or "}}" in remainder:
                raise PromptInvalid("invalid_template")
            names.update(_TOKEN.findall(message.text))
        if len(names) > MAX_VARIABLES:
            raise PromptInvalid("prompt_too_large")
        object.__setattr__(self, "variables", tuple(sorted(names)))

    def render(self, values: dict[str, str]) -> "RenderedPrompt":
        if type(values) is not dict or len(values) > MAX_VARIABLES:
            raise PromptInvalid("invalid_variables")
        if any(type(name) is not str or not _NAME.fullmatch(name) for name in values):
            raise PromptInvalid("invalid_variables")
        if set(values) != set(self.variables):
            raise PromptInvalid("invalid_variables")
        total = 0
        for value in values.values():
            size = _size(value)
            total += size
            if size > MAX_MESSAGE_BYTES or total > MAX_TOTAL_BYTES:
                raise PromptInvalid("prompt_too_large")
        rendered = []
        total = 0
        for message in self.messages:
            # Check expanded size before allocating it (repeated variables can
            # otherwise turn a small valid template into an enormous string).
            size = _size(message.text)
            for match in _TOKEN.finditer(message.text):
                size += _size(values[match[1]]) - len(match[0])
            total += size
            if size > MAX_MESSAGE_BYTES or total > MAX_TOTAL_BYTES:
                raise PromptInvalid("prompt_too_large")
            rendered.append(Message(message.role, _TOKEN.sub(lambda m: values[m[1]], message.text)))
        return RenderedPrompt(self.tenant_id, self.asset_id, self.version_id, tuple(rendered))


@dataclass(frozen=True)
class RenderedPrompt:
    """Pinned provenance plus immutable messages; sensitive content is not repr'd."""

    tenant_id: str
    asset_id: UUID
    version_id: UUID
    messages: tuple[Message, ...] = field(repr=False)
