"""Bounded strict UTF-8 JSON parsing before Pydantic structural validation."""

import json
from dataclasses import dataclass

from pydantic import ValidationError

from llm_gateway.adapters.configuration_dto import BundleSubmission, ChangeSetSubmission, SUBMISSION_DTO
from llm_gateway.domain.configuration import ConfigurationDiagnostic


@dataclass(frozen=True)
class ParseLimits:
    max_bytes: int
    max_depth: int
    max_nodes: int
    max_collection_items: int
    max_string_bytes: int

    def __post_init__(self):
        if any(type(value) is not int or value < 1 for value in self.__dict__.values()):
            raise ValueError("Positive parser ceilings required")


class ConfigurationStructureError(Exception):
    def __init__(self):
        self.diagnostics = (ConfigurationDiagnostic("invalid_structure", None),)
        super().__init__("Invalid configuration structure")


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("Duplicate member")
        result[key] = value
    return result


def _reject_constant(value):
    raise ValueError("Non-finite number")


def parse_strict_json(body: bytes, limits: ParseLimits) -> object:
    try:
        if len(body) > limits.max_bytes or body.startswith(b"\xef\xbb\xbf"):
            raise ValueError("Invalid input size or BOM")
        text = body.decode("utf-8")
        # Reject excessive container nesting before recursive JSON construction.
        depth, quoted, escaped = 0, False, False
        for character in text:
            if quoted:
                if escaped:
                    escaped = False
                elif character == "\\":
                    escaped = True
                elif character == '"':
                    quoted = False
            elif character == '"':
                quoted = True
            elif character in "[{":
                depth += 1
                if depth > limits.max_depth:
                    raise ValueError("Nesting ceiling exceeded")
            elif character in "]}":
                depth -= 1
        value = json.loads(text, object_pairs_hook=_unique_object, parse_constant=_reject_constant)
        pending, nodes = [value], 0
        while pending:
            item = pending.pop()
            nodes += 1
            if nodes > limits.max_nodes:
                raise ValueError("Node ceiling exceeded")
            if isinstance(item, (dict, list)):
                if len(item) > limits.max_collection_items:
                    raise ValueError("Collection ceiling exceeded")
                pending.extend(item.values() if isinstance(item, dict) else item)
                if isinstance(item, dict):
                    pending.extend(item.keys())
            elif isinstance(item, str):
                if len(item.encode("utf-8")) > limits.max_string_bytes:
                    raise ValueError("String ceiling exceeded")
        return value
    except (ValueError, TypeError, RecursionError, ValidationError):
        raise ConfigurationStructureError() from None


def parse_configuration_json(body: bytes, limits: ParseLimits) -> BundleSubmission | ChangeSetSubmission:
    value = parse_strict_json(body, limits)
    try:
        return SUBMISSION_DTO.validate_python(value)
    except ValidationError:
        raise ConfigurationStructureError() from None


def parse_bundle_json(body: bytes, limits: ParseLimits) -> BundleSubmission:
    result = parse_configuration_json(body, limits)
    if not isinstance(result, BundleSubmission):
        raise ConfigurationStructureError()
    return result
