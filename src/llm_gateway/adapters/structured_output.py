"""Bounded outer-boundary parsing and local validation for JSON output modes."""

import json
import re
from contextlib import aclosing
from dataclasses import replace
from decimal import Decimal

from jsonschema import Draft202012Validator, SchemaError, ValidationError

from llm_gateway.domain.model import FailureCode, OutputFormat, ProviderFailure, ProviderResult, TextOutput
from llm_gateway.domain.streaming import DeltaKind, StreamCompleted, StreamDelta, StreamFailed


_NAME = re.compile(r"^[A-Za-z][A-Za-z0-9_-]{0,63}$")
_DIALECT = "https://json-schema.org/draft/2020-12/schema"
# This is deliberately narrower than the frozen portable profile. A feature
# which cannot yet be translated exactly is rejected before Provider I/O.
_KEYWORDS = frozenset({"$schema", "$defs", "$ref", "type", "enum", "const", "properties", "required",
    "additionalProperties", "items", "title", "description"})
_ANTHROPIC_FEATURES = frozenset({"array-items", "atomic-enum", "const", "local-ref", "object-closed", "required"})


def adapter_schema_features(adapter_type: str, adapter_version: str) -> frozenset[str]:
    # Unknown contracts must not inherit another Adapter's feature claims.
    return _ANTHROPIC_FEATURES if (adapter_type, adapter_version) == ("anthropic_messages", "v1") else frozenset()


class InvalidOutputFormat(ValueError):
    pass


class DuplicateKey(ValueError):
    pass


class NonFiniteNumber(ValueError):
    pass


class StructuredResourceLimit(ValueError):
    pass


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise DuplicateKey()
        result[key] = value
    return result


def _nonfinite(value):
    raise NonFiniteNumber()


def _bounded_number(token, *, integer=False):
    if len(token) > 256:
        raise StructuredResourceLimit()
    exponent = re.search(r"[eE]([+-]?\d+)$", token)
    if exponent is not None and abs(int(exponent.group(1))) > 1000:
        raise StructuredResourceLimit()
    return int(token) if integer else Decimal(token)


def _extract_one(text: str) -> tuple[str | None, str | None]:
    """Select one complete JSON value without altering its internal bytes."""
    if text.startswith("\ufeff"):
        text = text[1:]
    stripped = text.strip()
    fence = re.fullmatch(r"```(?:json)?[ \t]*\r?\n(.*?)\r?\n```", stripped, re.DOTALL | re.IGNORECASE)
    if fence:
        stripped = fence.group(1).strip()
    # Candidate framing must not hide a duplicate-key/non-finite validation
    # failure; the definitive strict parser below classifies those values.
    decoder = json.JSONDecoder(parse_float=Decimal)
    if stripped.startswith(("{", "[")):
        try:
            decoder.raw_decode(stripped)
        except (ValueError, RecursionError):
            return stripped, None  # A broken root is not surrounding prose.
    found = None
    index = 0
    attempts = 0
    while index < len(stripped):
        if stripped[index] not in '{["-0123456789tfnNI':
            index += 1
            continue
        if index and (stripped[index - 1].isalnum() or stripped[index - 1] == "_"):
            index += 1
            continue
        attempts += 1
        if attempts > 8192:
            return None, "structured_resource_limit"
        try:
            _, end = decoder.raw_decode(stripped, index)
        except (ValueError, RecursionError):
            index += 1
            continue
        if end < len(stripped) and (stripped[end].isalnum() or stripped[end] == "_"):
            index += 1
            continue
        if found is not None:
            return None, "json_ambiguous"
        found = stripped[index:end]
        index = end  # Nested values belong to this candidate, not a second one.
    return (found, None) if found is not None else (None, "json_missing")


def _schema_structure(value, depth=1):
    """Check decoded scalar validity and JSON nesting, including literal data."""
    if depth > 64:
        raise InvalidOutputFormat()
    if isinstance(value, dict):
        for key, child in value.items():
            key.encode("utf-8")
            _schema_structure(child, depth + 1)
    elif isinstance(value, list):
        for child in value:
            _schema_structure(child, depth + 1)
    elif isinstance(value, str):
        value.encode("utf-8")


def _schema_node(node, *, root, visited, stack, depth=0, ref_depth=0):
    if not isinstance(node, dict) or depth > 64 or ref_depth > 32:
        raise InvalidOutputFormat()
    identity = id(node)
    if identity in stack:
        raise InvalidOutputFormat()
    if identity in visited:
        return
    if len(visited) >= 4096:
        raise InvalidOutputFormat()
    stack.add(identity)
    if set(node) - _KEYWORDS or node.get("$schema", _DIALECT) != _DIALECT:
        raise InvalidOutputFormat()
    if isinstance(node.get("type"), list):
        raise InvalidOutputFormat()
    if isinstance(node.get("additionalProperties"), dict):
        raise InvalidOutputFormat()
    if "enum" in node and any(isinstance(value, (dict, list)) for value in node["enum"]):
        raise InvalidOutputFormat()
    properties = node.get("properties", {})
    required = node.get("required", [])
    if (len(properties) > 256 or len(required) > 256 or len(required) != len(set(required))
            or any(name not in properties for name in required)):
        raise InvalidOutputFormat()
    if "$ref" in node:
        ref = node["$ref"]
        if not isinstance(ref, str) or not (ref == "#" or ref.startswith("#/")):
            raise InvalidOutputFormat()
        target = root
        for part in ref[2:].split("/") if ref != "#" else ():
            if re.search(r"~(?![01])", part):
                raise InvalidOutputFormat()
            part = part.replace("~1", "/").replace("~0", "~")
            if not isinstance(target, dict) or part not in target:
                raise InvalidOutputFormat()
            target = target[part]
        _schema_node(target, root=root, visited=visited, stack=stack, depth=depth + 1,
                     ref_depth=ref_depth + 1)
    for key in ("$defs", "properties"):
        for child in node.get(key, {}).values():
            _schema_node(child, root=root, visited=visited, stack=stack, depth=depth + 1,
                         ref_depth=ref_depth)
    for key in ("items", "additionalProperties"):
        child = node.get(key)
        if isinstance(child, dict):
            _schema_node(child, root=root, visited=visited, stack=stack, depth=depth + 1,
                         ref_depth=ref_depth)
    stack.remove(identity)
    visited.add(identity)


def parse_output_format(value: object) -> OutputFormat | None:
    """Validate the closed Chat Completions shape before admission or Provider I/O."""
    if not isinstance(value, dict):
        raise InvalidOutputFormat()
    kind = value.get("type")
    if not isinstance(kind, str):
        raise InvalidOutputFormat()
    if kind in {"text", "json_object"} and set(value) == {"type"}:
        return None if kind == "text" else OutputFormat("json_object")
    if kind != "json_schema" or set(value) != {"type", "json_schema"}:
        raise InvalidOutputFormat()
    description = value["json_schema"]
    if (not isinstance(description, dict) or set(description) not in (
            {"name", "schema"}, {"name", "schema", "strict"})
            or description.get("strict", True) is not True or not isinstance(description.get("name"), str)
            or not _NAME.fullmatch(description["name"])):
        raise InvalidOutputFormat()
    schema = description.get("schema")
    if not isinstance(schema, dict):
        raise InvalidOutputFormat()
    try:
        _schema_structure(schema)
        encoded = json.dumps(schema, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)
        if len(encoded.encode("utf-8")) > 65536:
            raise InvalidOutputFormat()
        Draft202012Validator.check_schema(schema)
        _schema_node(schema, root=schema, visited=set(), stack=set())
    except (TypeError, ValueError, UnicodeError, RecursionError, SchemaError) as error:
        raise InvalidOutputFormat() from error
    return OutputFormat("json_schema", description["name"], encoded)


def schema_feature_ids(schema_json: str) -> tuple[str, ...]:
    """Project only registered routing features; literal enum/const data is opaque."""
    root = json.loads(schema_json)
    features = set()
    pending = [root]
    while pending:
        node = pending.pop()
        if not (set(node) - {"$schema", "$defs", "title", "description"}):
            features.add("unconstrained")
        if "$ref" in node:
            features.add("local-ref")
        if "enum" in node:
            features.add("atomic-enum")
        if "const" in node:
            features.add("const")
        if "required" in node:
            features.add("required")
        if node.get("type") == "object" or any(key in node for key in ("properties", "required", "additionalProperties")):
            features.add("object-closed" if node.get("additionalProperties") is False else "object-open")
        if node.get("type") == "array" or "items" in node:
            features.add("array-items" if "items" in node else "array-unconstrained")
        for key in ("$defs", "properties"):
            pending.extend(node.get(key, {}).values())
        if isinstance(node.get("items"), dict):
            pending.append(node["items"])
    return tuple(sorted(features))


def validate_structured_result(result: ProviderResult | ProviderFailure, output_format: OutputFormat | None):
    """Never treat Provider JSON-mode claims as a Gateway validation result."""
    if output_format is None or isinstance(result, ProviderFailure) or not isinstance(result.output, TextOutput):
        return result

    def invalid(reason, path=None):
        return ProviderFailure(FailureCode.STRUCTURED_OUTPUT_INVALID, False,
            observed_usage=result.usage, observed_model=result.resolved_model,
            structured_reason=reason, structured_path=path)

    if result.finish_reason == "length":
        return invalid("output_truncated")
    content = result.output.text
    if not content:
        return invalid("json_missing")
    try:
        size = len(content.encode("utf-8"))
    except UnicodeError:
        return invalid("json_invalid_unicode")
    if size > 4 * 1024 * 1024:
        return invalid("structured_resource_limit")
    if output_format.local_extraction:
        content, reason = _extract_one(content)
        if reason is not None:
            return invalid(reason)
    try:
        instance = json.loads(content, object_pairs_hook=_unique_object,
                              parse_int=lambda token: _bounded_number(token, integer=True),
                              parse_float=_bounded_number, parse_constant=_nonfinite)
    except DuplicateKey:
        return invalid("json_duplicate_key")
    except NonFiniteNumber:
        return invalid("json_non_finite_number")
    except StructuredResourceLimit:
        return invalid("structured_resource_limit")
    except (ValueError, RecursionError):
        return invalid("json_malformed")
    pending, nodes = [(instance, 1)], 0
    while pending:
        value, depth = pending.pop()
        nodes += 1
        if depth > 64 or nodes > 65536:
            return invalid("structured_resource_limit")
        if isinstance(value, dict):
            if len(value) > 1024:
                return invalid("structured_resource_limit")
            try:
                if any(len(key) > 262144 or len(key.encode("utf-8")) > 1024 * 1024 for key in value):
                    return invalid("structured_resource_limit")
            except UnicodeError:
                return invalid("json_invalid_unicode")
            pending.extend((item, depth + 1) for item in value.values())
        elif isinstance(value, list):
            if len(value) > 1024:
                return invalid("structured_resource_limit")
            pending.extend((item, depth + 1) for item in value)
        elif isinstance(value, str):
            try:
                if len(value) > 262144 or len(value.encode("utf-8")) > 1024 * 1024:
                    return invalid("structured_resource_limit")
            except UnicodeError:
                return invalid("json_invalid_unicode")
    if output_format.type == "json_object":
        return replace(result, output=TextOutput(content)) if isinstance(instance, dict) else invalid("json_wrong_root")
    schema = json.loads(output_format.schema_json, parse_float=Decimal)
    try:
        Draft202012Validator(schema).validate(instance)
    except ValidationError as error:
        path = "".join("/" + str(part).replace("~", "~0").replace("/", "~1") for part in error.absolute_path)
        return invalid("schema_mismatch", path)
    return replace(result, output=TextOutput(content))


class _StreamObjectBoundary:
    """Track object closure across deltas without retaining their contents."""

    def __init__(self):
        self.stack = []
        self.quoted = False
        self.escaped = False
        self.closed = False

    def feed(self, text: str) -> str | None:
        for char in text:
            if self.closed:
                if char not in " \t\r\n":
                    return "json_extraneous_content"
                continue
            if self.quoted:
                if self.escaped:
                    self.escaped = False
                elif char == "\\":
                    self.escaped = True
                elif char == '"':
                    self.quoted = False
                continue
            if char == '"':
                self.quoted = True
            elif char in "{[":
                self.stack.append(char)
                if len(self.stack) > 64:
                    return "structured_resource_limit"
            elif char in "}]":
                if not self.stack or self.stack.pop() != ("{" if char == "}" else "["):
                    return "json_malformed"
                if not self.stack:
                    self.closed = True
        return None


async def validate_structured_stream(events, output_format: OutputFormat):
    """Gate the first JSON object byte and validate the provider terminal.

    Provider event numbering is internal; emitted events receive contiguous
    numbering even when leading whitespace is held before the first object.
    This deliberately supports strict JSON Object streams only. The existing
    bounded provider stream and attempt executor own the aggregate byte bound.
    """
    if output_format.type != "json_object":
        raise ValueError("Structured streaming requires JSON Object mode")
    sequence, prefix, model, opened = 1, "", None, False
    boundary = _StreamObjectBoundary()

    async with aclosing(events) as source:
        async for event in source:
            if isinstance(event, StreamDelta):
                model = event.resolved_model
                if event.kind == DeltaKind.TEXT and not opened:
                    prefix += event.text
                    stripped = prefix.lstrip(" \t\r\n")
                    if not stripped:
                        if len(prefix.encode("utf-8")) > 4096:
                            yield StreamFailed(sequence, ProviderFailure(FailureCode.STRUCTURED_OUTPUT_INVALID, False,
                                structured_reason="structured_resource_limit"), resolved_model=model)
                            return
                        continue
                    if not stripped.startswith("{"):
                        yield StreamFailed(sequence, ProviderFailure(FailureCode.STRUCTURED_OUTPUT_INVALID, False,
                            structured_reason="json_wrong_root"), resolved_model=model)
                        return
                    event = replace(event, sequence=sequence, text=prefix)
                    prefix, opened = "", True
                elif prefix:
                    yield StreamDelta(sequence, model, DeltaKind.TEXT, prefix)
                    sequence += 1
                    prefix = ""
                    event = replace(event, sequence=sequence)
                else:
                    event = replace(event, sequence=sequence)
                if event.kind == DeltaKind.TEXT:
                    reason = boundary.feed(event.text)
                    if reason is not None:
                        yield StreamFailed(sequence, ProviderFailure(FailureCode.STRUCTURED_OUTPUT_INVALID, False,
                            structured_reason=reason), resolved_model=model)
                        return
                yield event
                sequence += 1
                continue
            if isinstance(event, StreamFailed):
                yield replace(event, sequence=sequence)
                return
            if not isinstance(event, StreamCompleted):
                raise ValueError("Typed provider stream required")
            # Extraction is a synchronous-only normalization. Streaming does
            # not rewrite bytes that may already have crossed the commit edge.
            checked = validate_structured_result(event.result, replace(output_format, local_extraction=False))
            if prefix and not isinstance(checked, ProviderFailure):
                yield StreamDelta(sequence, model or event.result.resolved_model, DeltaKind.TEXT, prefix)
                sequence += 1
            if isinstance(checked, ProviderFailure):
                yield StreamFailed(sequence, checked, event.result.usage, event.result.resolved_model)
            else:
                yield StreamCompleted(sequence, checked)
            return
