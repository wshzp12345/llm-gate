"""YAML 1.2 Core subset; nodes are decoded explicitly, never constructed as objects."""

import io
import json
import math
import re

from ruamel.yaml import YAML
from ruamel.yaml.error import YAMLError
from ruamel.yaml.nodes import MappingNode, ScalarNode, SequenceNode
from ruamel.yaml.tokens import (
    AliasToken, AnchorToken, TagToken, DirectiveToken, DocumentStartToken,
    ScalarToken, BlockMappingStartToken, BlockSequenceStartToken, FlowMappingStartToken,
    FlowSequenceStartToken, BlockEndToken, FlowMappingEndToken, FlowSequenceEndToken,
)

from llm_gateway.adapters.configuration_dto import Bundle, Metadata
from llm_gateway.adapters.configuration_json import ConfigurationStructureError, ParseLimits
from llm_gateway.domain.configuration import revision_number
from llm_gateway.domain.configuration_changes import SECTIONS


def _scalar(node):
    value = node.value
    if node.style is not None:
        return value
    if value in {"", "~", "null", "Null", "NULL"}:
        return None
    if value in {"true", "True", "TRUE"}:
        return True
    if value in {"false", "False", "FALSE"}:
        return False
    if re.fullmatch(r"[-+]?[0-9]+", value):
        return int(value, 10)
    if re.fullmatch(r"0o[0-7]+", value):
        return int(value[2:], 8)
    if re.fullmatch(r"0x[0-9a-fA-F]+", value):
        return int(value[2:], 16)
    if re.fullmatch(r"[-+]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][-+]?[0-9]+)?", value):
        result = float(value)
        if not math.isfinite(result):
            raise ValueError("Non-finite number")
        return result
    if re.fullmatch(r"[-+]?\.(?:inf|Inf|INF|nan|NaN|NAN)", value):
        raise ValueError("Non-finite number")
    # Core has no timestamp/sexagesimal/yes-no coercion.
    return value


def parse_bundle_yaml(body: bytes, limits: ParseLimits) -> Bundle:
    try:
        if len(body) > limits.max_bytes or body.startswith(b"\xef\xbb\xbf"):
            raise ValueError("Invalid input size or BOM")
        text = body.decode("utf-8")
        if "${" in text:
            raise ValueError("Interpolation is not supported")
        depth, nodes, documents = 0, 0, 0
        starts = (BlockMappingStartToken, BlockSequenceStartToken, FlowMappingStartToken, FlowSequenceStartToken)
        ends = (BlockEndToken, FlowMappingEndToken, FlowSequenceEndToken)
        for token in YAML(typ="base", pure=True).scan(text):
            if isinstance(token, (AnchorToken, AliasToken, TagToken)):
                raise ValueError("YAML references and tags are forbidden")
            if isinstance(token, DirectiveToken) and (token.name != "YAML" or token.value != (1, 2)):
                raise ValueError("Only YAML 1.2 is supported")
            if isinstance(token, DocumentStartToken):
                documents += 1
                if documents > 1:
                    raise ValueError("Multiple documents")
            if isinstance(token, starts):
                depth += 1
                nodes += 1
                if depth > limits.max_depth:
                    raise ValueError("Nesting ceiling exceeded")
            elif isinstance(token, ends):
                depth -= 1
            elif isinstance(token, ScalarToken):
                nodes += 1
                if len(token.value.encode("utf-8")) > limits.max_string_bytes:
                    raise ValueError("String ceiling exceeded")
            if nodes > limits.max_nodes:
                raise ValueError("Node ceiling exceeded")
        yaml = YAML(typ="base", pure=True)
        yaml.version = (1, 2)
        root = yaml.compose(text)  # compose rejects a second implicit/explicit document.

        def decode(node, level=1):
            if level > limits.max_depth:
                raise ValueError("Nesting ceiling exceeded")
            if isinstance(node, ScalarNode):
                return _scalar(node)
            if isinstance(node, SequenceNode):
                if len(node.value) > limits.max_collection_items:
                    raise ValueError("Collection ceiling exceeded")
                return [decode(value, level + 1) for value in node.value]
            if isinstance(node, MappingNode):
                if len(node.value) > limits.max_collection_items:
                    raise ValueError("Collection ceiling exceeded")
                result = {}
                for key_node, value_node in node.value:
                    key = decode(key_node, level + 1)
                    if not isinstance(key, str) or key == "<<" or key in result:
                        raise ValueError("Invalid or duplicate mapping key")
                    result[key] = decode(value_node, level + 1)
                return result
            raise ValueError("Unsupported YAML node")

        return Bundle.model_validate(decode(root))
    except (YAMLError, ValueError, TypeError, RecursionError):
        raise ConfigurationStructureError() from None


def export_bundle_yaml(snapshot_json: bytes, revision: str, description: str | None = None) -> bytes:
    """Export immutable domain content with the exported Revision as its base."""
    revision_number(revision)
    snapshot = json.loads(snapshot_json)
    result = {"schema_version": snapshot["schema_version"], "base_revision": revision}
    if description is not None:
        result["metadata"] = Metadata(description=description).model_dump()
    for section in SECTIONS:
        value = snapshot[section]
        if section not in {"resource_policies", "replay_policies"}:
            value = dict(sorted(value.items(), key=lambda pair: pair[0].encode("utf-8")))
        result[section] = value
    output = io.StringIO()
    yaml = YAML(typ="safe", pure=True)
    yaml.default_flow_style = False
    yaml.sort_base_mapping_type_on_output = False
    yaml.allow_unicode = True
    yaml.line_break = "\n"
    yaml.dump(result, output)
    return (output.getvalue().rstrip("\n") + "\n").encode("utf-8")
