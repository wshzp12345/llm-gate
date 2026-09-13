"""Whole-resource configuration changes over already canonicalized values.

Full Snapshot structural/reference/capability validation remains a separate
application responsibility after changes have been applied atomically.
"""

import re
from dataclasses import dataclass
from types import MappingProxyType
from collections.abc import Mapping


REGISTRIES = (
    "providers", "provider_model_bindings", "model_aliases", "routing_policies",
    "pricing_tables", "safety_policies",
)
SINGLETONS = ("resource_policies", "replay_policies")
SECTIONS = REGISTRIES + SINGLETONS


@dataclass(frozen=True)
class Target:
    section: str
    resource_id: str | None = None

    def __post_init__(self) -> None:
        if self.section not in SECTIONS:
            raise ValueError("Unknown configuration section")
        if self.section in SINGLETONS:
            if self.resource_id is not None:
                raise ValueError("Singleton cannot carry a resource ID")
        elif (not isinstance(self.resource_id, str) or len(self.resource_id) > 128
              or not re.fullmatch(r"[a-z][a-z0-9]*(?:[._-][a-z0-9]+)*", self.resource_id)):
            raise ValueError("Invalid configuration resource ID")

    @property
    def order(self) -> tuple[int, bytes]:
        return (SECTIONS.index(self.section), (self.resource_id or "").encode("utf-8"))


@dataclass(frozen=True)
class Operation:
    op: str
    target: Target
    value: bytes | None = None

    def __post_init__(self) -> None:
        if self.op not in {"add", "replace", "remove"}:
            raise ValueError("Unknown configuration operation")
        if self.target.section in SINGLETONS and self.op != "replace":
            raise ValueError("Singleton only permits replace")
        if self.op == "remove":
            if self.value is not None:
                raise ValueError("Remove cannot carry a value")
        elif not isinstance(self.value, bytes) or not self.value:
            raise ValueError("Complete canonical value required")


@dataclass(frozen=True)
class SnapshotResources:
    values: Mapping[Target, bytes]

    def __post_init__(self) -> None:
        if any(not isinstance(key, Target) or not isinstance(value, bytes) or not value
               for key, value in self.values.items()):
            raise ValueError("Invalid canonical resources")
        object.__setattr__(self, "values", MappingProxyType(dict(self.values)))


class ChangeConflict(Exception):
    def __init__(self, reason: str, target: Target) -> None:
        self.reason = reason
        self.target = target
        super().__init__(reason)


def canonical_operations(operations: tuple[Operation, ...]) -> tuple[Operation, ...]:
    targets = [operation.target for operation in operations]
    if len(set(targets)) != len(targets):
        raise ValueError("Duplicate configuration operation target")
    return tuple(sorted(operations, key=lambda operation: operation.target.order))


def apply_changes(
    base: SnapshotResources, operations: tuple[Operation, ...],
) -> tuple[SnapshotResources, tuple[Operation, ...]]:
    provisional = dict(base.values)
    normalized = []
    for operation in canonical_operations(operations):
        present = operation.target in base.values
        if operation.op == "add" and present:
            raise ChangeConflict("target_exists", operation.target)
        if operation.op in {"replace", "remove"} and not present:
            raise ChangeConflict("target_missing", operation.target)
        if operation.op == "remove":
            del provisional[operation.target]
        else:
            if operation.value == base.values.get(operation.target):
                continue
            provisional[operation.target] = operation.value
        normalized.append(operation)
    return SnapshotResources(provisional), tuple(normalized)


def derive_changes(base: SnapshotResources, intended: SnapshotResources) -> tuple[Operation, ...]:
    result = []
    for target in sorted(base.values.keys() | intended.values.keys(), key=lambda item: item.order):
        before, after = base.values.get(target), intended.values.get(target)
        if before == after:
            continue
        if after is None:
            result.append(Operation("remove", target))
        elif before is None and target.section in REGISTRIES:
            result.append(Operation("add", target, after))
        else:
            # Initial Bundle singletons are represented by replace; they are not
            # an explicit add operation on a Gateway singleton resource.
            result.append(Operation("replace", target, after))
    return tuple(result)


def rebase_changes(
    original: SnapshotResources, intended: SnapshotResources,
    current: SnapshotResources, operations: tuple[Operation, ...],
) -> tuple[SnapshotResources, tuple[Operation, ...]]:
    provisional = dict(current.values)
    for operation in canonical_operations(operations):
        target = operation.target
        before, after, now = original.values.get(target), intended.values.get(target), current.values.get(target)
        if now == after:
            continue
        if now != before:
            raise ChangeConflict("resource_changed", target)
        if after is None:
            provisional.pop(target, None)
        else:
            provisional[target] = after
    result = SnapshotResources(provisional)
    return result, derive_changes(current, result)
