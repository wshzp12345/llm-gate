import pytest

from llm_gateway.domain.configuration_changes import (
    ChangeConflict, Operation, SnapshotResources, Target, apply_changes,
    canonical_operations, derive_changes, rebase_changes,
)


A = Target("providers", "a")
B = Target("providers", "b")
POLICY = Target("resource_policies")


def test_apply_is_atomic_and_never_mutates_base():
    base = SnapshotResources({A: b'{"a":1}'})
    with pytest.raises(ChangeConflict):
        apply_changes(base, (Operation("remove", A), Operation("remove", B)))
    assert base.values[A] == b'{"a":1}'
    with pytest.raises(TypeError):
        base.values[A] = b"{}"


def test_operations_are_order_independent_and_remove_noop_replacements():
    base = SnapshotResources({A: b"{}", POLICY: b"{}"})
    operations = (Operation("replace", POLICY, b"{}"), Operation("add", B, b"{}"), Operation("remove", A))
    result, normalized = apply_changes(base, operations)
    assert normalized == (Operation("remove", A), Operation("add", B, b"{}"))
    assert apply_changes(base, tuple(reversed(operations))) == (result, normalized)
    assert derive_changes(base, result) == normalized


@pytest.mark.parametrize("op,base", [
    (Operation("add", A, b"{}"), SnapshotResources({A: b"{}"})),
    (Operation("replace", A, b"{}"), SnapshotResources({})),
    (Operation("remove", A), SnapshotResources({})),
])
def test_target_preconditions(op, base):
    with pytest.raises(ChangeConflict):
        apply_changes(base, (op,))


def test_duplicate_targets_rejected_before_noop_elimination():
    with pytest.raises(ValueError):
        canonical_operations((Operation("add", A, b"{}"), Operation("remove", A)))


def test_rebase_disjoint_resources():
    base = SnapshotResources({A: b"1", B: b"1"})
    intended = SnapshotResources({A: b"2", B: b"1"})
    current = SnapshotResources({A: b"1", B: b"3"})
    result, operations = rebase_changes(base, intended, current, derive_changes(base, intended))
    assert result == SnapshotResources({A: b"2", B: b"3"})
    assert operations == (Operation("replace", A, b"2"),)


def test_rebase_same_resource_disjoint_fields_conflict():
    base = SnapshotResources({A: b'{"x":1,"y":1}'})
    intended = SnapshotResources({A: b'{"x":2,"y":1}'})
    current = SnapshotResources({A: b'{"x":1,"y":2}'})
    with pytest.raises(ChangeConflict) as failure:
        rebase_changes(base, intended, current, derive_changes(base, intended))
    assert failure.value.reason == "resource_changed"
    assert failure.value.target == A


@pytest.mark.parametrize("base,intended", [
    (SnapshotResources({}), SnapshotResources({A: b"{}"})),
    (SnapshotResources({A: b"{}"}), SnapshotResources({})),
    (SnapshotResources({A: b"1"}), SnapshotResources({A: b"2"})),
])
def test_rebase_convergent_operations_disappear(base, intended):
    result, operations = rebase_changes(base, intended, intended, derive_changes(base, intended))
    assert result == intended
    assert operations == ()


@pytest.mark.parametrize("resource_id", ["", "A", " a", "a ", "a..b", "a_", "a/b", "模型", "a" * 129])
def test_resource_id_profile(resource_id):
    with pytest.raises(ValueError):
        Target("providers", resource_id)


@pytest.mark.parametrize("kwargs", [
    {"op": "add", "target": POLICY, "value": b"{}"},
    {"op": "remove", "target": POLICY},
    {"op": "remove", "target": A, "value": b"{}"},
    {"op": "replace", "target": A},
])
def test_closed_operation_variants(kwargs):
    with pytest.raises(ValueError):
        Operation(**kwargs)


def test_section_order_is_protocol_order_not_alphabetical():
    operations = (Operation("replace", POLICY, b"{}"), Operation("add", Target("model_aliases", "a"), b"{}"), Operation("add", B, b"{}"))
    assert [op.target.section for op in canonical_operations(operations)] == ["providers", "model_aliases", "resource_policies"]
