"""Opaque business identifiers, independent of Trace and authorization identity."""

from dataclasses import dataclass
import re


@dataclass(frozen=True)
class BusinessCorrelation:
    task_id: str | None = None
    turn_id: str | None = None
    step_id: str | None = None

    def __post_init__(self):
        for value in (self.task_id, self.turn_id, self.step_id):
            if value is not None and (type(value) is not str or not re.fullmatch(r"[A-Za-z0-9._:-]{1,128}", value)):
                raise ValueError("Invalid business correlation")

    def require_for(self, caller_type, operation_scope):
        if caller_type not in {"agent", "non-agent"} or operation_scope not in {"turn", "step"}:
            raise ValueError("Invalid correlation declaration")
        if caller_type == "non-agent":
            if operation_scope != "turn":
                raise ValueError("Agent operation scope requires Agent caller")
            return
        if self.task_id is None or self.turn_id is None or (operation_scope == "step" and self.step_id is None):
            raise ValueError("Required Agent correlation missing")
