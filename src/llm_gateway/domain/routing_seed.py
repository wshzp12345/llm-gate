"""The v1 Routing seed message encoding; no key access or hashing."""

import re
from uuid import UUID

from llm_gateway.domain.configuration import revision_number


def routing_seed_message(call_id: UUID, routing_policy: str, configuration_revision: str) -> bytes:
    if not isinstance(call_id, UUID) or call_id.version != 4:
        raise ValueError("Routing seed requires Invocation UUIDv4")
    if (not isinstance(routing_policy, str) or len(routing_policy) > 128
            or not re.fullmatch(r"[a-z][a-z0-9]*(?:[._-][a-z0-9]+)*", routing_policy)):
        raise ValueError("Invalid Routing Policy Resource ID")
    revision_number(configuration_revision)
    fields = (str(call_id).encode("ascii"), routing_policy.encode("utf-8"),
              configuration_revision.encode("ascii"))
    return b"gateway.routing-seed/v1" + b"".join(
        len(value).to_bytes(4, "big") + value for value in fields)
