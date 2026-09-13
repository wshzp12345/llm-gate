"""Read the original ASGI fields, never a framework-combined Header."""

import re

from llm_gateway.application.authorization import Unauthorized


def caller_bearer(raw_headers):
    values = [value for name, value in raw_headers if name.lower() == b"authorization"]
    if (len(values) != 1 or not isinstance(values[0], bytes) or len(values[0]) > 16391
            or not re.fullmatch(rb"(?i:Bearer) ([A-Za-z0-9._~+/-]+={0,})", values[0])):
        raise Unauthorized()
    return values[0][7:].decode("ascii")
