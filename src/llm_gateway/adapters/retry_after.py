"""Parse one Provider Retry-After into a duration, never a retry permission."""

import math
import re
from datetime import timezone
from email.utils import parsedate_to_datetime


def provider_retry_after_ms(headers, *, now):
    values = headers.get_list("retry-after")
    if len(values) != 1:
        return None
    value = values[0].strip(" \t")
    if not value or len(value) > 128:
        return None
    if re.fullmatch(r"[0-9]+", value):
        return int(value) * 1000
    try:
        date = parsedate_to_datetime(value)
    except (ValueError, TypeError, OverflowError):
        return None
    if date.tzinfo is None or date.utcoffset() is None:
        return None
    current = now()
    if current.tzinfo is None or current.utcoffset() is None:
        raise RuntimeError("Retry-After clock must be timezone-aware")
    return max(0, math.ceil((date.astimezone(timezone.utc) - current).total_seconds() * 1000))
