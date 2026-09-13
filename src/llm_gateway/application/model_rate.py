"""Atomic per-alias rolling-minute admission, shared by sync and streaming.

One consume per validated logical request, never per Provider Attempt. State is
process-local, independent of configuration revision, and idle entries expire.
The outer instance API gate bounds the number of recent admitted requests.
"""

from collections import deque
from threading import Lock
from time import monotonic_ns


class ModelRate:
    def __init__(self, *, clock=monotonic_ns):
        self._clock = clock
        self._lock = Lock()
        self._requests = {}
        self._last = 0

    def try_consume(self, model: str, requests_per_minute: int = 5) -> bool:
        if not isinstance(model, str) or not model:
            raise ValueError("Model identity required")
        if type(requests_per_minute) is not int or requests_per_minute <= 0:
            raise ValueError("Positive integer RPM required")
        with self._lock:
            now = self._clock()
            if type(now) is not int or now < self._last:
                raise ValueError("Nondecreasing monotonic nanoseconds required")
            self._last = now
            cutoff = now - 60_000_000_000
            for identity, times in tuple(self._requests.items()):
                while times and times[0] <= cutoff:
                    times.popleft()
                if not times:
                    del self._requests[identity]
            times = self._requests.get(model)
            if times is not None and len(times) >= requests_per_minute:
                return False
            if times is None:
                times = self._requests[model] = deque()
            times.append(now)
            return True
