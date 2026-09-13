"""Process-local, fail-fast concurrency permits for future admission/Attempts.

There is no work queue, sleep, automatic retry, or asynchronous release point.
Pool wiring, live policy changes and routing Evidence are separate concerns.
"""

from threading import Lock


class ConcurrencyLease:
    """An acquired permit. Use with/finally so cancellation returns capacity."""

    def __init__(self, pool: "ConcurrencyPool"):
        self._pool = pool
        self._entered = False

    def release(self) -> None:
        with self._pool._lock:
            self._pool._leases.discard(self)

    def __enter__(self):
        with self._pool._lock:
            if self not in self._pool._leases or self._entered:
                raise RuntimeError("Concurrency lease cannot be reused")
            self._entered = True
        return self

    def __exit__(self, exc_type, exc, traceback):
        self.release()
        return False


class ConcurrencyPool:
    def __init__(self, limit: int):
        if type(limit) is not int or limit < 1:
            raise ValueError("Positive concurrency limit required")
        self._limit = limit
        self._leases: set[ConcurrencyLease] = set()
        self._lock = Lock()

    @property
    def in_use(self) -> int:
        with self._lock:
            return len(self._leases)

    def try_acquire(self) -> ConcurrencyLease | None:
        # The lock protects only this counter; no I/O or await occurs while held.
        with self._lock:
            if len(self._leases) >= self._limit:
                return None
            lease = ConcurrencyLease(self)
            self._leases.add(lease)
            return lease
