"""Fixed-cardinality request metrics, counted before export queue admission."""

from dataclasses import dataclass
import time


BOUNDS = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60)
OUTCOMES = ("ok", "error", "cancelled", "unknown")


@dataclass(frozen=True)
class AccessMetricsSnapshot:
    started_unix_ns: int
    requests: tuple[int, ...]
    latency_count: int
    latency_sum_ns: int
    latency_buckets: tuple[int, ...]


class AccessMetrics:
    def __init__(self):
        self._started = time.time_ns()
        self._requests = [0] * len(OUTCOMES)
        self._buckets = [0] * (len(BOUNDS) + 1)
        self._count = self._sum = 0

    def observe(self, access):
        root = next((span for span in access.spans if span.stage == "http"), None)
        outcome = root.outcome if root is not None and root.outcome in OUTCOMES else "unknown"
        self._requests[OUTCOMES.index(outcome)] += 1
        if root is not None:
            seconds = root.duration_ns / 1_000_000_000
            bucket = next((index for index, bound in enumerate(BOUNDS) if seconds <= bound), len(BOUNDS))
            self._buckets[bucket] += 1
            self._count += 1
            self._sum += root.duration_ns

    def snapshot(self):
        return AccessMetricsSnapshot(self._started, tuple(self._requests), self._count, self._sum, tuple(self._buckets))
