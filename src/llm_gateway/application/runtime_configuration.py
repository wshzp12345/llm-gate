"""Process-local publication observation; immutable snapshots replace atomically."""

import asyncio
import math
from typing import Protocol

from llm_gateway.application.active_configuration import LoadedConfiguration
from llm_gateway.domain.configuration import ConfigurationPersistenceUnavailable


class RuntimeSnapshotTarget(Protocol):
    async def install(self, snapshot: LoadedConfiguration) -> None: ...
    def suspend(self) -> None: ...
    async def reap(self) -> None: ...


class RuntimeConfiguration:
    def __init__(self, *, loader, target: RuntimeSnapshotTarget, timeout_seconds: float):
        if type(timeout_seconds) not in (int, float) or not math.isfinite(timeout_seconds) or timeout_seconds <= 0:
            raise ValueError("Positive runtime refresh deadline required")
        self._loader, self._target = loader, target
        self._timeout = timeout_seconds
        self._snapshot = None
        self._generation = 0
        self._lock = asyncio.Lock()

    @property
    def current(self) -> LoadedConfiguration | None:
        return self._snapshot

    async def refresh(self):
        async with self._lock:
            generation = self._generation
            try:
                async with asyncio.timeout(self._timeout):
                    snapshot = await self._loader.load(timeout_seconds=self._timeout)
                    # A publication invalidated this already-running read. Never
                    # restore an observation obtained before that publication.
                    if generation != self._generation:
                        return None
                    if snapshot is None:
                        self._target.suspend()
                        await self._target.reap()
                    else:
                        await self._target.install(snapshot)
                    if generation != self._generation:
                        return None
                    self._snapshot = snapshot
                    return snapshot
            except BaseException:
                if generation == self._generation:
                    self._snapshot = None
                    self._target.suspend()
                raise

    async def after_publication(self):
        # Runs before acquiring the refresh lock, fencing a concurrent old read.
        self._generation += 1
        generation = self._generation
        self._snapshot = None
        try:
            if await self.refresh() is None:
                raise ConfigurationPersistenceUnavailable()
        except BaseException:
            if generation == self._generation:
                self._snapshot = None
                self._target.suspend()
            raise

    async def watch(self, *, stop: asyncio.Event, interval_seconds: float):
        if type(interval_seconds) not in (int, float) or not math.isfinite(interval_seconds) or interval_seconds <= 0:
            raise ValueError("Positive observation interval required")
        while not stop.is_set():
            try:
                await asyncio.wait_for(stop.wait(), timeout=interval_seconds)
            except TimeoutError:
                if stop.is_set():
                    return
                try:
                    await self.refresh()
                except Exception:
                    # Configuration recovery stays available; no stale Snapshot
                    # is exposed after a failed fresh observation.
                    pass
                try:
                    async with asyncio.timeout(self._timeout):
                        await self._target.reap()
                except Exception:
                    # Retired clients remain owned for the next cleanup pass.
                    pass
