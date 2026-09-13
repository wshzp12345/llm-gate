import asyncio
import json
from datetime import timedelta
from types import SimpleNamespace

import pytest

from llm_gateway.adapters.trust_bundle import pem_identity
from llm_gateway.adapters.trust_expiry_monitor import TrustExpiryMonitor
from tests.test_text_fingerprints import snapshot
from tests.test_trust_bundle import certificate, NOW


def selected(*, days=31):
    content = json.loads(snapshot().snapshot_json)
    pem = certificate(end=NOW + timedelta(days=days))
    content["providers"]["provider-a"]["transport"]["tls"] = {
        "trust": "bundle", "trust_bundle": {"identity": pem_identity(pem), "pem": pem}}
    return snapshot(content)


class Sink:
    def __init__(self):
        self.gauges, self.events, self.deliveries = {}, {}, []
        self.fail = False

    async def replace(self, gauges):
        self.gauges = gauges

    async def append(self, event):
        self.deliveries.append(event)
        self.events.setdefault(event.event_id, event)
        if self.fail:
            raise RuntimeError("Lost acknowledgement")


def test_warning_boundaries_and_single_expired_transition():
    async def run():
        now, sink = [NOW], Sink()
        monitor = TrustExpiryMonitor(metrics=sink, events=sink, clock=lambda: now[0])
        config = selected()
        await monitor.refresh(config)
        assert sink.gauges == {"provider-a": 31} and not sink.events
        now[0] += timedelta(days=1)
        await monitor.refresh(config)
        assert len(sink.events) == 1
        now[0] += timedelta(hours=23, minutes=59)
        await monitor.refresh(config)
        assert len(sink.events) == 1
        now[0] += timedelta(minutes=1)
        await monitor.refresh(config)
        assert len(sink.events) == 2
        now[0] = NOW + timedelta(days=31)
        await monitor.refresh(config)
        assert sink.gauges == {"provider-a": 0}
        assert all(event.kind == "expiring" for event in sink.events.values())
        now[0] += timedelta(microseconds=1)
        await monitor.refresh(config)
        await monitor.refresh(config)
        assert sum(event.kind == "expired" for event in sink.events.values()) == 1
        assert sink.gauges["provider-a"] < 0

    asyncio.run(run())


def test_identity_rotation_and_metric_replacement_without_content_leakage():
    async def run():
        sink = Sink()
        monitor = TrustExpiryMonitor(metrics=sink, events=sink, clock=lambda: NOW)
        first, second = selected(days=10), selected(days=11)
        await monitor.refresh(first)
        await monitor.refresh(second)
        await monitor.refresh(first)
        assert len(sink.events) == 2
        assert "BEGIN CERTIFICATE" not in repr(sink.events) and "provider.invalid" not in repr(sink.events)
        assert set(sink.gauges) == {"provider-a"}
        await monitor.refresh(snapshot())
        assert sink.gauges == {}

    asyncio.run(run())


def test_ambiguous_event_ack_reuses_identity_and_cooldown_starts_after_ack():
    async def run():
        now, sink = [NOW], Sink()
        monitor = TrustExpiryMonitor(metrics=sink, events=sink, clock=lambda: now[0])
        config = selected(days=10)
        sink.fail = True
        with pytest.raises(RuntimeError):
            await monitor.refresh(config)
        first = sink.deliveries[0]
        now[0] += timedelta(hours=2)
        sink.fail = False
        await monitor.refresh(config)
        assert sink.deliveries[1] is first and len(sink.events) == 1
        assert not monitor._pending
        now[0] += timedelta(hours=23)
        await monitor.refresh(config)
        assert len(sink.events) == 1

    asyncio.run(run())


def test_concurrent_refreshes_do_not_duplicate_warning():
    async def run():
        sink = Sink()
        monitor = TrustExpiryMonitor(metrics=sink, events=sink, clock=lambda: NOW)
        config = selected(days=10)
        await asyncio.gather(*(monitor.refresh(config) for _ in range(10)))
        assert len(sink.events) == len(sink.deliveries) == 1

    asyncio.run(run())


def test_invalid_bundle_does_not_publish_partial_metrics():
    async def run():
        sink = Sink()
        monitor = TrustExpiryMonitor(metrics=sink, events=sink, clock=lambda: NOW)
        content = json.loads(selected().snapshot_json)
        content["providers"]["provider-a"]["transport"]["tls"]["trust_bundle"]["pem"] = "invalid"
        with pytest.raises(ValueError):
            await monitor.refresh(snapshot(content))
        assert not sink.gauges and not sink.events

    asyncio.run(run())


def test_lifecycle_task_observes_current_snapshot_and_cancels_cleanly():
    async def run():
        observed = asyncio.Event()
        class Metrics(Sink):
            async def replace(self, gauges):
                await super().replace(gauges)
                observed.set()
        sink = Metrics()
        monitor = TrustExpiryMonitor(metrics=sink, events=sink, clock=lambda: NOW)
        task = asyncio.create_task(monitor.run(SimpleNamespace(current=selected(days=10)), interval_seconds=60))
        await asyncio.wait_for(observed.wait(), 2)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert sink.gauges == {"provider-a": 10}

    asyncio.run(run())
