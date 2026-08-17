# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""Regression tests for HubRegistry's resource bounds.

These lock in two fixes to the per-client pipeline registry:

* a concurrent burst of *distinct* clients must never exceed ``MAX_PIPELINES``, and
* the per-client lock must not leak when a pipeline is evicted while idle.

The registry is driven with a **stubbed Hub** — no real replay, no application
threads — so the suite is fast and hermetic and starts no server. Construction
still goes through ``asyncio.to_thread`` inside ``acquire``, which is where the
finding-1 race lived; the stub keeps the one property that matters there, that
building a pipeline yields the event loop.

Everything runs on one event loop with no true parallelism, exactly as in
production, so these are deterministic rather than probabilistic: the fix makes
``_make_room`` count reservations *synchronously* right after the capacity check,
so "how many got in" does not depend on scheduling.
"""

import asyncio

import pytest

from pswamp_web import hub as hub_module
from pswamp_web.hub import CapacityError, HubRegistry


class FakeHub:
    """Instant stand-in for a real pipeline: no player, no threads, no data."""

    def __init__(self, client_id: str = "-") -> None:
        self.client_id = client_id
        self.stopped = False

    def start(self, loop) -> None:  # noqa: ARG002 — mirrors Hub.start's signature
        # Deliberately instant. acquire() still runs this via asyncio.to_thread,
        # which yields the loop regardless, so concurrent acquires interleave.
        pass

    def stop(self) -> None:
        self.stopped = True

    def dead_threads(self):
        return []


@pytest.fixture
async def make_registry(monkeypatch):
    """Factory for HubRegistries whose pipelines are FakeHubs, on the test loop.

    Returns a callable so a test can pick its own cap / idle timeout. Every
    registry it hands out is drained on teardown, and any idle-evict task a
    capacity eviction detached is cancelled, so the loop closes clean.
    """
    monkeypatch.setattr(hub_module, "Hub", FakeHub)
    created: list[HubRegistry] = []

    def _make(*, max_pipelines: int = 3, idle_seconds: float = 0.02) -> HubRegistry:
        reg = HubRegistry(max_pipelines=max_pipelines, idle_seconds=idle_seconds)
        reg.bind(asyncio.get_running_loop())
        created.append(reg)
        return reg

    yield _make

    for reg in created:
        await reg.stop_all()
    lingering = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    for task in lingering:
        task.cancel()
    if lingering:
        await asyncio.gather(*lingering, return_exceptions=True)


async def _wait_until(predicate, *, timeout: float = 1.0, interval: float = 0.005):
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        if predicate():
            return
        await asyncio.sleep(interval)
    raise AssertionError("condition not met within timeout")


async def test_one_client_many_sockets_build_one_pipeline(make_registry):
    """The invariant the per-client lock exists for: five simultaneous
    first-connects from ONE client build one pipeline, not five."""
    reg = make_registry()
    hubs = await asyncio.gather(*(reg.acquire("c1") for _ in range(5)))
    assert reg.live == 1
    assert len({id(h) for h in hubs}) == 1
    for _ in range(5):
        reg.release("c1")


async def test_concurrent_distinct_clients_never_exceed_cap(make_registry):
    """Finding 1: a burst of distinct clients connecting at once is admitted only
    up to the cap; the rest are refused, never over-built.

    Pre-fix this produced ``live == n`` and zero refusals, because ``_make_room``
    read ``live`` while every in-flight construction was still invisible to it.
    """
    reg = make_registry(max_pipelines=3)
    n = reg.max_pipelines + 4
    results = await asyncio.gather(
        *(reg.acquire(f"c{i}") for i in range(n)), return_exceptions=True
    )
    acquired = [r for r in results if not isinstance(r, Exception)]
    refused = [r for r in results if isinstance(r, CapacityError)]
    unexpected = [
        r for r in results
        if isinstance(r, Exception) and not isinstance(r, CapacityError)
    ]
    assert not unexpected, unexpected
    assert reg.live == reg.max_pipelines  # the property that must hold: no overshoot
    assert len(acquired) == reg.max_pipelines
    assert len(refused) == n - reg.max_pipelines
    assert reg._pending == 0  # every reservation was released


async def test_new_client_refused_when_all_slots_in_use(make_registry):
    """At the cap with every pipeline still watched, a new client is refused with
    CapacityError (which connected_hub turns into the 1013 close)."""
    reg = make_registry(max_pipelines=3)
    for i in range(reg.max_pipelines):
        await reg.acquire(f"c{i}")  # socket kept open — not evictable
    with pytest.raises(CapacityError):
        await reg.acquire("newcomer")
    assert reg.live == reg.max_pipelines


async def test_idle_eviction_reclaims_the_per_client_lock(make_registry):
    """Finding 4: after a client's pipeline idle-evicts, its lock is gone too.

    Pre-fix ``_locks`` kept one lock per distinct client id forever, because the
    idle evictor holds the lock while evicting and the old guard only dropped an
    *unheld* lock.
    """
    reg = make_registry(max_pipelines=8, idle_seconds=0.02)
    for i in range(5):
        await reg.acquire(f"c{i}")
        reg.release(f"c{i}")  # sockets -> 0, schedules the idle evictor
    await _wait_until(lambda: reg.live == 0)
    assert reg.live == 0
    assert reg._locks == {}, f"lock leak: {list(reg._locks)}"
    assert reg._acquiring == {}


async def test_capacity_eviction_reclaims_the_victim_lock(make_registry):
    """The other eviction path: a new client at the cap evicts the LRU idle
    pipeline and reclaims its lock (idle_seconds is large so only the capacity
    path can fire here)."""
    reg = make_registry(max_pipelines=3, idle_seconds=5.0)
    for i in range(reg.max_pipelines):
        await reg.acquire(f"c{i}")
        reg.release(f"c{i}")  # idle & evictable, but its timer will not fire soon
    assert set(reg._locks) == {"c0", "c1", "c2"}
    await reg.acquire("newcomer")  # forces a capacity eviction of the LRU victim
    assert reg.live == reg.max_pipelines
    assert "newcomer" in reg._locks
    assert "c0" not in reg._locks  # the evicted victim's lock was reclaimed
    assert len(reg._locks) == reg.max_pipelines


async def test_reconnect_before_idle_reuses_same_pipeline(make_registry):
    """Closing the last socket starts an idle timer rather than tearing down, so a
    reconnect within the window rejoins the very same pipeline."""
    reg = make_registry(idle_seconds=0.05)
    first = await reg.acquire("c1")
    reg.release("c1")  # schedules idle eviction
    second = await reg.acquire("c1")  # reconnect before it fires
    assert first is second
    assert reg.live == 1
    await asyncio.sleep(0.1)  # past the original idle deadline
    assert reg.live == 1, "the idle eviction was not cancelled on reconnect"
    reg.release("c1")


async def test_failed_construction_leaves_no_reservation_or_lock(make_registry, monkeypatch):
    """If Hub.start raises, the slot reservation and the per-client lock are both
    released — a failed connect must not permanently consume capacity."""

    class BoomHub(FakeHub):
        def start(self, loop):  # noqa: ARG002
            raise RuntimeError("boom")

    monkeypatch.setattr(hub_module, "Hub", BoomHub)
    reg = make_registry()
    with pytest.raises(RuntimeError, match="boom"):
        await reg.acquire("c1")
    assert reg.live == 0
    assert reg._pending == 0
    assert reg._locks == {}
    assert reg._acquiring == {}
