# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""PipelineRegistry's resource bounds, plus one real Pipeline end to end.

The registry cases are ``app/server-python/tests/test_hub_registry.py``
re-targeted at the core registry: a concurrent burst of distinct keys never
exceeds the cap, and the per-key lock is reclaimed on eviction. Driven with a
stub pipeline, so nothing streams and no server starts.
"""

from __future__ import annotations

import asyncio

import pytest
from support import HISTORY, Measurement, NumberResult, at, measurements, take

from pswamp_core.bus import InProcessBus, Overflow
from pswamp_core.datagateway import DataGateway, Player
from pswamp_core.datagateway.clients import InMemoryClient
from pswamp_core.messages import PlayerStatus
from pswamp_core.pipeline import CapacityError, Pipeline, PipelineRegistry
from test_module import Doubler


class FakePipeline:
    """Instant stand-in: no player, no tasks, no data."""

    def __init__(self, key: str) -> None:
        self.key = key
        self.stopped = False

    async def start(self) -> None:
        await asyncio.sleep(0)  # yield the loop, as a real build does

    async def stop(self) -> None:
        self.stopped = True


@pytest.fixture
async def make_registry():
    created: list[PipelineRegistry] = []

    def _make(*, max_pipelines: int = 3, idle_seconds: float = 0.02, factory=FakePipeline):
        reg = PipelineRegistry(factory, max_pipelines=max_pipelines, idle_seconds=idle_seconds)
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


async def test_one_key_many_sockets_build_one_pipeline(make_registry):
    reg = make_registry()
    pipelines = await asyncio.gather(*(reg.acquire("c1") for _ in range(5)))
    assert reg.live == 1
    assert len({id(p) for p in pipelines}) == 1
    assert reg.watchers("c1") == 5
    for _ in range(5):
        reg.release("c1")


async def test_concurrent_distinct_keys_never_exceed_cap(make_registry):
    reg = make_registry(max_pipelines=3)
    n = reg.max_pipelines + 4
    results = await asyncio.gather(
        *(reg.acquire(f"c{i}") for i in range(n)), return_exceptions=True
    )
    acquired = [r for r in results if not isinstance(r, Exception)]
    refused = [r for r in results if isinstance(r, CapacityError)]
    unexpected = [
        r for r in results if isinstance(r, Exception) and not isinstance(r, CapacityError)
    ]
    assert not unexpected, unexpected
    assert reg.live == reg.max_pipelines
    assert len(acquired) == reg.max_pipelines
    assert len(refused) == n - reg.max_pipelines
    assert reg._pending == 0


async def test_new_key_refused_when_all_slots_in_use(make_registry):
    reg = make_registry(max_pipelines=3)
    for i in range(reg.max_pipelines):
        await reg.acquire(f"c{i}")
    with pytest.raises(CapacityError):
        await reg.acquire("newcomer")
    assert reg.live == reg.max_pipelines
    assert reg.peek("newcomer") is None


async def test_idle_eviction_reclaims_the_per_key_lock(make_registry):
    reg = make_registry(max_pipelines=8, idle_seconds=0.02)
    for i in range(5):
        await reg.acquire(f"c{i}")
        reg.release(f"c{i}")
    await _wait_until(lambda: reg.live == 0)
    assert reg._locks == {}, f"lock leak: {list(reg._locks)}"
    assert reg._acquiring == {}


async def test_capacity_eviction_reclaims_the_victim_lock(make_registry):
    reg = make_registry(max_pipelines=3, idle_seconds=5.0)
    victims = []
    for i in range(reg.max_pipelines):
        victims.append(await reg.acquire(f"c{i}"))
        reg.release(f"c{i}")
    assert set(reg._locks) == {"c0", "c1", "c2"}
    await reg.acquire("newcomer")
    assert reg.live == reg.max_pipelines
    assert "newcomer" in reg._locks
    assert "c0" not in reg._locks
    assert victims[0].stopped is True


async def test_reconnect_before_idle_reuses_same_pipeline(make_registry):
    reg = make_registry(idle_seconds=0.05)
    first = await reg.acquire("c1")
    reg.release("c1")
    second = await reg.acquire("c1")
    assert first is second
    await asyncio.sleep(0.1)
    assert reg.live == 1, "the idle eviction was not cancelled on reconnect"
    reg.release("c1")


async def test_failed_construction_leaves_no_reservation_or_lock(make_registry):
    class Boom(FakePipeline):
        async def start(self):
            raise RuntimeError("boom")

    reg = make_registry(factory=Boom)
    with pytest.raises(RuntimeError, match="boom"):
        await reg.acquire("c1")
    assert reg.live == 0
    assert reg._pending == 0
    assert reg._locks == {}
    assert reg._acquiring == {}


async def test_async_factory_and_session(make_registry):
    async def build(key: str) -> FakePipeline:
        await asyncio.sleep(0)
        return FakePipeline(key)

    reg = make_registry(factory=build)
    async with reg.session("c9") as pipeline:
        assert pipeline.key == "c9"
        assert reg.keys() == ["c9"]
        assert reg.watchers("c9") == 1
    assert reg.watchers("c9") == 0


# --- a real pipeline, end to end ------------------------------------------------


async def test_pipeline_runs_player_and_module_on_one_bus():
    client = InMemoryClient("memory", Measurement, measurements(4), capabilities=HISTORY)
    gateway = DataGateway([client])
    bus = InProcessBus()
    player = Player(gateway, bus, model=Measurement, paced=False, autoplay=True)
    pipeline = Pipeline("k", gateway, bus, player, [Doubler()])

    with bus.subscribe(NumberResult, overflow=Overflow.GROW) as results:
        await pipeline.start()
        got = await take(results, 4)
    assert [r.result.value for r in got] == [0.0, 2.0, 4.0, 6.0]
    assert pipeline.latest.get(NumberResult).result.value == 6.0
    assert pipeline.latest.get(Measurement).mRID == "m3"
    assert isinstance(pipeline.latest.get(PlayerStatus), PlayerStatus)
    assert pipeline.latest.get(Measurement).timestamp == at(3)
    await pipeline.stop()
    await pipeline.stop()  # idempotent
