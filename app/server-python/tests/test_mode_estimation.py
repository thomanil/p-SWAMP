# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""The mode-estimation app: the copied N4SID as a module in each of its three
execution modes, the same module as its own service, and the pipeline end to end."""

from __future__ import annotations

import asyncio
import contextlib

import pytest

from islanding_stream.n44_client import EPOCH, N44RecordingClient
from mode_estimation import api
from mode_estimation.n4sid_module import ModeEstimationResult, N4SIDModule, WINDOW_SECONDS
from pswamp_core.bus import InProcessBus, Overflow
from pswamp_core.datagateway import DataGateway
from pswamp_core.messages import ErrorEvent
from pswamp_core.remote import ModuleHost, RemoteModule
from pswamp_core.transport import InMemoryTransport


@pytest.fixture(scope="module")
def recording():
    return N44RecordingClient(measurements=["f"])


async def bound(module: N4SIDModule) -> InProcessBus:
    bus = InProcessBus()
    bus.bind(asyncio.get_running_loop())
    await module.setup(DataGateway([]), bus)
    return bus


async def feed(module, recording, until_s, *, pace=0.0):
    """Frames up to ``until_s`` into ``module``; the results, with their offsets."""
    results = []
    for frame in recording.frames():
        offset = (frame.timestamp - EPOCH).total_seconds()
        if offset > until_s:
            break
        result = await module.process(frame)
        if result is not None:
            results.append((offset, result))
        if pace:
            await asyncio.sleep(pace)
    return results


async def test_inline_identifies_the_poorly_damped_modes_after_the_trip(recording):
    module = N4SIDModule("inline")
    await bound(module)
    results = await feed(module, recording, WINDOW_SECONDS + 1.1)
    assert [round(offset) for offset, _ in results] == [45, 46]
    modes = results[-1][1]
    assert modes.status == "Emergency" and modes.execution == "inline"
    freqs = sorted(m.freq_hz for m in modes.modes[:2])
    assert freqs[0] == pytest.approx(1.08, abs=0.03) and freqs[1] == pytest.approx(1.42, abs=0.03)
    assert all(m.damping < 0.03 for m in modes.modes[:2])
    assert len(modes.modes[0].stations) == 5 and modes.modes[0].participation[0] == 1.0
    assert modes.compute_ms > 10 and modes.queue_ms == 0 and modes.evaluations_skipped == 0
    assert modes.stations == 44 and modes.window_s == WINDOW_SECONDS


async def test_threaded_skips_what_falls_due_while_busy_and_says_so(recording):
    module = N4SIDModule("thread")
    bus = await bound(module)
    with bus.subscribe(ErrorEvent, overflow=Overflow.GROW) as errors:
        # Unpaced: the whole second half of the recording goes by while the
        # first identification runs, so every later one falls due while busy.
        results = await feed(module, recording, 70)
        (report,) = [errors.get_nowait()]
    assert module.evaluations == 26 and module.skipped == 25 and results == []
    assert report is not None and report.source == "n4sid"
    assert report.message.startswith("the n4sid analysis cannot identify every 1 s of data")
    assert "identifications skipped" in (report.detail or "")
    # The one that ran comes out with the next frame once it has finished.
    await asyncio.wait_for(asyncio.shield(module._pending.future), 10)
    after = await module.process(recording.frame(0))  # time goes back: resets, but still collects
    assert after is not None and after.status == "Emergency" and after.evaluations_skipped == 25
    assert after.latency_ms >= after.compute_ms > 0


async def test_threaded_keeps_up_when_paced_slower_than_it_computes(recording):
    module = N4SIDModule("thread")
    await bound(module)
    # 45 s fill unpaced, then never hand over a frame that makes the next
    # identification due while the previous one still runs. Paced on the
    # module's own progress rather than a fixed sleep, which a slow CI runner
    # (one identification well over 0.4 s) outran.
    await feed(module, recording, WINDOW_SECONDS - 0.02)
    results = []
    for index in range(int((WINDOW_SECONDS - 0.02) * 50) + 1, int((WINDOW_SECONDS + 3.1) * 50)):
        frame = recording.frame(index)
        due = module._next_eval is not None and frame.timestamp.timestamp() >= module._next_eval
        if due and module._pending is not None:
            await asyncio.wait_for(asyncio.shield(module._pending.future), 30)
        result = await module.process(frame)
        if result is not None:
            results.append(result)
    assert module.skipped == 0 and len(results) >= 2
    assert all(r.execution == "thread" and r.compute_cpu_ms > 0 for r in results)


def test_the_process_pool_runs_the_identification():
    # A process pool pickles the function by module path and the window by value.
    from mode_estimation.n4sid_module import _pool, identify

    client = N44RecordingClient(measurements=["f"])
    window = client._data[int(20 * 50) : int((20 + WINDOW_SECONDS) * 50)][:, client._columns]
    done = _pool("process").submit(identify, 0.02, window, 10, 10, "process").result(timeout=120)
    assert done.compute_ms > 0 and done.compute_cpu_ms > 0 and done.em_idx.any()


async def test_the_module_runs_as_its_own_service(recording):
    broker = InMemoryTransport()
    remote = RemoteModule(N4SIDModule, broker, "7")
    host = ModuleHost(lambda: N4SIDModule("inline"), broker)
    served = asyncio.create_task(host.serve())
    bus = InProcessBus()
    bus.bind(asyncio.get_running_loop())
    await remote.setup(DataGateway([]), bus)
    running = asyncio.create_task(remote.run(bus))
    await asyncio.sleep(0.01)
    try:
        with bus.subscribe(ModeEstimationResult, overflow=Overflow.GROW) as results:
            for index in range(int((WINDOW_SECONDS + 0.1) * 50)):
                bus.publish(recording.frame(index))
                if index % 25 == 0:
                    await asyncio.sleep(0.005)  # a queue of 64: let the worker side drain
            result = await asyncio.wait_for(results.get(), 30)
    finally:
        for task in (running, served):
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
    assert result.app.name == "n4sid" and result.result.status in ("OK", "Alert", "Emergency")
    assert result.result.input_age_s is not None


async def test_the_pipeline_state_carries_player_throughput_and_no_result_yet(monkeypatch):
    monkeypatch.delenv(api.DATA_CLIENTS_VARIABLE, raising=False)
    monkeypatch.delenv(api.MODULE_TRANSPORT_VARIABLE, raising=False)
    monkeypatch.setenv("MODES_N44_MEASUREMENTS", "f")
    monkeypatch.setattr(api, "TRANSPORT", None)
    pipeline = await api.build_pipeline("42")
    assert isinstance(pipeline.estimator, N4SIDModule)
    await pipeline.start()
    try:
        await asyncio.sleep(0.3)
        meter = api.RateMeter(pipeline)
        await asyncio.sleep(0.3)
        meter.read()
        message = api.state_message(pipeline, meter)
    finally:
        await pipeline.stop()
    assert message.result is None  # 45 s of data before the first identification
    assert message.throughput.module_runs == "in-process" and message.throughput.frames_per_s > 20
    assert message.duration_s == pytest.approx(70.02, abs=0.05)


def test_the_environment_sends_the_module_to_the_worker(monkeypatch):
    monkeypatch.setenv(api.MODULE_TRANSPORT_VARIABLE, "mem:pswamp_core.transport:InMemoryTransport")
    monkeypatch.setattr(api, "TRANSPORT", None)
    try:
        pipeline = asyncio.run(api.build_pipeline("9"))
        assert isinstance(pipeline.estimator, RemoteModule) and pipeline.estimator.name == "n4sid"
    finally:
        api.TRANSPORT = None


def test_execution_is_checked(monkeypatch):
    monkeypatch.setenv("MODE_ESTIMATION_EXECUTION", "gpu")
    with pytest.raises(ValueError):
        N4SIDModule()
