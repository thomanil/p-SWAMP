# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""The islanding-stream app: the N44 provider, the copied detector as a module,
the same module as its own service, and the pipeline end to end."""

from __future__ import annotations

import asyncio
import contextlib
from datetime import timedelta

import pytest

from errors.forwarder import ErrorForwarderModule
from islanding_stream import api
from islanding_stream.islanding_module import IslandingModule, IslandingStreamResult
from islanding_stream.n44_client import EPOCH, N44RecordingClient
from pswamp_core.bus import InProcessBus, Overflow
from pswamp_core.datagateway import CimReferenceEnricher, DataGateway
from pswamp_core.datagateway.conformance import DataClientConformance
from pswamp_core.messages import PmuFrame
from pswamp_core.remote import ModuleHost, RemoteModule
from pswamp_core.transport import InMemoryTransport

#: The stations the line trip at 20 s separates from the main system.
ISLANDED = {"6500", "6700", "6701"}


class TestN44RecordingClientConformance(DataClientConformance):
    """Over the frequency columns only, so the suite handles 3501 frames of 44
    values rather than of 700; the full layout is checked below."""

    @pytest.fixture
    def client_under_test(self):
        return N44RecordingClient(measurements=["f"])

    @pytest.fixture
    def conformance_model(self):
        return PmuFrame

    @pytest.fixture
    def conformance_records(self, client_under_test):
        return list(client_under_test.frames())


def test_the_full_recording_carries_all_700_channels_with_units():
    client = N44RecordingClient()
    frame = client.frame(0)
    assert frame.header.n_columns == 700 and len(frame.values) == 700
    assert len(frame.header.columns(measurement="f")) == 44
    assert set(frame.header.units) == {"Hz", "Hz/s", "V", "rad", "A"}
    assert frame.header.data_rate == 50.0
    assert client.n_frames == 3501


def test_measurements_select_the_columns_from_the_environment(monkeypatch):
    monkeypatch.setenv("N44_MEASUREMENTS", "f,Df")
    client = N44RecordingClient.from_env("n44")
    assert set(client.header.measurement) == {"f", "Df"} and client.header.n_columns == 88


async def run_module(module: IslandingModule, client: N44RecordingClient, until_s: float):
    """Feed ``module`` the recording up to ``until_s``; the results, by offset."""
    results = {}
    for frame in client.frames():
        offset = (frame.timestamp - EPOCH).total_seconds()
        if offset > until_s:
            break
        result = await module.process(frame)
        if result is not None:
            results[round(offset)] = result
    return results


async def test_the_detector_finds_the_islands_the_line_trip_makes():
    results = await run_module(IslandingModule(), N44RecordingClient(measurements=["f"]), 35)
    # No result until the 10 s window has filled, then one per data-second.
    assert min(results) >= 10
    before, after = results[19], results[30]
    assert before.status == "OK" and before.islands == [] and before.main_system == 44
    assert after.status == "Emergency"
    assert {station for island in after.islands for station in island} == ISLANDED
    assert after.main_system == 41 and after.stations == 44
    # The groups are a partition: no station reported twice.
    listed = [s for island in after.islands for s in island]
    assert len(listed) == len(set(listed))
    assert after.frames_in == 50 and after.input_age_s is None


async def test_the_window_restarts_when_time_goes_backwards():
    client = N44RecordingClient(measurements=["f"])
    module = IslandingModule()
    await run_module(module, client, 15)
    # A loop: back to the start. No result until a fresh window has filled.
    assert await module.process(client.frame(0)) is None
    early = [await module.process(client.frame(i)) for i in range(1, 400)]
    assert all(r is None for r in early)


async def test_the_module_runs_as_its_own_service_with_the_same_result():
    broker = InMemoryTransport()
    client = N44RecordingClient(measurements=["f"])
    remote = RemoteModule(IslandingModule, broker, "7")
    host = ModuleHost(IslandingModule, broker)
    served = asyncio.create_task(host.serve())
    bus = InProcessBus()
    bus.bind(asyncio.get_running_loop())
    await remote.setup(DataGateway([]), bus)
    running = asyncio.create_task(remote.run(bus))
    await asyncio.sleep(0.01)
    try:
        with bus.subscribe(IslandingStreamResult, overflow=Overflow.GROW) as results:
            for index in range(int(30 * 50)):  # 30 s of recording, as fast as it goes
                bus.publish(client.frame(index))
                if index % 50 == 0:
                    await asyncio.sleep(0)  # let the two sides drain
            await asyncio.sleep(0.2)
            got = []
            while (result := results.get_nowait()) is not None:
                got.append(result)
    finally:
        running.cancel()
        served.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await running
        with contextlib.suppress(asyncio.CancelledError):
            await served
    assert got, "no result came back from the worker side"
    last = got[-1]
    assert last.app.name == "islanding" and last.result.status == "Emergency"
    assert {s for island in last.result.islands for s in island} == ISLANDED
    # Crossed a transport: the module could tell how old its input was.
    assert last.result.input_age_s is not None


async def test_the_pipeline_autoplays_and_the_socket_state_carries_result_and_throughput(monkeypatch):
    monkeypatch.delenv(api.DATA_CLIENTS_VARIABLE, raising=False)
    monkeypatch.delenv(api.MODULE_TRANSPORT_VARIABLE, raising=False)
    monkeypatch.setenv("N44_MEASUREMENTS", "f")
    monkeypatch.setattr(api, "TRANSPORT", None)
    pipeline = await api.build_pipeline("42")
    assert isinstance(pipeline.islanding, IslandingModule)
    await pipeline.start()
    try:
        assert not pipeline.player.status().paused
        pipeline.bus.publish(api.Command(client_id="42", verb="speed", args={"speed": 50}))
        meter = api.RateMeter(pipeline)
        with pipeline.bus.subscribe(IslandingStreamResult, overflow=Overflow.GROW) as results:
            result = await asyncio.wait_for(results.get(), 5)
        await asyncio.sleep(0.25)
        meter.read()
        message = api.state_message(pipeline, meter)
    finally:
        await pipeline.stop()
    assert result.app.name == "islanding"
    assert message.player.speed == 50
    assert message.result is not None and message.duration_s == pytest.approx(70.02, abs=0.05)
    assert message.throughput.module_runs == "in-process"
    assert message.throughput.frames_per_s > 50 and message.throughput.published is None
    assert pipeline.frames_emitted > 500


def test_the_environment_sends_the_module_to_the_worker(monkeypatch):
    monkeypatch.setenv(api.MODULE_TRANSPORT_VARIABLE, "mem:pswamp_core.transport:InMemoryTransport")
    monkeypatch.setattr(api, "TRANSPORT", None)
    try:
        pipeline = asyncio.run(api.build_pipeline("9"))
        assert isinstance(pipeline.islanding, RemoteModule)
        assert pipeline.islanding.name == "islanding" and pipeline.islanding.key == "9"
    finally:
        api.TRANSPORT = None


def test_the_speed_command_is_bounded_by_the_contract():
    assert api.SpeedBody(speed=50).speed == 50
    with pytest.raises(ValueError):
        api.SpeedBody(speed=51)


def test_the_error_forwarder_never_reports_itself():
    assert ErrorForwarderModule.keep_up is None


def test_coverage_is_the_recording_plus_one_interval():
    client = N44RecordingClient(measurements=["f"])
    assert client.time_range.end - client.time_range.start == timedelta(seconds=70.01 - 0.01 + 0.02)


# --- the cimReferenceId, stamped in the gateway and read by the module ------------------


def stamped_frames(reference: str = api.DEFAULT_CIM_REFERENCE):
    """The recording's frequency frames as the gateway hands them on: each
    header stamped by the stub enricher."""
    enricher = CimReferenceEnricher(reference)
    return (enricher.enrich(frame) for frame in N44RecordingClient(measurements=["f"]).frames())


async def test_the_pipeline_stamps_the_reference_early_and_the_module_picks_it_up(monkeypatch):
    monkeypatch.delenv(api.DATA_CLIENTS_VARIABLE, raising=False)
    monkeypatch.delenv(api.CIM_REFERENCE_VARIABLE, raising=False)
    monkeypatch.delenv(api.MODULE_TRANSPORT_VARIABLE, raising=False)
    monkeypatch.setenv("N44_MEASUREMENTS", "f")
    monkeypatch.setattr(api, "TRANSPORT", None)
    pipeline = await api.build_pipeline("77")
    await pipeline.start()
    try:
        with pipeline.bus.subscribe(PmuFrame, overflow=Overflow.GROW) as frames:
            frame = await asyncio.wait_for(frames.get(), 5)
    finally:
        await pipeline.stop()
    # Stamped by the gateway, so every frame on the bus already carries it.
    assert frame.header.cimReferenceId == api.DEFAULT_CIM_REFERENCE

    # Later in the pipeline, the module reads it off the frames it evaluated.
    results = {}
    module = IslandingModule()
    for f in stamped_frames():
        if (f.timestamp - EPOCH).total_seconds() > 30:
            break
        if (result := await module.process(f)) is not None:
            results[round((f.timestamp - EPOCH).total_seconds())] = result
    assert results[30].status == "Emergency" and results[30].cim_reference_id == api.DEFAULT_CIM_REFERENCE


async def test_switched_off_the_reference_is_none(monkeypatch):
    monkeypatch.delenv(api.DATA_CLIENTS_VARIABLE, raising=False)
    monkeypatch.setenv(api.CIM_REFERENCE_VARIABLE, "none")
    monkeypatch.setenv("N44_MEASUREMENTS", "f")
    monkeypatch.setattr(api, "TRANSPORT", None)
    pipeline = await api.build_pipeline("78")
    await pipeline.start()
    try:
        with pipeline.bus.subscribe(PmuFrame, overflow=Overflow.GROW) as frames:
            frame = await asyncio.wait_for(frames.get(), 5)
    finally:
        await pipeline.stop()
    assert frame.header.cimReferenceId is None
    results = await run_module(IslandingModule(), N44RecordingClient(measurements=["f"]), 30)
    assert results[30].status == "Emergency" and results[30].cim_reference_id is None


async def test_the_reference_crosses_the_worker_hop_with_the_frames():
    broker = InMemoryTransport()
    remote = RemoteModule(IslandingModule, broker, "8")
    host = ModuleHost(IslandingModule, broker)  # no configuration: the reference rides in the frame
    served = asyncio.create_task(host.serve())
    bus = InProcessBus()
    bus.bind(asyncio.get_running_loop())
    await remote.setup(DataGateway([]), bus)
    running = asyncio.create_task(remote.run(bus))
    await asyncio.sleep(0.01)
    try:
        with bus.subscribe(IslandingStreamResult, overflow=Overflow.GROW) as results:
            for index, frame in enumerate(stamped_frames("worker-hop-ref")):
                if index >= 30 * 50:
                    break
                bus.publish(frame)
                if index % 50 == 0:
                    await asyncio.sleep(0)
            await asyncio.sleep(0.2)
            got = []
            while (result := results.get_nowait()) is not None:
                got.append(result)
    finally:
        for task in (running, served):
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
    assert got, "no result came back from the worker side"
    assert {s for island in got[-1].result.islands for s in island} == ISLANDED
    assert got[-1].result.cim_reference_id == "worker-hop-ref"
