# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""The PMU test streamer's pieces over the core -- no server started."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from fastapi import HTTPException

from pmu_test_streamer import api
from pmu_test_streamer import pipeline as streamer
from pmu_test_streamer.live_client import LIVE_STREAM_ID, LiveSyntheticClient
from pmu_test_streamer.sample_client import EPOCH, STREAM_ID, SampleRecordingClient, load_sample
from pmu_test_streamer.stats_module import FrameStats, FrameStatsModule, FrameStatsResult, ResetStatsCommand
from pswamp_core.bus import Overflow
from pswamp_core.command_routing import CommandRefused
from pswamp_core.datagateway import Capability, DataGateway
from pswamp_core.datagateway.clients import InMemoryClient
from pswamp_core.datagateway.conformance import DataClientConformance
from pswamp_core.messages import ErrorEvent, PlayCommand, PlayerStatus, PmuFrame, PmuHeader
from pswamp_core.pipeline import Pipeline
from pswamp_core.remote import ModuleHost, RemoteModule
from pswamp_core.transport import InMemoryTransport
from pswamp_core.util.time import utcnow


T0 = datetime(2026, 1, 1, tzinfo=timezone.utc)

TWO_STATIONS = PmuHeader(
    station=["a", "a", "a", "b", "b", "b"],
    channel=["V", "V", "f", "V", "V", "f"],
    measurement=["V_Magnitude", "V_Angle", "f"] * 2,
    units=["kV", "deg", "Hz"] * 2,
    data_rate=20.0,
)


# --- the module ------------------------------------------------------------------------


async def test_stats_module_computes_per_frame():
    module = FrameStatsModule()
    frame = PmuFrame(timestamp=T0, mRID="s", header=TWO_STATIONS, values=[400.0, 10.0, 50.0, 410.0, -5.0, 50.2])

    stats = await module.process(frame)

    assert stats.n_stations == 2
    assert stats.mean_frequency_hz == 50.1
    assert (stats.min_frequency_hz, stats.max_frequency_hz) == (50.0, 50.2)
    assert stats.angle_spread_deg == 15.0
    assert stats.mean_voltage_kv == 405.0
    assert FrameStatsResult.topic == "frame.stats.result"


async def test_the_module_primes_itself_from_the_frame_and_follows_a_layout_change():
    """What makes the module host-independent: the layout comes with the frame,
    so a fresh instance works from the first frame it sees, and a frame with a
    different layout re-primes it."""
    module = FrameStatsModule()
    await module.process(PmuFrame(timestamp=T0, mRID="s", header=TWO_STATIONS, values=[1.0] * 6))
    assert module.parameters["header_id"] == TWO_STATIONS.header_id

    other = PmuHeader(station=["z"], channel=["f"], measurement=["f"], units=["Hz"], data_rate=1.0)
    stats = await module.process(PmuFrame(timestamp=T0, mRID="z", header=other, values=[49.5]))
    assert stats.n_stations == 1 and stats.mean_frequency_hz == 49.5
    assert module.parameters == {"header_id": other.header_id, "stations": ["z"]}


# --- the sample file and its provider -----------------------------------------------


def test_sample_parses_into_one_header_and_sixty_frames():
    recording = load_sample()
    header, frames = recording.header, recording.frames

    assert len(frames) == 60
    assert header.stations == ["3000", "3245", "5100", "6500", "7000"]
    assert header.n_columns == 15
    assert header.data_rate == pytest.approx(20.0)
    assert header.measurement[:3] == ["V_Magnitude", "V_Angle", "f"]
    assert frames[0].timestamp == EPOCH + timedelta(seconds=0.05)
    assert all(f.header == header for f in frames)  # every frame carries the layout
    assert all(f.mRID == STREAM_ID for f in frames)
    assert frames[0].values[0] == pytest.approx(419.95)
    assert recording.coverage.end == frames[-1].timestamp + timedelta(seconds=0.05)


def test_sample_client_reads_lazily_and_from_env(monkeypatch, tmp_path):
    copy = tmp_path / "other.txt"
    copy.write_text("\n".join(load_sample().path.read_text().splitlines()[:10]))  # 2 instants
    monkeypatch.setenv("MINE_PATH", str(copy))
    monkeypatch.setenv("MINE_PRIORITY", "3")

    client = SampleRecordingClient.from_env("mine")

    assert (client.name, client.priority, len(client.frames)) == ("mine", 3, 2)
    assert client.capabilities == Capability.HISTORY_CONSUME


class TestSampleRecordingClientConformance(DataClientConformance):
    """A provider written outside the core, proving itself against the contract."""

    @pytest.fixture
    def client_under_test(self):
        return SampleRecordingClient()

    @pytest.fixture
    def conformance_model(self):
        return PmuFrame

    @pytest.fixture
    def conformance_records(self, client_under_test):
        return list(client_under_test.frames)


# --- the live provider --------------------------------------------------------------


class TestLiveSyntheticClientConformance(DataClientConformance):
    """The tail-only provider: the history cases skip, the live cases run."""

    @pytest.fixture
    def client_under_test(self):
        return LiveSyntheticClient()

    @pytest.fixture
    def conformance_model(self):
        return PmuFrame

    @pytest.fixture
    def conformance_records(self):
        return []


async def test_live_client_ticks_at_the_recording_rate_with_its_own_identity():
    client = LiveSyntheticClient()
    assert client.capabilities == Capability.LIVE_CONSUME
    await client.open()
    try:
        start = utcnow()
        stream = DataGateway([client]).consume(PmuFrame, start, start + timedelta(seconds=0.35))
        got = await asyncio.wait_for(_collect(stream), timeout=3)
    finally:
        await client.close()
    assert not client.ticking

    assert len(got) >= 3  # ~7 at 20 Hz; a lower bound, since runners jitter
    assert all(f.mRID == LIVE_STREAM_ID for f in got)
    assert all(f.header == load_sample().header for f in got)  # the live source describes itself
    stamps = [f.timestamp for f in got]
    assert all(start <= t < start + timedelta(seconds=0.35) for t in stamps)
    assert stamps == sorted(stamps)


async def _collect(stream) -> list[PmuFrame]:
    return [frame async for frame in stream]


#: The file the local k8s manifest mounts for the live feed (k8s/p-swamp-local.yaml).
K8S_EXAMPLE_FILE = Path(__file__).resolve().parents[3] / "k8s" / "deployment_pmu_data_file_example.txt"


def test_k8s_example_file_feeds_the_live_client(monkeypatch):
    """The deployment example: the live client re-pointed by ``LIVE_PATH`` at a
    file outside the image, keeping the recording's layout. Every value counts
    up by one per frame, the same in every station."""
    monkeypatch.setenv("LIVE_PATH", str(K8S_EXAMPLE_FILE))
    recording = LiveSyntheticClient.from_env("live").recording
    assert recording.header.header_id == load_sample().header.header_id
    for index, frame in enumerate(recording.frames):
        assert frame.values[:3] == [100.0 + index, 0.0 + index, 60.0 + index]


# --- a pipeline over the streamer's providers and module ------------------------------


def streamer_pipeline(key: str, modules) -> Pipeline:
    """The streamer's own pipeline definition, unpaced for the test."""
    pipeline = streamer.build_pipeline(key, modules)
    pipeline.player.paced = False
    return pipeline


async def test_pipeline_streams_frames_and_stats_onto_the_bus():
    pipeline = streamer_pipeline("42", [FrameStatsModule()])
    assert list(pipeline.gateway.clients) == ["sample", "live"]
    await pipeline.start()
    try:
        status = pipeline.player.status()
        assert status.mode == "replay" and status.can_seek and status.can_go_live
        with pipeline.bus.subscribe(PmuFrame, overflow=Overflow.GROW) as frames, pipeline.bus.subscribe(
            FrameStatsResult, overflow=Overflow.GROW
        ) as results:
            pipeline.player.resume()
            got = [await asyncio.wait_for(frames.get(), 2) for _ in range(3)]
            stats = await asyncio.wait_for(results.get(), 2)
        assert [f.timestamp for f in got] == [f.timestamp for f in load_sample().frames[:3]]
        assert stats.app.name == "frame-stats" and stats.result.n_stations == 5
        assert pipeline.latest.get(FrameStatsResult) is not None
    finally:
        await pipeline.stop()


async def test_the_same_pipeline_with_the_module_in_a_worker():
    """The module's stand-in in the pipeline, the worker's host beside it, one
    in-memory transport between: the same results land on the same bus -- and
    keep coming when the replay loops and its timestamps go backwards."""
    broker = InMemoryTransport()
    host = ModuleHost(FrameStatsModule, broker)
    host_task = asyncio.create_task(host.serve())
    remote = RemoteModule(FrameStatsModule, broker, "42")
    pipeline = streamer_pipeline("42", [remote])
    await pipeline.start()
    try:
        with pipeline.bus.subscribe(FrameStatsResult, overflow=Overflow.GROW) as results:
            pipeline.player.resume()
            got = [await asyncio.wait_for(results.get(), 2) for _ in range(3)]
            assert got[0].app.name == "frame-stats" and got[0].result.n_stations == 5
            assert host.keys() == ["42"]
            previous, wrapped = got[-1].timestamp, False
            deadline = asyncio.get_running_loop().time() + 5
            while not wrapped and asyncio.get_running_loop().time() < deadline:
                result = await asyncio.wait_for(results.get(), 2)
                wrapped, previous = result.timestamp < previous, result.timestamp
            assert wrapped
    finally:
        await pipeline.stop()
        host_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await host_task


# --- commands: one to the player, one to the module --------------------------------------


async def _reset_round_trip(pipeline: Pipeline) -> tuple[FrameStatsResult, FrameStatsResult]:
    """Play a few frames, reset, and return (the reset's answer, the next frame's result)."""
    bus = pipeline.bus
    with bus.subscribe(FrameStatsResult, overflow=Overflow.GROW) as results:
        pipeline.dispatch(PlayCommand())                    # routed to the player by class
        for _ in range(5):
            last = await asyncio.wait_for(results.get(), 2)
        assert last.result.frames_since_reset >= 5
        reset = ResetStatsCommand()
        pipeline.dispatch(reset)                            # routed to the module by class
        while True:
            answer = await asyncio.wait_for(results.get(), 2)
            if answer.request_id == reset.request_id:
                break
        after = await asyncio.wait_for(results.get(), 2)
    return answer, after


async def test_a_command_reaches_the_module_that_declared_it():
    module = FrameStatsModule()
    with pytest.raises(CommandRefused, match="nothing to reset"):
        module.validate(ResetStatsCommand())             # the edge's 409

    pipeline = streamer_pipeline("42", [module])
    assert [r.name for r in pipeline.receivers] == ["player", "frame-stats"]
    await pipeline.start()
    try:
        answer, after = await _reset_round_trip(pipeline)
    finally:
        await pipeline.stop()
    assert answer.result.frames_since_reset == 0 and answer.result.peak_angle_spread_deg is None
    assert after.result.frames_since_reset in (1, 2)


async def test_the_module_s_command_crosses_to_the_worker():
    broker = InMemoryTransport()
    host = ModuleHost(FrameStatsModule, broker)
    host_task = asyncio.create_task(host.serve())
    pipeline = streamer_pipeline("42", [RemoteModule(FrameStatsModule, broker, "42")])
    await pipeline.start()
    try:
        answer, _ = await _reset_round_trip(pipeline)
        assert answer.result.frames_since_reset == 0
    finally:
        await pipeline.stop()
        host_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await host_task


async def test_a_refusal_in_the_worker_comes_back_with_the_request_id():
    broker = InMemoryTransport()
    host = ModuleHost(FrameStatsModule, broker)
    host_task = asyncio.create_task(host.serve())
    pipeline = streamer_pipeline("7", [RemoteModule(FrameStatsModule, broker, "7")])
    await pipeline.start()
    try:
        with pipeline.bus.subscribe(ErrorEvent, overflow=Overflow.GROW) as errors:
            reset = ResetStatsCommand()
            pipeline.dispatch(reset)  # the stand-in accepts; the worker's module has seen no frame
            (error,) = [await asyncio.wait_for(errors.get(), 2)]
        assert (error.source, error.request_id) == ("frame-stats", reset.request_id)
        assert "nothing to reset" in error.detail
    finally:
        await pipeline.stop()
        host_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await host_task


# --- the CIM reference -------------------------------------------------------------------


async def test_the_gateway_stamps_the_cim_reference_and_the_module_hands_it_on(monkeypatch):
    monkeypatch.delenv(streamer.CIM_REFERENCE_VARIABLE, raising=False)
    pipeline = streamer_pipeline("42", [FrameStatsModule()])
    await pipeline.start()
    try:
        with pipeline.bus.subscribe(PmuFrame, FrameStatsResult, overflow=Overflow.GROW) as sub:
            pipeline.player.resume()
            frame = await asyncio.wait_for(sub.get(), 2)
            result = await asyncio.wait_for(sub.get(), 2)
    finally:
        await pipeline.stop()
    assert frame.header.cimReferenceId == streamer.DEFAULT_CIM_REFERENCE
    assert frame.header.header_id == load_sample().header.header_id  # the same layout
    assert result.result.cim_reference_id == streamer.DEFAULT_CIM_REFERENCE


def test_the_cim_reference_is_configured_or_switched_off(monkeypatch):
    monkeypatch.setenv(streamer.CIM_REFERENCE_VARIABLE, "none")
    assert streamer.cim_reference_enrichers() == []
    monkeypatch.setenv(streamer.CIM_REFERENCE_VARIABLE, "grid-2026")
    (enricher,) = streamer.cim_reference_enrichers()
    assert enricher.reference == "grid-2026"


# --- the web edge ---------------------------------------------------------------------------


@pytest.fixture
async def registry(monkeypatch):
    """The edge's own registry, bound to this loop, with the module in-process."""
    monkeypatch.delenv("PSWAMP_DATA_CLIENTS", raising=False)
    monkeypatch.delenv(streamer.MODULE_TRANSPORT_VARIABLE, raising=False)
    api.REGISTRY.bind(asyncio.get_running_loop())
    yield api.REGISTRY
    await api.REGISTRY.stop_all()
    api.REGISTRY.bind(None)


async def _wait_status(subscription, predicate, timeout: float = 2.0) -> PlayerStatus:
    async def _wait():
        while True:
            message = await subscription.get()
            if isinstance(message, PlayerStatus) and predicate(message):
                return message

    return await asyncio.wait_for(_wait(), timeout)


async def test_posts_become_commands_and_a_refusal_is_a_409(registry):
    pipeline = await registry.acquire("9")
    try:
        with pipeline.bus.subscribe(PlayerStatus, overflow=Overflow.GROW) as statuses:
            assert (await api.live("9")).applied == "go.live"
            await _wait_status(statuses, lambda s: s.mode == "live")
            with pytest.raises(HTTPException) as refused:
                await api.seek("9", api.SeekBody(offset_s=1.0))
            assert refused.value.status_code == 409 and "live mode" in refused.value.detail
            assert (await api.replay("9")).applied == "replay"
            await _wait_status(statuses, lambda s: s.mode == "replay")
            assert (await api.play("9")).applied == "play"
            assert (await api.stop("9")).applied == "pause"
        with pytest.raises(HTTPException) as refused:
            await api.reset_stats("9")  # nothing played yet: the module refuses
        assert refused.value.status_code == 409 and "nothing to reset" in refused.value.detail
        with pytest.raises(HTTPException) as missing:
            await api.play("no-such-client")  # a command never builds a pipeline
        assert missing.value.status_code == 404
    finally:
        registry.release("9")


async def test_the_socket_message_follows_the_switch_between_recorded_and_live(registry):
    pipeline = await registry.acquire("43")
    pipeline.player.paced = False
    try:
        message = api.state_message(pipeline)
        assert message.player.mode == "replay" and message.frame is None
        with pipeline.bus.subscribe(PmuFrame, FrameStatsResult, overflow=Overflow.GROW) as sub:
            await api.forward("43")
            await asyncio.wait_for(sub.get(), 2)
            await asyncio.wait_for(sub.get(), 2)
        message = api.state_message(pipeline)
        assert message.frame.mRID == STREAM_ID and (message.frame_index, message.frame_count) == (0, 60)
        assert message.stats is not None and message.stats.result.cim_reference_id == streamer.DEFAULT_CIM_REFERENCE

        with pipeline.bus.subscribe(PmuFrame, overflow=Overflow.GROW) as frames:
            await api.live("43")
            frame = await asyncio.wait_for(frames.get(), 3)
            while frame.mRID != LIVE_STREAM_ID:
                frame = await asyncio.wait_for(frames.get(), 3)
        message = api.state_message(pipeline)
        assert message.player.mode == "live" and message.frame_index is None

        await api.replay("43")
        await asyncio.sleep(0.05)
        message = api.state_message(pipeline)
        # Nothing has played on the replay yet: no live frame under a "recorded" badge.
        assert message.player.mode == "replay" and message.frame is None and message.stats is None
    finally:
        registry.release("43")


class TinyClient(InMemoryClient):
    """A stand-in provider, to prove the swap is configuration only."""

    def __init__(self, name: str):
        header = PmuHeader(station=["x"], channel=["f"], measurement=["f"], units=["Hz"], data_rate=1.0)
        frames = [
            PmuFrame(timestamp=EPOCH + timedelta(seconds=i), mRID="tiny", header=header, values=[50.0 + i])
            for i in range(3)
        ]
        super().__init__(name, [PmuFrame], frames, capabilities=Capability.HISTORY_CONSUME)


async def test_the_provider_is_swapped_by_environment_alone_and_live_is_then_refused(registry, monkeypatch):
    monkeypatch.setenv("PSWAMP_DATA_CLIENTS", "tiny:test_pmu_test_streamer:TinyClient")
    pipeline = await registry.acquire("11")
    try:
        assert list(pipeline.gateway.clients) == ["tiny"]
        assert pipeline.player.status().can_go_live is False
        with pytest.raises(HTTPException) as refused:
            await api.live("11")
        assert refused.value.status_code == 409 and "no live source" in refused.value.detail
    finally:
        registry.release("11")


def test_the_environment_sends_the_module_to_the_worker(monkeypatch):
    monkeypatch.delenv(streamer.MODULE_TRANSPORT_VARIABLE, raising=False)
    assert isinstance(streamer.stats_modules("1")[0], FrameStatsModule)
    monkeypatch.setenv(streamer.MODULE_TRANSPORT_VARIABLE, "mem:pswamp_core.transport:InMemoryTransport")
    (module,) = streamer.stats_modules("1")
    assert isinstance(module, RemoteModule)
    assert (module.name, module.key, module.output_model) == ("frame-stats", "1", FrameStatsResult)
    assert streamer.stats_modules("2")[0].transport is module.transport  # one per process
    asyncio.run(streamer.close_module_transport())


def test_state_keeps_stats_for_the_frame_or_the_one_just_before_it():
    """With the module in another process its result lands after the frame;
    the previous frame's stats are kept for that gap, and no longer."""
    frames = load_sample().frames
    identity = FrameStatsModule().identity
    body = FrameStats(
        n_stations=5, mean_frequency_hz=50.0, min_frequency_hz=50.0, max_frequency_hz=50.0,
        angle_spread_deg=0.0, mean_voltage_kv=400.0, frames_since_reset=1,
        peak_angle_spread_deg=0.0, cim_reference_id=None,
    )

    def stats_for(frame: PmuFrame) -> FrameStatsResult:
        return FrameStatsResult(timestamp=frame.timestamp, mRID=frame.mRID, app=identity, result=body)

    status = PlayerStatus(
        timestamp=utcnow(), mode="replay", cursor=None, speed=1.0, paused=True, loop=True, ended=False,
        can_seek=True, can_go_live=True, coverage_start=EPOCH, coverage_end=None, frame_interval_s=0.05,
    )
    assert api._current(stats_for(frames[3]), frames[3], status)
    assert api._current(stats_for(frames[2]), frames[3], status)  # one frame behind: kept
    assert not api._current(stats_for(frames[1]), frames[3], status)  # two behind: gone
    assert not api._current(stats_for(frames[4]), frames[3], status)  # from the future: gone
    live = frames[2].model_copy(update={"mRID": LIVE_STREAM_ID})
    assert not api._current(stats_for(live), frames[3], status)  # another stream: gone
    answer = stats_for(frames[0]).model_copy(update={"request_id": "r1"})
    assert api._current(answer, frames[3], status)  # a command's answer: shown
