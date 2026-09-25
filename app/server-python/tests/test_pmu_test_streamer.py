# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""The streamer's providers, its module, and its pipeline -- no server started.

Five things: the sample file parses into the wire shape it should; both
providers pass the core's conformance suite (the worked examples of providers
written outside the core proving themselves against the contract -- one with
history, one that can only tail); the whole pipeline the endpoint builds runs,
so the frames and the module's results reach the bus and the recorded/live
switch works end to end; the providers can be swapped by environment alone;
and the stats module runs as its own service -- the worker's ``ModuleHost``
beside the pipeline's ``RemoteModule``, over the portless in-memory transport
-- with the same results landing on the same bus.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta
from pathlib import Path

import pytest
from fastapi import HTTPException

from pmu_test_streamer import api
from pmu_test_streamer.live_client import LIVE_STREAM_ID, LiveSyntheticClient
from pmu_test_streamer.sample_client import EPOCH, STREAM_ID, SampleRecordingClient, load_sample
from pmu_test_streamer.stats_module import FrameStats, FrameStatsModule, FrameStatsResult
from pswamp_core.bus import InProcessBus, Overflow
from pswamp_core.datagateway import Capability, DataGateway, Player, gateway_from_env
from pswamp_core.datagateway.clients import InMemoryClient
from pswamp_core.datagateway.conformance import DataClientConformance
from pswamp_core.messages import GoLiveCommand, PauseCommand, PlayCommand, PlayerStatus, PmuFrame, PmuHeader, ReplayCommand
from pswamp_core.pipeline import Pipeline
from pswamp_core.remote import ModuleHost, RemoteModule
from pswamp_core.transport import InMemoryTransport
from pswamp_core.util.time import utcnow

# --- the sample file ---------------------------------------------------------------


def test_sample_parses_into_one_header_and_sixty_frames():
    recording = load_sample()
    header, frames = recording.header, recording.frames

    assert len(frames) == 60
    assert header.stations == ["3000", "3245", "5100", "6500", "7000"]
    assert header.n_columns == 15
    assert header.data_rate == pytest.approx(20.0)
    assert header.measurement[:3] == ["V_Magnitude", "V_Angle", "f"]
    assert header.units[:3] == ["kV", "deg", "Hz"]
    assert frames[0].timestamp == EPOCH + timedelta(seconds=0.05)
    assert all(f.header == header for f in frames)  # every frame carries the layout
    assert all(f.mRID == STREAM_ID for f in frames)
    assert all(len(f.values) == 15 for f in frames)
    assert frames[0].values[0] == pytest.approx(419.95)
    assert recording.coverage.end == frames[-1].timestamp + timedelta(seconds=0.05)


def test_sample_client_reads_lazily_and_from_env(monkeypatch, tmp_path):
    copy = tmp_path / "other.txt"
    copy.write_text("\n".join(load_sample().path.read_text().splitlines()[:10]))  # 2 instants
    monkeypatch.setenv("MINE_PATH", str(copy))
    monkeypatch.setenv("MINE_PRIORITY", "3")

    client = SampleRecordingClient.from_env("mine")

    assert client.name == "mine"
    assert client.priority == 3
    assert len(client.frames) == 2
    assert client.capabilities == Capability.HISTORY_CONSUME


# --- the provider conformance suite --------------------------------------------------


class TestSampleRecordingClientConformance(DataClientConformance):
    @pytest.fixture
    def client_under_test(self):
        return SampleRecordingClient()

    @pytest.fixture
    def conformance_model(self):
        return PmuFrame

    @pytest.fixture
    def conformance_records(self, client_under_test):
        return list(client_under_test.frames)


async def test_frames_carry_the_layout_and_the_client_serves_nothing_else():
    client = SampleRecordingClient()
    gateway = DataGateway([client])
    assert client.supported_models == {PmuFrame}
    assert await gateway.coverage(PmuFrame) is not None
    frames = [f async for f in gateway.consume(PmuFrame)]
    assert len(frames) == 60 and all(f.header.header_id == load_sample().header.header_id for f in frames)


# --- the module ------------------------------------------------------------------------


async def test_stats_module_computes_per_frame():
    module = FrameStatsModule()
    recording = load_sample()
    module.use_header(recording.header)

    stats = await module.process(recording.frames[0])

    assert stats is not None
    assert stats.n_stations == 5
    assert 49.9 < stats.mean_frequency_hz < 50.1
    assert stats.min_frequency_hz <= stats.mean_frequency_hz <= stats.max_frequency_hz
    assert stats.angle_spread_deg >= 0
    assert stats.mean_voltage_kv > 0
    assert FrameStatsResult.topic == "frame.stats.result"


# --- the live provider -------------------------------------------------------------


class TestLiveSyntheticClientConformance(DataClientConformance):
    """The tail-only provider against the same suite: the history cases skip,
    the live cases run. No ticker runs, since nothing opens the client."""

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
        assert client.ticking
        start = utcnow()
        stream = DataGateway([client]).consume(PmuFrame, start, start + timedelta(seconds=0.35))
        got = await asyncio.wait_for(_collect(stream), timeout=3)
    finally:
        await client.close()
    assert not client.ticking

    recording = load_sample()
    assert len(got) >= 3  # ~7 at 20 Hz; a lower bound, since runners jitter
    assert all(f.mRID == LIVE_STREAM_ID for f in got)
    assert all(f.header == recording.header for f in got)  # the live source describes itself
    assert all(len(f.values) == recording.header.n_columns for f in got)
    stamps = [f.timestamp for f in got]
    assert all(start <= t < start + timedelta(seconds=0.35) for t in stamps)
    assert stamps == sorted(stamps)
    rows = [tuple(f.values) for f in recording.frames]
    assert all(tuple(f.values) in rows for f in got)


async def _collect(stream) -> list[PmuFrame]:
    return [frame async for frame in stream]


#: The file the local k8s manifest mounts for the live feed (k8s/p-swamp-local.yaml).
K8S_EXAMPLE_FILE = Path(__file__).resolve().parents[3] / "k8s" / "deployment_pmu_data_file_example.txt"


def test_k8s_example_file_feeds_the_live_client_with_constant_frames(monkeypatch):
    """The deployment example: the live client re-pointed by ``LIVE_PATH`` at a
    file outside the image. It keeps the recording's channel layout, which every
    frame it emits carries. Every
    value counts up by one per frame from a round start, the same in every
    station, so a page in live mode shows at a glance both that the configured
    source is what feeds it and that it is moving."""
    monkeypatch.setenv("LIVE_PATH", str(K8S_EXAMPLE_FILE))
    client = LiveSyntheticClient.from_env("live")
    recording = client.recording
    assert recording.header.header_id == load_sample().header.header_id
    assert len(recording.frames) == 60
    for index, frame in enumerate(recording.frames):
        assert all(len(set(frame.values[i::3])) == 1 for i in range(3))  # same in every station
        v, ang, f = frame.values[:3]
        assert (v, ang, f) == (100.0 + index, 0.0 + index, 60.0 + index)


# --- the pipeline the endpoint builds ----------------------------------------------


async def test_pipeline_streams_frames_and_stats_onto_the_bus(monkeypatch):
    monkeypatch.delenv("PSWAMP_DATA_CLIENTS", raising=False)
    pipeline = await api.build_pipeline("42")
    assert list(pipeline.gateway.clients) == ["sample", "live"]
    await pipeline.start()
    try:
        with pipeline.bus.subscribe(PmuFrame, overflow=Overflow.GROW) as frames, pipeline.bus.subscribe(
            FrameStatsResult, overflow=Overflow.GROW
        ) as results:
            pipeline.player.paced = False
            pipeline.dispatch(PlayCommand(client_id="42"))
            got = [await asyncio.wait_for(frames.get(), 2) for _ in range(3)]
            stats = await asyncio.wait_for(results.get(), 2)
        assert [f.timestamp for f in got] == [f.timestamp for f in load_sample().frames[:3]]
        assert stats.app.name == "frame-stats"
        assert stats.result.n_stations == 5

        message = api.state_message(pipeline)
        assert message.frame is not None and message.frame.header.n_columns == 15
        assert message.frame_count == 60
        assert message.frame_index is not None and 0 <= message.frame_index < 60
        assert message.player.mode == "replay" and message.player.can_seek
        assert message.player.can_go_live
    finally:
        await pipeline.stop()


async def test_default_pipeline_replays_then_goes_live_then_returns(monkeypatch):
    """The switch, end to end over the composed default: the replay is the
    recording at its epoch, live is the same layout stamped now, and replay
    lands back at the start, paused."""
    monkeypatch.delenv("PSWAMP_DATA_CLIENTS", raising=False)
    pipeline = await api.build_pipeline("43")
    await pipeline.start()
    try:
        header = load_sample().header
        with pipeline.bus.subscribe(PmuFrame, overflow=Overflow.GROW) as frames, pipeline.bus.subscribe(
            FrameStatsResult, overflow=Overflow.GROW
        ) as results, pipeline.bus.subscribe(PlayerStatus, overflow=Overflow.GROW) as statuses:
            pipeline.player.paced = False
            pipeline.dispatch(PlayCommand(client_id="43"))
            replayed = [await asyncio.wait_for(frames.get(), 2) for _ in range(3)]
            assert all(f.mRID == STREAM_ID for f in replayed)
            assert replayed[0].timestamp == EPOCH + timedelta(seconds=0.05)

            pipeline.dispatch(GoLiveCommand(client_id="43"))
            status = await _wait_status(statuses, lambda s: s.mode == "live")
            assert status.paused is False and status.can_seek is False and status.can_go_live
            # Drain whatever the replay still had queued, then read live frames.
            live_frames = []
            deadline = asyncio.get_running_loop().time() + 3
            while len(live_frames) < 3 and asyncio.get_running_loop().time() < deadline:
                frame = await asyncio.wait_for(frames.get(), 2)
                if frame.mRID == LIVE_STREAM_ID:
                    live_frames.append(frame)
            assert len(live_frames) == 3
            assert all(abs((utcnow() - f.timestamp).total_seconds()) < 2 for f in live_frames)
            assert all(f.header == header for f in live_frames)
            while results.get_nowait() is not None:
                pass
            stats = await asyncio.wait_for(results.get(), 2)
            assert stats.result.n_stations == 5  # the module still works on live frames
            message = api.state_message(pipeline)
            assert message.player.mode == "live"
            assert message.frame_index is None and message.frame_count is None
            assert message.frame is not None and message.frame.mRID == LIVE_STREAM_ID
            assert message.stats is not None and message.stats.timestamp == message.frame.timestamp

            pipeline.dispatch(ReplayCommand(client_id="43"))
            status = await _wait_status(statuses, lambda s: s.mode == "replay")
            assert status.paused is True and status.can_seek is True
            assert status.cursor == EPOCH + timedelta(seconds=0.05)
            index, count = api._position(status, load_sample().frames[0])
            assert (index, count) == (0, 60)
            # Nothing has played on the replay yet: the page must not be shown
            # the live feed's last frame (or its stats) under a "recorded" badge.
            message = api.state_message(pipeline)
            assert message.frame is None and message.stats is None
            await pipeline.player.step()
            await asyncio.sleep(0.05)
            message = api.state_message(pipeline)
            assert message.frame is not None and message.frame.mRID == STREAM_ID
    finally:
        await pipeline.stop()
    assert not pipeline.gateway.clients["live"].ticking  # type: ignore[attr-defined]


async def _wait_status(subscription, predicate, timeout: float = 2.0) -> PlayerStatus:
    async def _wait():
        while True:
            message = await subscription.get()
            if predicate(message):
                return message

    return await asyncio.wait_for(_wait(), timeout)


async def test_dispatch_answers_409_for_a_verb_the_mode_refuses(monkeypatch):
    monkeypatch.delenv("PSWAMP_DATA_CLIENTS", raising=False)
    api.REGISTRY.bind(asyncio.get_running_loop())
    pipeline = await api.REGISTRY.acquire("9")
    try:
        with pipeline.bus.subscribe(PlayerStatus, overflow=Overflow.GROW) as statuses:
            ack = await api.live("9")
            assert ack.applied == "go.live"
            await _wait_status(statuses, lambda s: s.mode == "live")
            with pytest.raises(HTTPException) as refused:
                await api.seek("9", api.SeekBody(offset_s=1.0))
            assert refused.value.status_code == 409
            assert "live mode" in refused.value.detail
            with pytest.raises(HTTPException) as refused:
                await api.play("9")
            assert refused.value.status_code == 409
            ack = await api.replay("9")
            assert ack.applied == "replay"
            await _wait_status(statuses, lambda s: s.mode == "replay")
            assert (await api.play("9")).applied == "play"
            assert (await api.stop("9")).applied == "pause"
        with pytest.raises(HTTPException) as missing:
            await api.play("no-such-client")
        assert missing.value.status_code == 404
    finally:
        api.REGISTRY.release("9")
        await api.REGISTRY.stop_all()
        api.REGISTRY.bind(None)


async def test_dispatch_refuses_live_without_a_live_source(monkeypatch):
    monkeypatch.setenv("PSWAMP_DATA_CLIENTS", "tiny:test_pmu_test_streamer:TinyClient")
    api.REGISTRY.bind(asyncio.get_running_loop())
    pipeline = await api.REGISTRY.acquire("11")
    try:
        assert pipeline.player.status().can_go_live is False
        with pytest.raises(HTTPException) as refused:
            await api.live("11")
        assert refused.value.status_code == 409
        assert "no live source" in refused.value.detail
    finally:
        api.REGISTRY.release("11")
        await api.REGISTRY.stop_all()
        api.REGISTRY.bind(None)


class TinyClient(InMemoryClient):
    """A stand-in provider, to prove the swap is configuration only."""

    def __init__(self, name: str):
        header = PmuHeader(station=["x"], channel=["f"], measurement=["f"], units=["Hz"], data_rate=1.0)
        frames = [
            PmuFrame(timestamp=EPOCH + timedelta(seconds=i), mRID="tiny", header=header, values=[50.0 + i])
            for i in range(3)
        ]
        super().__init__(name, [PmuFrame], frames, capabilities=Capability.HISTORY_CONSUME)


async def test_provider_is_swapped_by_environment_alone(monkeypatch):
    monkeypatch.setenv("PSWAMP_DATA_CLIENTS", "tiny:test_pmu_test_streamer:TinyClient")
    pipeline = await api.build_pipeline("7")
    assert list(pipeline.gateway.clients) == ["tiny"]
    await pipeline.start()
    try:
        with pipeline.bus.subscribe(PmuFrame, overflow=Overflow.GROW) as frames:
            pipeline.player.paced = False
            pipeline.player.resume()
            got = [await asyncio.wait_for(frames.get(), 2) for _ in range(3)]
        assert [f.values[0] for f in got] == [50.0, 51.0, 52.0]
        status = pipeline.latest.get(PlayerStatus)
        assert status is not None and status.coverage_start == EPOCH
        assert status.can_go_live is False
    finally:
        await pipeline.stop()


def test_state_keeps_stats_for_the_frame_or_the_one_just_before_it():
    """With the module in another process its result lands after the frame;
    the previous frame's stats are kept for that gap, and no longer."""
    frames = load_sample().frames
    identity = FrameStatsModule().identity
    body = FrameStats(
        n_stations=5, mean_frequency_hz=50.0, min_frequency_hz=50.0, max_frequency_hz=50.0,
        angle_spread_deg=0.0, mean_voltage_kv=400.0,
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
    assert not api._current(None, frames[3], status)


# --- the module as its own service ---------------------------------------------------


async def test_the_module_primes_itself_from_the_frame_and_follows_a_layout_change():
    """What makes the module host-independent: it needs nothing before its
    first frame. The layout comes with the frame, so a fresh instance -- in a
    worker that started late, say -- works from the first frame it sees, and
    a frame with a different layout re-primes it."""
    module = FrameStatsModule()
    recording = load_sample()
    stats = await module.process(recording.frames[0])
    assert stats is not None and stats.n_stations == 5
    assert module.parameters["header_id"] == recording.header.header_id

    other = PmuHeader(station=["z"], channel=["f"], measurement=["f"], units=["Hz"], data_rate=1.0)
    stats = await module.process(PmuFrame(timestamp=EPOCH, mRID="z", header=other, values=[49.5]))
    assert stats is not None and stats.n_stations == 1 and stats.mean_frequency_hz == 49.5
    assert module.parameters == {"header_id": other.header_id, "stations": ["z"]}


def test_environment_sends_the_module_to_the_worker(monkeypatch):
    monkeypatch.setattr(api, "TRANSPORT", None)
    monkeypatch.delenv(api.MODULE_TRANSPORT_VARIABLE, raising=False)
    assert isinstance(api.stats_modules("1")[0], FrameStatsModule)
    monkeypatch.setattr(api, "TRANSPORT", None)
    monkeypatch.setenv(api.MODULE_TRANSPORT_VARIABLE, "mem:pswamp_core.transport:InMemoryTransport")
    (module,) = api.stats_modules("1")
    assert isinstance(module, RemoteModule)
    assert (module.name, module.key, module.output_model) == ("frame-stats", "1", FrameStatsResult)
    assert api.stats_modules("2")[0].transport is module.transport  # one per process
    monkeypatch.setattr(api, "TRANSPORT", None)


async def test_stats_module_runs_as_its_own_service_over_the_transport(monkeypatch):
    """The streamer's pipeline with the module's stand-in, and the worker's
    host running the real module beside it, over one in-memory transport:
    the results land on the pipeline's bus as they do in-process -- and keep
    coming when the replay loops and its timestamps go backwards."""
    monkeypatch.delenv("PSWAMP_DATA_CLIENTS", raising=False)
    broker = InMemoryTransport()
    host = ModuleHost(FrameStatsModule, broker)
    host_task = asyncio.create_task(host.serve())
    gateway = gateway_from_env(api.DEFAULT_DATA_CLIENTS)
    bus = InProcessBus()
    player = Player(gateway, bus, model=PmuFrame, loop=True)
    remote = RemoteModule(FrameStatsModule, broker, "42")
    pipeline = Pipeline("42", gateway, bus, player, [remote])
    await pipeline.start()
    try:
        with bus.subscribe(PmuFrame, overflow=Overflow.GROW) as frames, bus.subscribe(
            FrameStatsResult, overflow=Overflow.GROW
        ) as results:
            pipeline.player.paced = False
            pipeline.dispatch(PlayCommand(client_id="42"))
            played = [await asyncio.wait_for(frames.get(), 2) for _ in range(3)]
            got = [await asyncio.wait_for(results.get(), 2) for _ in range(3)]
            assert [r.timestamp for r in got] == [f.timestamp for f in played]
            assert got[0].app.name == "frame-stats" and got[0].result.n_stations == 5
            assert host.keys() == ["42"]

            # The replay loops at its 60th frame: timestamps go back to the
            # epoch, and the transport carries them regardless.
            previous = got[-1].timestamp
            wrapped = False
            deadline = asyncio.get_running_loop().time() + 5
            while not wrapped and asyncio.get_running_loop().time() < deadline:
                result = await asyncio.wait_for(results.get(), 2)
                wrapped = result.timestamp < previous
                previous = result.timestamp
            assert wrapped
            assert remote.published > 60 and remote.dropped == 0 and remote.received > 60

        pipeline.dispatch(PauseCommand(client_id="42"))
        await asyncio.sleep(0.05)
        message = api.state_message(pipeline)
        assert message.player.mode == "replay" and message.frame is not None
    finally:
        await pipeline.stop()
        host_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await host_task
    assert host.keys() == []
