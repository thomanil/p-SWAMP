# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""The streamer's providers, its module, and its pipeline -- no server started.

Four things: the sample file parses into the wire shape it should; both
providers pass the core's conformance suite (the worked examples of providers
written outside the core proving themselves against the contract -- one with
history, one that can only tail); the whole pipeline the endpoint builds runs,
so the frames and the module's results reach the bus and the recorded/live
switch works end to end; and the providers can be swapped by environment alone.
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
from pmu_test_streamer.stats_module import FrameStatsModule, FrameStatsResult
from pswamp_core.bus import Overflow
from pswamp_core.datagateway import Capability, DataGateway
from pswamp_core.datagateway.clients import InMemoryClient
from pswamp_core.datagateway.conformance import DataClientConformance
from pswamp_core.messages import Command, PlayerStatus, PmuFrame, PmuHeader
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
    assert all(f.header_id == header.header_id for f in frames)
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


async def test_header_is_served_as_its_own_model():
    gateway = DataGateway([SampleRecordingClient()])
    headers = [h async for h in gateway.consume(PmuHeader)]
    assert len(headers) == 1
    assert headers[0].header_id == load_sample().header.header_id
    assert await gateway.coverage(PmuFrame) is not None


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
    assert not client.supports(PmuHeader)  # it serves frames only; see the module docstring
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
    assert all(f.header_id == recording.header.header_id for f in got)
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
    file outside the image. It must keep the recording's channel layout (the live
    client serves no header, so the recording's describes its frames). Every
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
            pipeline.bus.publish(Command(client_id="42", verb="play"))
            got = [await asyncio.wait_for(frames.get(), 2) for _ in range(3)]
            stats = await asyncio.wait_for(results.get(), 2)
        assert [f.timestamp for f in got] == [f.timestamp for f in load_sample().frames[:3]]
        assert stats.app.name == "frame-stats"
        assert stats.result.n_stations == 5

        header = await api.stream_header(pipeline.gateway)
        message = api.state_message(pipeline, header, first=True)
        assert message.header is not None
        assert message.frame_count == 60
        assert message.frame_index is not None and 0 <= message.frame_index < 60
        assert message.player.mode == "replay" and message.player.can_seek
        assert message.player.can_go_live
        assert api.state_message(pipeline, header, first=False).header is None
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
        header = await api.stream_header(pipeline.gateway)
        with pipeline.bus.subscribe(PmuFrame, overflow=Overflow.GROW) as frames, pipeline.bus.subscribe(
            FrameStatsResult, overflow=Overflow.GROW
        ) as results, pipeline.bus.subscribe(PlayerStatus, overflow=Overflow.GROW) as statuses:
            pipeline.player.paced = False
            pipeline.bus.publish(Command(client_id="43", verb="play"))
            replayed = [await asyncio.wait_for(frames.get(), 2) for _ in range(3)]
            assert all(f.mRID == STREAM_ID for f in replayed)
            assert replayed[0].timestamp == EPOCH + timedelta(seconds=0.05)

            pipeline.bus.publish(Command(client_id="43", verb="live"))
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
            assert all(f.header_id == header.header_id for f in live_frames)
            while results.get_nowait() is not None:
                pass
            stats = await asyncio.wait_for(results.get(), 2)
            assert stats.result.n_stations == 5  # the module still works on live frames
            message = api.state_message(pipeline, header, first=False)
            assert message.player.mode == "live"
            assert message.frame_index is None and message.frame_count is None
            assert message.frame is not None and message.frame.mRID == LIVE_STREAM_ID
            assert message.stats is not None and message.stats.timestamp == message.frame.timestamp

            pipeline.bus.publish(Command(client_id="43", verb="replay"))
            status = await _wait_status(statuses, lambda s: s.mode == "replay")
            assert status.paused is True and status.can_seek is True
            assert status.cursor == EPOCH + timedelta(seconds=0.05)
            index, count = api._position(status, header)
            assert (index, count) == (0, 60)
            # Nothing has played on the replay yet: the page must not be shown
            # the live feed's last frame (or its stats) under a "recorded" badge.
            message = api.state_message(pipeline, header, first=False)
            assert message.frame is None and message.stats is None
            await pipeline.player.step()
            await asyncio.sleep(0.05)
            message = api.state_message(pipeline, header, first=False)
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
            ack = await api.dispatch("9", "live")
            assert ack.applied == "live"
            await _wait_status(statuses, lambda s: s.mode == "live")
            with pytest.raises(HTTPException) as refused:
                await api.dispatch("9", "seek", offset_s=1.0)
            assert refused.value.status_code == 409
            assert "live mode" in refused.value.detail
            with pytest.raises(HTTPException) as refused:
                await api.dispatch("9", "play")
            assert refused.value.status_code == 409
            ack = await api.dispatch("9", "replay")
            assert ack.applied == "replay"
            await _wait_status(statuses, lambda s: s.mode == "replay")
            assert (await api.dispatch("9", "play")).applied == "play"
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
            await api.dispatch("11", "live")
        assert refused.value.status_code == 409
    finally:
        api.REGISTRY.release("11")
        await api.REGISTRY.stop_all()
        api.REGISTRY.bind(None)


class TinyClient(InMemoryClient):
    """A stand-in provider, to prove the swap is configuration only."""

    def __init__(self, name: str):
        header = PmuHeader.build(
            timestamp=EPOCH, mRID="tiny", station=["x"], channel=["f"], measurement=["f"],
            units=["Hz"], data_rate=1.0,
        )
        frames = [
            PmuFrame(timestamp=EPOCH + timedelta(seconds=i), mRID="tiny", header_id=header.header_id, values=[50.0 + i])
            for i in range(3)
        ]
        super().__init__(name, [PmuHeader, PmuFrame], [header, *frames], capabilities=Capability.HISTORY_CONSUME)


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
