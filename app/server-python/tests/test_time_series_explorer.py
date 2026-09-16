# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""The Time Series Explorer: the row-count module, the pipeline the endpoint
builds, the two commands end to end, the refusals -- and the provider swapped
by environment for the time-series client over the in-process stub."""

from __future__ import annotations

import asyncio
from datetime import timedelta

import httpx
import pytest
from fastapi import HTTPException

from errors import HUB
from pmu_test_streamer.sample_client import SampleRecordingClient, load_sample
from pswamp_core.bus import InProcessBus, Overflow
from pswamp_core.datagateway import Capability, DataGateway
from pswamp_core.datagateway.clients import InMemoryClient
from pswamp_core.datagateway.clients.time_series_database import (
    InMemoryResultFeed,
    TimeSeriesDatabaseClient,
)
from pswamp_core.messages import Command, ErrorEvent, PlayerStatus, PmuFrame, PmuHeader
from time_series_explorer import api
from time_series_explorer.row_count_module import RowCountModule, RowCountResult
from time_series_stub.app import create_app
from time_series_stub.recording import TiledRecording
from time_series_stub.service import QueryService

ONE_SECOND = timedelta(seconds=1)


class FailsAfterTwo(InMemoryClient):
    """A history client whose stream dies after two records."""

    def __init__(self, name: str = "flaky") -> None:
        recording = load_sample()
        super().__init__(
            name, [PmuHeader, PmuFrame], [recording.header, *recording.frames],
            capabilities=Capability.HISTORY_CONSUME,
        )

    async def consume(self, model, time_range, mRID=None):
        sent = 0
        async for record in super().consume(model, time_range, mRID):
            if sent == 2 and model is PmuFrame:
                raise TimeoutError("the store went quiet")
            sent += 1
            yield record


# --- the module ------------------------------------------------------------------------


async def test_row_count_module_counts_a_range_and_reports_a_failure():
    t0 = load_sample().frames[0].timestamp
    module = RowCountModule()
    await module.setup(DataGateway([SampleRecordingClient()]), InProcessBus())
    result = await module.count(t0, t0 + ONE_SECOND)
    assert (result.count, result.error) == (20, None) and result.elapsed_s >= 0
    assert (await module.count(t0 + timedelta(seconds=10), t0 + timedelta(seconds=11))).count == 0
    assert RowCountResult.topic == "row.count.result"
    assert RowCountModule.target == "row-count"

    flaky = RowCountModule()
    await flaky.setup(DataGateway([FailsAfterTwo()]), InProcessBus())
    failed = await flaky.count(t0, t0 + ONE_SECOND)
    assert failed.count == 2 and failed.error == "TimeoutError: the store went quiet"
    bad = await flaky.count_from_args({"start": "not a time"})
    assert bad.error is not None and bad.error.startswith("bad range")

    class Unreachable(SampleRecordingClient):
        async def coverage(self, model, mRID=None):
            raise ConnectionError("refused")

    down = RowCountModule()
    await down.setup(DataGateway([Unreachable()]), InProcessBus())
    unreachable = await down.count(t0, t0 + ONE_SECOND)
    assert unreachable.count == 0 and "no coverage" in (unreachable.error or "")


async def test_the_module_answers_a_command_with_its_request_id_and_raises_an_error_event():
    bus = InProcessBus()
    bus.bind(asyncio.get_running_loop())
    module = RowCountModule()
    await module.setup(DataGateway([FailsAfterTwo()]), bus)
    task = asyncio.create_task(module.run(bus))
    await asyncio.sleep(0)
    t0 = load_sample().frames[0].timestamp
    try:
        with bus.subscribe(RowCountResult) as results, bus.subscribe(ErrorEvent) as errors:
            bus.publish(Command(target="player", verb="count"))  # not ours
            bus.publish(Command(target="row-count", verb="play"))  # not our verb
            command = Command(target="row-count", verb="count", args={"start": t0.isoformat(), "end": (t0 + ONE_SECOND).isoformat()})
            bus.publish(command)
            result = await asyncio.wait_for(results.get(), 2)
            event = await asyncio.wait_for(errors.get(), 2)
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    assert result.request_id == command.request_id and result.result.count == 2
    assert result.result.error is not None and result.app.name == "row-count"
    assert event.request_id == command.request_id and event.source == "row-count"
    assert event.detail == result.result.error


# --- the pipeline the endpoint builds --------------------------------------------------


async def test_pipeline_counts_a_range_and_plays_a_bounded_range(monkeypatch):
    monkeypatch.delenv(api.DATA_CLIENTS_VARIABLE, raising=False)
    pipeline = await api.build_pipeline("21")
    assert list(pipeline.gateway.clients) == ["sample"]
    assert [m.name for m in pipeline.modules] == ["row-count", "error-forwarder"]
    await pipeline.start()
    try:
        header = await api.stream_header(pipeline.gateway)
        opening = api.state_message(pipeline, header, first=True)
        assert opening.header is not None and opening.frame is None and opening.count is None
        assert opening.player.mode == "replay" and opening.player.paused and opening.player.loop is False
        t0 = opening.player.coverage_start
        assert t0 is not None and opening.player.coverage_end == t0 + timedelta(seconds=3.0)

        with pipeline.bus.subscribe(RowCountResult, overflow=Overflow.GROW) as results, pipeline.bus.subscribe(
            PmuFrame, overflow=Overflow.GROW
        ) as frames, pipeline.bus.subscribe(PlayerStatus, overflow=Overflow.GROW) as statuses:
            pipeline.player.paced = False
            command = Command(client_id="21", target="row-count", verb="count",
                              args={"start": t0.isoformat(), "end": (t0 + ONE_SECOND).isoformat()})
            pipeline.bus.publish(command)
            result = await asyncio.wait_for(results.get(), 2)
            assert result.result.count == 20 and result.request_id == command.request_id
            message = api.state_message(pipeline, header, first=False)
            assert message.header is None and message.count is not None and message.count.result.count == 20
            assert message.frame is None  # a count plays nothing

            pipeline.bus.publish(Command(client_id="21", verb="replay", args={
                "to": (t0 + ONE_SECOND).isoformat(), "end": (t0 + timedelta(seconds=1.25)).isoformat(), "play": True,
            }))
            got = [await asyncio.wait_for(frames.get(), 2) for _ in range(5)]
            ended = await _wait_status(statuses, lambda s: s.ended)
            assert [f.timestamp for f in got] == [t0 + timedelta(seconds=1.0 + 0.05 * i) for i in range(5)]
            assert ended.paused and ended.range_end == t0 + timedelta(seconds=1.25) and ended.error is None
            message = api.state_message(pipeline, header, first=False)
            assert message.frame is not None and message.frame.timestamp == got[-1].timestamp
            assert message.count is not None  # the last count is kept alongside
    finally:
        await pipeline.stop()


async def test_commands_are_refused_outside_the_coverage_and_dispatched_inside_it(monkeypatch):
    monkeypatch.delenv(api.DATA_CLIENTS_VARIABLE, raising=False)
    api.REGISTRY.bind(asyncio.get_running_loop())
    pipeline = await api.REGISTRY.acquire("22")
    try:
        t0 = pipeline.player.status().coverage_start
        with pytest.raises(HTTPException) as refused:
            api.publish("22", "count", api.RangeBody(start=t0 - ONE_SECOND, end=t0), target="row-count")
        assert refused.value.status_code == 409 and "outside the coverage" in refused.value.detail
        with pytest.raises(HTTPException) as refused:
            api.publish("22", "replay", api.RangeBody(start=t0, end=t0 + timedelta(seconds=30)))
        assert refused.value.status_code == 409
        with pytest.raises(HTTPException) as missing:
            api.publish("23", "stop")
        assert missing.value.status_code == 404
        # refresh is never refused: it is the command for a provider reporting nothing.
        assert api.refusal(PlayerStatus(
            timestamp=t0, mode="replay", cursor=None, speed=1.0, paused=True, loop=False, ended=True,
            can_seek=False, coverage_start=None, coverage_end=None, frame_interval_s=None,
        ), None, "refresh") is None
        assert api.publish("22", "refresh").applied == "refresh"
        with pytest.raises(ValueError):
            api.RangeBody(start=t0, end=t0)
        with pipeline.bus.subscribe(RowCountResult, overflow=Overflow.GROW) as results:
            ack = api.publish("22", "count", api.RangeBody(start=t0, end=t0 + ONE_SECOND),
                              target="row-count", start=t0.isoformat(), end=(t0 + ONE_SECOND).isoformat())
            assert ack.applied == "count"
            result = await asyncio.wait_for(results.get(), 2)
            assert result.result.count == 20
        assert api.publish("22", "stop").applied == "stop"
    finally:
        api.REGISTRY.release("22")
        await api.REGISTRY.stop_all()
        api.REGISTRY.bind(None)


async def _wait_status(subscription, predicate, timeout: float = 2.0) -> PlayerStatus:
    async def _wait():
        while True:
            message = await subscription.get()
            if predicate(message):
                return message

    return await asyncio.wait_for(_wait(), timeout)


# --- the provider swapped by environment: the time-series client over the stub -------


class HermeticTsdbClient(TimeSeriesDatabaseClient):
    """The time-series client wired to the stub in-process -- what a deployment
    names in TIME_SERIES_EXPLORER_DATA_CLIENTS, minus the port and the broker."""

    services: list[QueryService] = []
    env_settings = ()  # nothing to read: the wiring is in-process

    def __init__(self, name: str) -> None:
        feed = InMemoryResultFeed()
        service = QueryService(TiledRecording.load(repeat=2), feed)
        HermeticTsdbClient.services.append(service)
        http = httpx.AsyncClient(transport=httpx.ASGITransport(app=create_app(service)), base_url="http://stub")
        super().__init__(name, url="http://stub", http_client=http, feed=feed)


class UnreachableClient(SampleRecordingClient):
    """A store whose URL cannot be reached, as the client reports it."""

    env_settings = ()

    def __init__(self, name: str) -> None:
        super().__init__(name)

    async def coverage(self, model, mRID=None):
        raise ConnectionError("cannot reach http://tsdb:8100: ConnectError: All connection attempts failed")


async def test_an_unreachable_store_reaches_the_error_tray_and_the_page_still_connects(monkeypatch):
    monkeypatch.setenv(api.DATA_CLIENTS_VARIABLE, "tsdb:test_time_series_explorer:UnreachableClient")
    api.REGISTRY.bind(asyncio.get_running_loop())
    HUB.forget("26")
    pipeline = await api.REGISTRY.acquire("26")  # no 1011: the pipeline starts, stopped
    try:
        for _ in range(50):
            if HUB.recent("26"):
                break
            await asyncio.sleep(0.01)
        (notice,) = HUB.recent("26")
        assert notice.app == "time-series-explorer" and notice.source == "tsdb"
        assert notice.message == "the provider cannot be reached"
        assert "cannot reach http://tsdb:8100" in (notice.detail or "")
        message = api.state_message(pipeline, None, first=True)
        assert message.player.error == notice.detail and message.player.coverage_start is None
        t0 = load_sample().frames[0].timestamp
        with pytest.raises(HTTPException) as refused:
            api.publish("26", "count", api.RangeBody(start=t0, end=t0 + ONE_SECOND), target="row-count")
        assert refused.value.status_code == 409 and "no coverage" in refused.value.detail
        assert api.publish("26", "refresh").applied == "refresh"  # the way back, once it answers
    finally:
        api.REGISTRY.release("26")
        await api.REGISTRY.stop_all()
        api.REGISTRY.bind(None)
        HUB.forget("26")


async def test_provider_is_swapped_by_environment_alone(monkeypatch):
    monkeypatch.setenv(api.DATA_CLIENTS_VARIABLE, "tsdb:test_time_series_explorer:HermeticTsdbClient")
    pipeline = await api.build_pipeline("24")
    assert list(pipeline.gateway.clients) == ["tsdb"]
    await pipeline.start()
    try:
        status = pipeline.player.status()
        t0 = status.coverage_start
        assert status.coverage_end == t0 + timedelta(seconds=6.0)  # the stub's tiled minute
        with pipeline.bus.subscribe(RowCountResult, overflow=Overflow.GROW) as results:
            pipeline.bus.publish(Command(target="row-count", verb="count", args={
                "start": (t0 + timedelta(seconds=4)).isoformat(), "end": (t0 + timedelta(seconds=5)).isoformat(),
            }))
            result = await asyncio.wait_for(results.get(), 3)
        assert result.result.count == 20 and result.result.error is None
        service = HermeticTsdbClient.services[-1]
        assert service.completed == 1  # the count crossed REST and the envelope
        header = await api.stream_header(pipeline.gateway)
        assert header is not None and service.completed == 2  # and so did the header query
        assert HUB.recent("24") == []
    finally:
        await pipeline.stop()
