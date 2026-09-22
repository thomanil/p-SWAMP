# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""The time-series provider without a service or a broker: the HTTP half over
``httpx.MockTransport``, the results half over ``InMemoryResultFeed``.

The conformance suite runs it against the stub service in
``app/server-python/tests/test_time_series_database_client.py``; here are the
mechanics the contract promises -- subscribe before POST, demultiplexing by
query id, the end and error envelopes, the timeout, the cancel.
"""

from __future__ import annotations

import asyncio
import json
import sys
from datetime import timedelta

import pytest
from support import at

from pswamp_core.datagateway import Capability, DataGateway, MissingSettingError, TimeRange
from pswamp_core.datagateway.clients.time_series_database import (
    InMemoryResultFeed,
    TimeSeriesDatabaseClient,
)
from pswamp_core.datagateway.config import gateway_from_env
from pswamp_core.messages import PmuFrame, PmuHeader, TimeSeriesResult
from pswamp_core.util.time import utcnow

httpx = pytest.importorskip("httpx")

HEADER = PmuHeader(
    station=["A", "A"],
    channel=["V", "V"],
    measurement=["V_Magnitude", "f"],
    units=["kV", "Hz"],
    data_rate=1.0,
)
FRAMES = [
    PmuFrame(timestamp=at(i), mRID="s", header=HEADER, values=[400.0 + i, 50.0])
    for i in range(10)
]


class FakeService:
    """The REST side as a MockTransport handler, answering straight into the feed."""

    def __init__(self, feed: InMemoryResultFeed, *, frames=FRAMES, coverage=True) -> None:
        self.feed = feed
        self.frames = frames
        self.coverage = coverage
        self.calls: list[tuple[str, str]] = []
        self.queue_existed_at_post: list[bool] = []
        self.mode = "answer"  # or "silent", "refuse", "fail"

    def transport(self) -> httpx.MockTransport:
        return httpx.MockTransport(self.handle)

    def handle(self, request: httpx.Request) -> httpx.Response:
        self.calls.append((request.method, request.url.path))
        if request.url.path == "/v1/coverage":
            if not self.coverage or request.url.params["model"] != "pmu.frame":
                return httpx.Response(200, json={"start": None, "end": None, "live": False})
            last = self.frames[-1].timestamp + timedelta(microseconds=1)
            return httpx.Response(
                200,
                json={"start": at(0).isoformat(), "end": last.isoformat(), "live": False},
            )
        if request.method == "POST":
            body = json.loads(request.content)
            self.queue_existed_at_post.append(self.feed.has(body["query_id"]))
            if self.mode == "refuse":
                return httpx.Response(503, text="store is down")
            if self.mode != "silent":
                self.answer(body)
            return httpx.Response(202, json={"query_id": body["query_id"]})
        if request.method == "DELETE":
            return httpx.Response(204)
        return httpx.Response(404)

    def answer(self, body: dict) -> None:
        qid, seq = body["query_id"], 0
        stamp = utcnow()
        if self.mode == "fail":
            self.feed.dispatch(TimeSeriesResult.failed(qid, 0, "disk on fire", timestamp=stamp))
            return
        records = self.frames if body["model"] == "pmu.frame" else []
        start = body["start"] and TimeRange(_iso(body["start"]), None).start
        end = body["end"] and TimeRange(None, _iso(body["end"])).end
        for record in records:
            if (start is None or record.timestamp >= start) and (end is None or record.timestamp < end):
                self.feed.dispatch(TimeSeriesResult.for_record(qid, seq, record, timestamp=stamp))
                seq += 1
        self.feed.dispatch(TimeSeriesResult.ended(qid, seq, seq, timestamp=stamp))


def _iso(value: str):
    from datetime import datetime

    return datetime.fromisoformat(value)


def make(service: FakeService, **kwargs) -> TimeSeriesDatabaseClient:
    return TimeSeriesDatabaseClient(
        "tsdb",
        url="http://stub",
        http_client=httpx.AsyncClient(transport=service.transport(), base_url="http://stub"),
        feed=service.feed,
        **kwargs,
    )


def test_constructing_imports_neither_httpx_nor_aiokafka_and_checks_its_inputs():
    before = "aiokafka" in sys.modules
    client = TimeSeriesDatabaseClient("tsdb", url="http://x", bootstrap_servers="k:9092")
    assert client.name == "tsdb" and ("aiokafka" in sys.modules) == before
    assert client.capabilities == Capability.HISTORY_CONSUME
    assert client.supports(PmuFrame)
    with pytest.raises(ValueError):
        TimeSeriesDatabaseClient("tsdb", bootstrap_servers="k:9092")  # no URL
    with pytest.raises(ValueError):
        TimeSeriesDatabaseClient("tsdb", url="http://x")  # no brokers and no feed


def test_from_env_reads_its_block(monkeypatch):
    monkeypatch.setenv("TSDB_URL", "http://tsdb:8100/")
    monkeypatch.setenv("TSDB_BOOTSTRAP_SERVERS", "a:9092, b:9092")
    monkeypatch.setenv("TSDB_TIMEOUT", "2.5")
    monkeypatch.setenv("TSDB_PRIORITY", "3")
    monkeypatch.setenv(
        "CLIENTS",
        "tsdb:pswamp_core.datagateway.clients.time_series_database:TimeSeriesDatabaseClient",
    )
    gateway = gateway_from_env(variable="CLIENTS")
    client = gateway.clients["tsdb"]
    assert isinstance(client, TimeSeriesDatabaseClient)
    assert client.url == "http://tsdb:8100"
    assert client.feed.bootstrap_servers == ["a:9092", "b:9092"]
    assert client.feed.topic == TimeSeriesResult.topic == "time.series.result"
    assert client.timeout == timedelta(seconds=2.5) and client.priority == 3
    monkeypatch.delenv("TSDB_URL")
    with pytest.raises(MissingSettingError):
        gateway_from_env(variable="CLIENTS")


async def test_coverage_is_asked_of_the_service_and_null_means_none():
    service = FakeService(InMemoryResultFeed())
    client = make(service)
    coverage = await client.coverage(PmuFrame)
    assert coverage is not None and coverage.range.start == at(0) and coverage.live is False
    assert coverage.range.end > at(9)
    service.coverage = False
    assert await client.coverage(PmuFrame) is None
    assert service.calls == [("GET", "/v1/coverage")] * 2
    await client.close()


async def test_consume_subscribes_before_posting_and_yields_the_range_until_end():
    service = FakeService(InMemoryResultFeed())
    client = make(service)
    got = [r async for r in client.consume(PmuFrame, TimeRange(at(2), at(5)))]
    assert [r.timestamp for r in got] == [at(2), at(3), at(4)]
    assert service.queue_existed_at_post == [True]
    assert [m for m, _ in service.calls] == ["POST"]  # no cancel: the query ended
    assert service.feed._queues == {}  # the queue was unregistered on exit
    assert all(r.header == HEADER for r in got)  # the layout came inside each record
    await client.close()


async def test_two_interleaved_queries_are_demultiplexed_and_a_stray_answer_is_dropped():
    feed = InMemoryResultFeed()
    service = FakeService(feed)
    client = make(service)
    first = client.consume(PmuFrame, TimeRange(at(0), at(3)))
    second = client.consume(PmuFrame, TimeRange(at(5), at(7)))
    a = await first.__anext__()
    b = await second.__anext__()
    rest_a = [r async for r in first]
    rest_b = [r async for r in second]
    assert [r.timestamp for r in [a, *rest_a]] == [at(0), at(1), at(2)]
    assert [r.timestamp for r in [b, *rest_b]] == [at(5), at(6)]
    feed.dispatch(TimeSeriesResult.ended("nobody", 0, 0, timestamp=utcnow()))
    assert feed.dropped == 1
    await client.close()


async def test_an_error_envelope_raises_and_a_refused_post_raises():
    service = FakeService(InMemoryResultFeed())
    client = make(service)
    service.mode = "fail"
    with pytest.raises(RuntimeError, match="disk on fire"):
        [r async for r in client.consume(PmuFrame, TimeRange(at(0), None))]
    service.mode = "refuse"
    with pytest.raises(RuntimeError, match="HTTP 503"):
        [r async for r in client.consume(PmuFrame, TimeRange(at(0), None))]
    assert ("DELETE", "/v1/queries/" + "x") not in service.calls
    assert not any(m == "DELETE" for m, _ in service.calls)  # nothing to cancel
    await client.close()


async def test_a_silent_service_times_out_and_the_query_is_cancelled():
    service = FakeService(InMemoryResultFeed())
    client = make(service, timeout=timedelta(seconds=0.05))
    service.mode = "silent"
    with pytest.raises(TimeoutError, match="within 0.05s"):
        [r async for r in client.consume(PmuFrame, TimeRange(at(0), None))]
    assert [m for m, _ in service.calls] == ["POST", "DELETE"]
    assert service.calls[1][1].startswith("/v1/queries/")
    await client.close()


async def test_closing_a_stream_early_cancels_the_query():
    service = FakeService(InMemoryResultFeed())
    client = make(service)
    stream = DataGateway([client]).consume(PmuFrame, at(0), None)
    first = await stream.__anext__()
    assert first.timestamp == at(0)
    await stream.aclose()
    assert [m for m, _ in service.calls][-1] == "DELETE"
    assert service.feed._queues == {}
    await client.close()


async def test_an_unreachable_service_is_a_connection_error_naming_the_url():
    def refuse(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("All connection attempts failed", request=request)

    client = TimeSeriesDatabaseClient(
        "tsdb", url="http://tsdb:8100",
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(refuse), base_url="http://tsdb:8100"),
        feed=InMemoryResultFeed(),
    )
    with pytest.raises(ConnectionError, match="cannot reach http://tsdb:8100: ConnectError"):
        await client.coverage(PmuFrame)
    with pytest.raises(ConnectionError, match="cannot reach http://tsdb:8100"):
        [r async for r in client.consume(PmuFrame, TimeRange(at(0), None))]
    gateway = DataGateway([client])
    assert await gateway.coverage(PmuFrame) is None
    assert "cannot reach http://tsdb:8100" in gateway.coverage_failures["tsdb"]
    await client.close()


async def test_produce_is_refused():
    client = make(FakeService(InMemoryResultFeed()))
    with pytest.raises(TypeError):
        await client.produce(FRAMES[0])
    await client.close()


async def test_open_and_close_are_idempotent_with_an_injected_feed():
    client = make(FakeService(InMemoryResultFeed()))
    await client.open()
    await client.open()
    await client.close()
    await client.close()
    await asyncio.sleep(0)
