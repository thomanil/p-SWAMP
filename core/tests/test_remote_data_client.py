# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""The Remote Data Client without a service: HTTP over ``httpx.MockTransport``,
with each query's answer a response body handed out one line at a time.

The conformance suite runs it against the stub service in
``app/server-python/tests/test_remote_data_service.py``; here are the
mechanics the contract promises -- the range until the end line, the error
line, a refused query, a body that stops short, the timeout, the lazy pull
(backpressure) and the cancel-by-closing.
"""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta

import pytest
from support import at

from pswamp_core.datagateway import DataGateway, MissingSettingError, TimeRange
from pswamp_core.datagateway.clients.remote_data import RemoteDataClient
from pswamp_core.datagateway.config import gateway_from_env
from pswamp_core.messages import PmuFrame, PmuHeader, RemoteDataResult

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


class LineBody(httpx.AsyncByteStream):
    """A response body that yields one line per chunk, and records how many it
    handed out and whether the client closed it -- the two things a streamed
    answer is for."""

    def __init__(self, lines: list[bytes], *, hang_after: int | None = None, break_after: int | None = None):
        self.lines = lines
        self.hang_after = hang_after
        self.break_after = break_after
        self.handed = 0
        self.closed = False

    async def __aiter__(self):
        for line in self.lines:
            if self.hang_after is not None and self.handed >= self.hang_after:
                await asyncio.Event().wait()
            if self.break_after is not None and self.handed >= self.break_after:
                raise httpx.ReadError("connection reset by peer")
            self.handed += 1
            yield line

    async def aclose(self) -> None:
        self.closed = True


class FakeService:
    """The service as a MockTransport handler. ``mode`` picks the answer:
    ``answer``, ``fail`` (an error line), ``refuse`` (503), ``truncate`` (no
    end line), ``silent`` (headers, then nothing), ``slow`` (no headers at
    all), ``break`` (the connection drops after two lines)."""

    def __init__(self, *, frames=FRAMES, coverage=True) -> None:
        self.frames = frames
        self.coverage = coverage
        #: Extra fields merged into the coverage answer, to show they are ignored.
        self.coverage_extra: dict = {}
        self.calls: list[tuple[str, str]] = []
        self.bodies: list[LineBody] = []
        self.extra_lines: list[bytes] = []
        self.mode = "answer"

    def transport(self) -> httpx.MockTransport:
        return httpx.MockTransport(self.handle)

    async def handle(self, request: httpx.Request) -> httpx.Response:
        self.calls.append((request.method, request.url.path))
        if request.url.path == "/v1/coverage":
            if not self.coverage or request.url.params["model"] != "pmu.frame":
                return httpx.Response(200, json={"start": None, "end": None})
            last = self.frames[-1].timestamp + timedelta(microseconds=1)
            return httpx.Response(
                200,
                json={"start": at(0).isoformat(), "end": last.isoformat(), **self.coverage_extra},
            )
        if request.method == "POST" and request.url.path == "/v1/queries":
            if self.mode == "refuse":
                return httpx.Response(503, text="store is down")
            if self.mode == "slow":
                await asyncio.Event().wait()
            body = LineBody(
                self.answer(json.loads(request.content)),
                hang_after=0 if self.mode == "silent" else None,
                break_after=2 if self.mode == "break" else None,
            )
            self.bodies.append(body)
            return httpx.Response(200, headers={"content-type": "application/x-ndjson"}, stream=body)
        return httpx.Response(404)

    def answer(self, body: dict) -> list[bytes]:
        if self.mode == "fail":
            return [RemoteDataResult.failed("disk on fire").to_line()]
        start = body["start"] and datetime.fromisoformat(body["start"])
        end = body["end"] and datetime.fromisoformat(body["end"])
        lines = list(self.extra_lines)
        for record in self.frames if body["model"] == "pmu.frame" else []:
            if (start is None or record.timestamp >= start) and (end is None or record.timestamp < end):
                lines.append(RemoteDataResult.for_record(record).to_line())
        if self.mode != "truncate":
            lines.append(RemoteDataResult.ended(len(lines)).to_line())
        return lines


def make(service: FakeService, **kwargs) -> RemoteDataClient:
    return RemoteDataClient(
        "remote_data",
        url="http://stub",
        http_client=httpx.AsyncClient(transport=service.transport(), base_url="http://stub"),
        **kwargs,
    )


def test_from_env_reads_its_block(monkeypatch):
    monkeypatch.setenv("REMOTE_DATA_URL", "http://remote-data:8100/")
    monkeypatch.setenv("REMOTE_DATA_TIMEOUT", "2.5")
    monkeypatch.setenv(
        "CLIENTS",
        "remote_data:pswamp_core.datagateway.clients.remote_data:RemoteDataClient",
    )
    gateway = gateway_from_env(variable="CLIENTS")
    client = gateway.clients["remote_data"]
    assert isinstance(client, RemoteDataClient)
    assert client.url == "http://remote-data:8100"
    assert client.timeout == timedelta(seconds=2.5)
    monkeypatch.delenv("REMOTE_DATA_URL")
    with pytest.raises(MissingSettingError):
        gateway_from_env(variable="CLIENTS")


async def test_coverage_is_asked_of_the_service_and_null_means_none():
    service = FakeService()
    client = make(service)
    coverage = await client.coverage(PmuFrame)
    assert coverage is not None and coverage.range.start == at(0) and coverage.live is False
    assert coverage.range.end > at(9)
    service.coverage = False
    assert await client.coverage(PmuFrame) is None
    assert service.calls == [("GET", "/v1/coverage")] * 2
    await client.close()


async def test_consume_yields_the_range_until_the_end_line():
    service = FakeService()
    client = make(service)
    got = [r async for r in client.consume(PmuFrame, TimeRange(at(2), at(5)))]
    assert [r.timestamp for r in got] == [at(2), at(3), at(4)]
    assert service.calls == [("POST", "/v1/queries")]  # one call; nothing to cancel
    assert all(r.header == HEADER for r in got)  # the layout came inside each record
    assert service.bodies[0].closed
    await client.close()


async def test_lines_are_pulled_only_as_records_are_wanted():
    # Backpressure: the client reads the next line when asked for the next
    # record, not the whole body up front -- so a service that honours the
    # socket's flow control reads its store at the player's pace.
    service = FakeService()
    client = make(service)
    stream = client.consume(PmuFrame, TimeRange(at(0), None))
    for _ in range(3):
        await stream.__anext__()
    body = service.bodies[0]
    assert body.handed == 3 and not body.closed
    await stream.aclose()
    assert body.closed and body.handed == 3


async def test_closing_a_stream_early_closes_the_response():
    service = FakeService()
    client = make(service)
    stream = DataGateway([client]).consume(PmuFrame, at(0), None)
    first = await stream.__anext__()
    assert first.timestamp == at(0)
    await stream.aclose()
    assert service.bodies[0].closed  # the connection closing is the cancel
    assert [m for m, _ in service.calls] == ["GET", "POST"]  # coverage, query; no cancel call
    await client.close()


async def test_an_error_line_raises_and_a_refused_query_raises():
    service = FakeService()
    client = make(service)
    service.mode = "fail"
    with pytest.raises(RuntimeError, match="disk on fire"):
        [r async for r in client.consume(PmuFrame, TimeRange(at(0), None))]
    service.mode = "refuse"
    with pytest.raises(RuntimeError, match="HTTP 503 store is down"):
        [r async for r in client.consume(PmuFrame, TimeRange(at(0), None))]
    await client.close()


async def test_a_body_without_an_end_line_is_a_failure_not_an_empty_answer():
    service = FakeService()
    service.mode = "truncate"
    client = make(service)
    got = []
    with pytest.raises(RuntimeError, match="ended without an end line"):
        async for record in client.consume(PmuFrame, TimeRange(at(0), at(3))):
            got.append(record)
    assert len(got) == 3  # what did arrive was handed on
    await client.close()


async def test_an_unreachable_service_is_a_connection_error_naming_the_url():
    def refuse(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("All connection attempts failed", request=request)

    client = RemoteDataClient(
        "remote_data", url="http://remote-data:8100",
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(refuse), base_url="http://remote-data:8100"),
    )
    with pytest.raises(ConnectionError, match="cannot reach http://remote-data:8100: ConnectError"):
        await client.coverage(PmuFrame)
    with pytest.raises(ConnectionError, match="cannot reach http://remote-data:8100"):
        [r async for r in client.consume(PmuFrame, TimeRange(at(0), None))]
    gateway = DataGateway([client])
    assert await gateway.coverage(PmuFrame) is None
    assert "cannot reach http://remote-data:8100" in gateway.coverage_failures["remote_data"]
    await client.close()
