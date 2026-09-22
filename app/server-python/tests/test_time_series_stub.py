# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""The stub time-series service without Kafka: the tiled recording, the query
service over a list sink, and the REST routes driven in-process."""

from __future__ import annotations

import asyncio
from datetime import timedelta

import httpx
import pytest

from pmu_test_streamer.sample_client import STREAM_ID, load_sample
from pswamp_core.messages import PmuFrame, TimeSeriesQuery, TimeSeriesResult
from time_series_stub.app import create_app
from time_series_stub.recording import TiledRecording
from time_series_stub.service import QueryService


class ListSink:
    def __init__(self) -> None:
        self.items: list[TimeSeriesResult] = []
        self.gate = asyncio.Event()
        self.gate.set()

    async def publish(self, result: TimeSeriesResult) -> None:
        await self.gate.wait()
        self.items.append(result)


def test_tiling_repeats_the_sample_back_to_back():
    sample = load_sample()
    tiled = TiledRecording.load(repeat=3)
    assert len(tiled.frames) == 180
    assert tiled.header == sample.header
    start, end = tiled.coverage(PmuFrame.topic)
    assert start == sample.frames[0].timestamp
    assert end == start + timedelta(seconds=9.0)
    stamps = [f.timestamp for f in tiled.frames]
    assert stamps == sorted(stamps) and len(set(stamps)) == 180
    assert tiled.frames[60].timestamp == sample.frames[0].timestamp + timedelta(seconds=3.0)
    assert tiled.coverage("pmu.header") is None  # no separate header: it rides in every frame
    assert tiled.coverage("nope") is None
    with pytest.raises(ValueError):
        TiledRecording.load(repeat=0)


def test_select_is_half_open_and_filters_by_mrid():
    tiled = TiledRecording.load(repeat=2)
    t0 = tiled.frames[0].timestamp
    rows = tiled.select(PmuFrame.topic, t0 + timedelta(seconds=1), t0 + timedelta(seconds=2))
    assert len(rows) == 20 and all(t0 + timedelta(seconds=1) <= r.timestamp < t0 + timedelta(seconds=2) for r in rows)
    assert tiled.select(PmuFrame.topic, None, None, mrid=["someone-else"]) == []
    assert len(tiled.select(PmuFrame.topic, None, None, mrid=[STREAM_ID])) == 120
    assert all(row.header == tiled.header for row in rows)
    with pytest.raises(KeyError):
        tiled.select("nope", None, None)


async def test_query_service_publishes_records_then_end():
    sink = ListSink()
    service = QueryService(TiledRecording.load(repeat=1), sink)
    t0 = service.recording.frames[0].timestamp
    query = TimeSeriesQuery(query_id="q1", model="pmu.frame", start=t0, end=t0 + timedelta(seconds=1))
    await service.start_query(query)
    kinds = [r.kind for r in sink.items]
    assert kinds == ["record"] * 20 + ["end"]
    assert [r.seq for r in sink.items] == list(range(21))
    assert sink.items[-1].count == 20
    assert all(r.query_id == "q1" for r in sink.items)
    assert PmuFrame.model_validate(sink.items[0].record).timestamp == t0
    assert service.completed == 1 and not service.running("q1")


async def test_unknown_model_is_an_error_envelope_and_a_duplicate_id_is_refused():
    sink = ListSink()
    service = QueryService(TiledRecording.load(repeat=1), sink)
    await service.start_query(TimeSeriesQuery(query_id="q2", model="nope"))
    (only,) = sink.items
    assert only.kind == "error" and "nope" in only.error and only.seq == 0
    sink.gate.clear()  # hold the next query mid-flight
    task = service.start_query(TimeSeriesQuery(query_id="q3", model="pmu.frame"))
    with pytest.raises(KeyError):
        service.start_query(TimeSeriesQuery(query_id="q3", model="pmu.frame"))
    assert service.cancel("q3") is True
    with pytest.raises(asyncio.CancelledError):
        await task
    assert service.cancel("q3") is False
    sink.gate.set()
    await asyncio.sleep(0)
    assert all(r.query_id != "q3" or r.kind == "record" for r in sink.items)  # never an 'end'


async def test_rest_routes():
    sink = ListSink()
    service = QueryService(TiledRecording.load(repeat=2), sink)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=create_app(service)), base_url="http://stub") as http:
        assert (await http.get("/healthz")).json() == {"status": "ok"}
        coverage = await http.get("/v1/coverage", params={"model": "pmu.frame"})
        assert coverage.status_code == 200
        body = coverage.json()
        assert body["model"] == "pmu.frame" and body["live"] is False
        assert body["start"].startswith("2026-01-01T00:00:00.05")
        assert (await http.get("/v1/coverage", params={"model": "nope"})).status_code == 404

        sink.gate.clear()
        accepted = await http.post(
            "/v1/queries", json=TimeSeriesQuery(query_id="r1", model="pmu.frame").model_dump(mode="json")
        )
        assert accepted.status_code == 202 and accepted.json() == {"query_id": "r1"}
        again = await http.post(
            "/v1/queries", json=TimeSeriesQuery(query_id="r1", model="pmu.frame").model_dump(mode="json")
        )
        assert again.status_code == 409
        assert (await http.delete("/v1/queries/r1")).status_code == 204
        assert (await http.delete("/v1/queries/r1")).status_code == 404
        sink.gate.set()
        bad = await http.post("/v1/queries", json={"query_id": "x"})  # no model
        assert bad.status_code == 422
