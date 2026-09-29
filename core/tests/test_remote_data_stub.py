# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""The example remote data service (``core/examples/remote_data_stub``): the
tiled recording, the query service's streamed answer iterated directly, and the
REST routes driven in-process. The black-box HTTP check of the same service is
``scripts/check-remote-data-service.sh``.

Skipped without FastAPI and httpx (the core's ``examples`` group); the server's
environment, where this suite runs, has both."""

from __future__ import annotations

from datetime import timedelta

import pytest

httpx = pytest.importorskip("httpx")
pytest.importorskip("fastapi")

from pswamp_core.messages import PmuFrame, RemoteDataQuery, RemoteDataResult  # noqa: E402
from remote_data_stub.app import create_app  # noqa: E402
from remote_data_stub.recording import TiledRecording, load_frames  # noqa: E402
from remote_data_stub.service import QueryService  # noqa: E402

STREAM_ID = "n44-sample"


async def answer(service: QueryService, query: RemoteDataQuery) -> list[RemoteDataResult]:
    return [RemoteDataResult.model_validate_json(line) async for line in service.stream(query)]


def test_tiling_repeats_the_sample_back_to_back():
    sample = load_frames()
    tiled = TiledRecording.load(repeat=3)
    assert len(tiled.frames) == 180
    assert tiled.header == sample[0].header
    start, end = tiled.coverage(PmuFrame.topic)
    assert start == sample[0].timestamp
    assert end == start + timedelta(seconds=9.0)
    stamps = [f.timestamp for f in tiled.frames]
    assert stamps == sorted(stamps) and len(set(stamps)) == 180
    assert tiled.frames[60].timestamp == sample[0].timestamp + timedelta(seconds=3.0)
    assert tiled.coverage("pmu.header") is None  # no separate header: it rides in every frame
    assert tiled.coverage("nope") is None
    with pytest.raises(ValueError):
        TiledRecording.load(repeat=0)


async def test_query_service_streams_records_then_end():
    service = QueryService(TiledRecording.load(repeat=1))
    t0 = service.recording.frames[0].timestamp
    query = RemoteDataQuery(query_id="q1", model="pmu.frame", start=t0, end=t0 + timedelta(seconds=1))
    lines = await answer(service, query)
    assert [r.kind for r in lines] == ["record"] * 20 + ["end"]
    assert lines[-1].count == 20
    assert PmuFrame.model_validate(lines[0].record).timestamp == t0
    assert service.completed == 1 and service.streaming == 0


async def test_rest_routes():
    service = QueryService(TiledRecording.load(repeat=2))
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=create_app(service)), base_url="http://stub") as http:
        assert (await http.get("/healthz")).json() == {"status": "ok"}
        coverage = await http.get("/v1/coverage", params={"model": "pmu.frame"})
        assert coverage.status_code == 200
        body = coverage.json()
        assert body["model"] == "pmu.frame" and "live" not in body
        assert body["start"].startswith("2026-01-01T00:00:00.05")
        assert (await http.get("/v1/coverage", params={"model": "nope"})).status_code == 404

        t0 = service.recording.frames[0].timestamp
        query = RemoteDataQuery(query_id="r1", model="pmu.frame", start=t0, end=t0 + timedelta(seconds=1))
        answered = await http.post("/v1/queries", json=query.model_dump(mode="json"))
        assert answered.status_code == 200
        assert answered.headers["content-type"].startswith("application/x-ndjson")
        assert answered.headers["x-accel-buffering"] == "no"
        lines = answered.text.splitlines()
        assert len(lines) == 21 and lines[-1] == '{"kind":"end","count":20}'
        assert RemoteDataResult.model_validate_json(lines[0]).kind == "record"

        unknown = await http.post(
            "/v1/queries", json=RemoteDataQuery(query_id="r2", model="nope").model_dump(mode="json")
        )
        assert unknown.status_code == 404  # refused while a status code can still say so
        bad = await http.post("/v1/queries", json={"query_id": "x"})  # no model
        assert bad.status_code == 422
    assert service.completed == 1
