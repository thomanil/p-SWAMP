# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""The error topic's edge: the hub keeps and fans out per client, the forwarder
carries a pipeline's ErrorEvents into it with the app's slug, and the socket
replays the recent ones then pushes new ones."""

from __future__ import annotations

import asyncio
import json

from fastapi import WebSocketDisconnect

from errors import HUB, ErrorForwarderModule, ErrorHub
from errors.api import ws_endpoint
from pswamp_core.bus import InProcessBus
from pswamp_core.datagateway import Capability, DataGateway, Player
from pswamp_core.datagateway.clients import InMemoryClient
from pswamp_core.messages import ErrorEvent, PmuFrame, PmuHeader
from pswamp_core.pipeline import Pipeline
from pswamp_core.util.time import utcnow


def event(message: str, **kwargs) -> ErrorEvent:
    return ErrorEvent(timestamp=utcnow(), source="player", message=message, **kwargs)


async def test_hub_keeps_the_recent_ones_per_client_and_fans_out_to_watchers():
    hub = ErrorHub(recent=2)
    with hub.watching("c1") as q1, hub.watching("c1") as q1b, hub.watching("c2") as q2:
        first = hub.publish("c1", "streamer", event("one", detail="X: y", request_id="r1"))
        hub.publish("c1", "streamer", event("two"))
        hub.publish("c1", "explorer", event("three"))
        assert [n.message for n in hub.recent("c1")] == ["two", "three"]  # ring of 2
        assert hub.recent("c2") == [] and q2.empty()
        assert q1.qsize() == 3 and q1b.qsize() == 3  # every socket of the client
        assert hub.watchers("c1") == 2
    assert hub.watchers("c1") == 0
    assert first.app == "streamer" and first.detail == "X: y" and first.request_id == "r1"
    assert first.type == "state" and len(first.id) == 32
    hub.forget("c1")
    assert hub.recent("c1") == []


class Dies(InMemoryClient):
    async def consume(self, model, time_range, mRID=None):
        if model is PmuFrame:
            raise RuntimeError("no such table")
        async for record in super().consume(model, time_range, mRID):
            yield record


async def test_forwarder_in_a_pipeline_tags_the_notice_with_the_app():
    hub = ErrorHub()
    header = PmuHeader.build(timestamp=utcnow(), mRID="d", station=["x"], channel=["f"], measurement=["f"], units=["Hz"], data_rate=1.0)
    frame = PmuFrame(timestamp=utcnow(), mRID="d", header_id=header.header_id, values=[50.0])
    client = Dies("dies", [PmuHeader, PmuFrame], [header, frame], capabilities=Capability.HISTORY_CONSUME)
    bus = InProcessBus()
    forwarder = ErrorForwarderModule("client-9", "some-app", hub)
    pipeline = Pipeline("client-9", DataGateway([client]), bus, Player(DataGateway([client]), bus, model=PmuFrame, paced=False), [forwarder])
    await pipeline.start()
    try:
        pipeline.player.resume()
        for _ in range(50):
            if hub.recent("client-9"):
                break
            await asyncio.sleep(0.02)
    finally:
        await pipeline.stop()
    (notice,) = hub.recent("client-9")
    assert notice.app == "some-app" and notice.source == "player"
    assert notice.detail == "RuntimeError: no such table"
    assert forwarder.forwarded == 1
    assert pipeline.player.status().error == notice.detail


class FakeWebSocket:
    def __init__(self, client_id: str | None) -> None:
        self.query_params = {} if client_id is None else {"client_id": client_id}
        self.sent: list[dict] = []
        self.closed: int | None = None
        self.accepted = False
        self.release = asyncio.Event()

    async def accept(self) -> None:
        self.accepted = True

    async def close(self, code: int) -> None:
        self.closed = code

    async def send_text(self, text: str) -> None:
        self.sent.append(json.loads(text))

    async def receive_text(self) -> str:
        await self.release.wait()
        raise WebSocketDisconnect(1000)


async def test_socket_replays_the_recent_notices_then_pushes_new_ones():
    HUB.forget("77")
    HUB.publish("77", "explorer", event("before the socket"))
    ws = FakeWebSocket("77")
    task = asyncio.create_task(ws_endpoint(ws))  # type: ignore[arg-type]
    await asyncio.sleep(0.01)
    assert ws.accepted and HUB.watchers("77") == 1
    HUB.publish("77", "streamer", event("after the socket", request_id="r2"))
    HUB.publish("78", "streamer", event("someone else's"))
    await asyncio.sleep(0.01)
    ws.release.set()
    await asyncio.wait_for(task, 2)
    assert [(n["app"], n["message"]) for n in ws.sent] == [
        ("explorer", "before the socket"),
        ("streamer", "after the socket"),
    ]
    assert ws.sent[1]["request_id"] == "r2" and ws.sent[1]["type"] == "state"
    assert HUB.watchers("77") == 0
    HUB.forget("77")
    HUB.forget("78")

    refused = FakeWebSocket(None)
    await ws_endpoint(refused)  # type: ignore[arg-type]
    assert refused.closed == 1008 and not refused.accepted
