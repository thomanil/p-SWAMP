# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""Line outage detection.

The web counterpart of p-SWAMP's ``LineOutageDetectionApp``, which watches the
current magnitude on every branch and reports the moment one stops carrying
current. The detector itself is upstream and unmodified; everything here is
transport.

Event-driven, and more so than islanding: ``run_analysis`` returns ``None``
unless a branch actually changed state, so a healthy grid produces no traffic at
all. There is nothing to poll and no ticker -- the client is sent the log on
connect, and again whenever it grows.

Note this application is the reason the recorded dataset carries current
channels. It reads ``i_Magnitude``, which the frequency- and voltage-based
applications ignore; a recording made without those channels leaves this
detector running happily and finding nothing, forever.
"""

import asyncio
import contextlib

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from ..hub import HUB, LINE_OUTAGE_RESULT_TOPIC
from ..wire import LineOutageLog, send_state

router = APIRouter()


def current_message() -> LineOutageLog:
    store = HUB.line_outages
    return LineOutageLog(
        app_uuid=store.app_uuid,
        app_name=store.app_name,
        window_length=store.window_length,
        events=store.list(),
    )


@router.websocket("/ws")
async def ws_endpoint(ws: WebSocket) -> None:
    await ws.accept()

    updates: asyncio.Queue = asyncio.Queue(maxsize=64)

    # The payload is re-read from the store rather than carried on the queue, so
    # a dropped notification costs latency and never content -- the same
    # arrangement the islanding endpoint uses.
    detach = HUB.bus.add_listener(
        LINE_OUTAGE_RESULT_TOPIC, lambda _payload: _offer(updates)
    )

    async def push() -> None:
        await send_state(ws, current_message())
        while True:
            await updates.get()
            await send_state(ws, current_message())

    pusher = asyncio.create_task(push())
    try:
        # No commands: this page is read-only. Reading anyway, because that is
        # what detects the client going away.
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        pass
    except Exception:
        pass
    finally:
        detach()
        pusher.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await pusher


def _offer(queue: asyncio.Queue) -> None:
    try:
        queue.put_nowait(None)
    except asyncio.QueueFull:
        pass
