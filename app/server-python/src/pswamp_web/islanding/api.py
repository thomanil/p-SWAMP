# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""Islanding detection and its alarms.

The web counterpart of p-SWAMP's Qt islanding alarm view and alarm overview,
and the one page where the whole chain is visible end to end: the recorded line
trip separates part of the grid, the unmodified upstream detector notices it,
the result crosses from the application thread to the event loop, and the map
recolours.

Unlike the measurement pages, this one is event-driven. Results arrive once a
second and alarms far less often than that, so there is nothing to gain from a
ticker -- the client is sent something when there is something to send.
"""

import asyncio
import contextlib

from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from pydantic import BaseModel

from ..hub import HUB, ISLANDING_RESULT_TOPIC
from ..wire import AlarmList, IslandingResult, send_state
from .adapt import to_wire

router = APIRouter()


class IslandingState(BaseModel):
    """Both halves of this page in one message.

    Detection and alarms are separate concerns upstream -- different topics, one
    derived from the other -- but they change together and are read together, so
    splitting them across two sockets would only mean the page could render them
    inconsistently.
    """

    type: str = "state"
    islanding: IslandingResult | None = None
    alarms: AlarmList


def current_message(latest: IslandingResult | None) -> IslandingState:
    return IslandingState(islanding=latest, alarms=AlarmList(alarms=HUB.alarms.list()))


@router.websocket("/ws")
async def ws_endpoint(ws: WebSocket) -> None:
    await ws.accept()

    latest: IslandingResult | None = None
    updates: asyncio.Queue = asyncio.Queue(maxsize=64)

    # Wake on either topic; the payload itself is re-read from the hub, so a
    # dropped notification costs nothing but latency.
    detach = [
        HUB.bus.add_listener(ISLANDING_RESULT_TOPIC, lambda p: _offer(updates, p)),
        HUB.bus.add_listener("alarms", lambda p: _offer(updates, None)),
    ]

    async def push() -> None:
        nonlocal latest
        await send_state(ws, current_message(latest))
        while True:
            payload = await updates.get()
            if payload is not None and HUB.islanding_app is not None:
                adapted = to_wire(HUB.islanding_app, payload)
                if adapted is not None:
                    latest = adapted
            await send_state(ws, current_message(latest))

    pusher = asyncio.create_task(push())
    try:
        while True:
            await handle_command(await ws.receive_json())
            # An operator action changes the alarm list, so answer immediately
            # rather than waiting for the next application event.
            _offer(updates, None)
    except WebSocketDisconnect:
        pass
    except Exception:
        pass
    finally:
        for remove in detach:
            remove()
        pusher.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await pusher


def _offer(queue: asyncio.Queue, payload) -> None:
    try:
        queue.put_nowait(payload)
    except asyncio.QueueFull:
        pass


# An operator note longer than this is a report, not an annotation; truncated
# rather than rejected so a slip cannot lose what was typed.
MAX_NOTE = 500


async def handle_command(message: dict) -> None:
    action = message.get("action")
    alarm_id = message.get("alarm_uuid")
    if action == "acknowledge" and alarm_id:
        HUB.alarms.annotate(alarm_id, "acknowledge", "Acknowledged by operator")
    elif action == "silence" and alarm_id:
        HUB.alarms.annotate(alarm_id, "silence", "Silenced by operator")
    elif action == "annotate" and alarm_id:
        # The Qt dialogue's "Annotate" button: a free-text operator note, which
        # upstream records as a `user_message` event on the alarm rather than as
        # a state change. Same event type here, so the two agree on the wire.
        note = str(message.get("message") or "").strip()[:MAX_NOTE]
        if note:
            HUB.alarms.annotate(alarm_id, "user_message", note)
