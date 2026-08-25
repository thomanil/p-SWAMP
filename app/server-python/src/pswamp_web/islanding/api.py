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

The operator actions -- acknowledge, silence, annotate -- come up as POSTs on the
alarm they apply to. They change the alarm list, which is not an application
event, so each one wakes this client's push task explicitly; see `_nudge`.
"""

import asyncio
import contextlib
from dataclasses import dataclass

from fastapi import APIRouter, HTTPException, WebSocket, WebSocketDisconnect
from pydantic import BaseModel, Field

from ..hub import ISLANDING_RESULT_TOPIC, Hub, connected_hub, live_hub, read_client_id
from ..sessions import SessionRegistry
from ..wire import (
    AlarmList,
    ClientId,
    CommandAck,
    IslandingResult,
    IslandingState,
    send_state,
)
from .adapt import to_wire

router = APIRouter()


def current_message(hub: Hub, latest: IslandingResult | None) -> IslandingState:
    return IslandingState(islanding=latest, alarms=AlarmList(alarms=hub.alarms.list()))


@dataclass
class Session:
    """One open view of this page, as a command needs to see it.

    Only the wake-up queue: everything a command actually changes lives in the
    client's Hub, which the registry in hub.py already addresses by client id.
    What a command cannot reach without this is the *push task* -- so this is how
    an HTTP request tells a socket that has been sitting on `updates.get()` that
    there is something new to send.
    """

    updates: asyncio.Queue


SESSIONS: SessionRegistry[Session] = SessionRegistry()


@router.websocket("/ws")
async def ws_endpoint(ws: WebSocket) -> None:
    async with connected_hub(ws) as hub:
        if hub is None:
            return

        client_id = read_client_id(ws)
        latest: IslandingResult | None = None
        updates: asyncio.Queue = asyncio.Queue(maxsize=64)

        # Wake on either topic; the payload itself is re-read from the hub, so a
        # dropped notification costs nothing but latency.
        detach = [
            hub.bus.add_listener(ISLANDING_RESULT_TOPIC, lambda p: _offer(updates, p)),
            hub.bus.add_listener("alarms", lambda p: _offer(updates, None)),
        ]

        async def push() -> None:
            nonlocal latest
            await send_state(ws, current_message(hub, latest))
            while True:
                payload = await updates.get()
                if payload is not None and hub.islanding_app is not None:
                    adapted = to_wire(hub.islanding_app, payload)
                    if adapted is not None:
                        latest = adapted
                await send_state(ws, current_message(hub, latest))

        with SESSIONS.registered(client_id, Session(updates=updates)):
            pusher = asyncio.create_task(push())
            try:
                # Operator actions are POSTs now; this loop only surfaces the
                # disconnect. See the commands at the bottom of this module.
                while True:
                    await ws.receive_text()
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


class AlarmNote(BaseModel):
    """Body of POST /alarms/{alarm_uuid}/annotate."""

    message: str = Field(
        min_length=1,
        description=(
            "The operator's note. Longer than 500 characters is truncated rather "
            "than rejected, so a slip cannot lose what was typed."
        ),
    )


# --- REST commands ----------------------------------------------------------
#
# The alarm is a resource with an id, so these read as actions on it:
#
#     POST /api/islanding/alarms/<uuid>/acknowledge
#
# Note the scope, which comes from per-client pipelines: each client annotates its
# own replay's alarms and nobody else's, which is why the client id is required
# here.


def _nudge(client_id: str) -> None:
    """Wake this client's open views so the change is pushed now.

    An operator action changes the alarm list, which no application publishes an
    event for; without this the page would show it only when the islanding
    detector next produced a result, up to a second later.
    """
    for session in SESSIONS.of(client_id):
        _offer(session.updates, None)


def _annotate(client_id: str, alarm_uuid: str, event_type: str, message: str) -> None:
    """Apply one operator event to this client's alarm, or 404.

    ``AlarmStore.annotate`` reports an unknown alarm by returning False, which
    this turns into a 404 rather than a success the caller cannot distinguish.
    """
    hub = live_hub(client_id)
    if not hub.alarms.annotate(alarm_uuid, event_type, message):
        raise HTTPException(status_code=404, detail=f"no alarm {alarm_uuid}")
    _nudge(client_id)


@router.post("/alarms/{alarm_uuid}/acknowledge", operation_id="islanding_acknowledge")
async def acknowledge(alarm_uuid: str, client_id: ClientId) -> CommandAck:
    """Mark an alarm as seen by an operator."""
    _annotate(client_id, alarm_uuid, "acknowledge", "Acknowledged by operator")
    return CommandAck(applied="acknowledge")


@router.post("/alarms/{alarm_uuid}/silence", operation_id="islanding_silence")
async def silence(alarm_uuid: str, client_id: ClientId) -> CommandAck:
    """Stop an alarm from asserting itself, without resolving it."""
    _annotate(client_id, alarm_uuid, "silence", "Silenced by operator")
    return CommandAck(applied="silence")


@router.post("/alarms/{alarm_uuid}/annotate", operation_id="islanding_annotate")
async def annotate(alarm_uuid: str, client_id: ClientId, body: AlarmNote) -> CommandAck:
    """Attach a free-text operator note to an alarm.

    The Qt dialogue's "Annotate" button. Upstream records this as a
    ``user_message`` event on the alarm rather than as a state change, so the
    same event type is used here and the two agree on the wire.
    """
    note = body.message.strip()[:MAX_NOTE]
    if not note:
        raise HTTPException(status_code=422, detail="note is empty")
    _annotate(client_id, alarm_uuid, "user_message", note)
    return CommandAck(applied="annotate")
