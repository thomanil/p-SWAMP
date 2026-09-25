# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""``GET /api/errors/ws?client_id=``: this client's error notices, as they happen.

On connect it sends what the hub kept for this client (a failure just before a
reload is not lost), then each new notice as the hub fans it out. Downstream
only; there is nothing to command. The web layout opens exactly one of these,
outside any page, so it survives navigation.

Reads the web-layer helpers from ``pswamp_web`` rather than ``shared``: this
package feeds ``shared`` (which re-exports the hub and the forwarder), and
reading from it too would be an import cycle.
"""

from __future__ import annotations

import asyncio
import contextlib

from fastapi import APIRouter, WebSocket

from pswamp_web.log import get_logger
from pswamp_web.pump import wait_for_disconnect
from pswamp_web.wire import read_client_id, send_state

from .hub import HUB

logger = get_logger("errors")

router = APIRouter()


@router.websocket("/ws")
async def ws_endpoint(ws: WebSocket) -> None:
    client_id = read_client_id(ws)
    if client_id is None:
        await ws.close(code=1008)
        return
    await ws.accept()
    with HUB.watching(client_id) as notices:
        for notice in HUB.recent(client_id):
            await send_state(ws, notice)

        async def push() -> None:
            while True:
                await send_state(ws, await notices.get())

        pusher = asyncio.create_task(push())
        try:
            await wait_for_disconnect(ws)
        finally:
            pusher.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await pusher
