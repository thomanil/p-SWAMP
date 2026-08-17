# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""Which monitoring applications are running, and what they report.

The web counterpart of p-SWAMP's ``gui.app_monitoring.AppStatusMonitoringWidget``.

Applications report status once a second, but the table has to update more often
than that: an application that dies stops sending, and "stale" is exactly the
condition of *nothing* having arrived. Nothing to react to means a ticker rather
than a subscription -- the same reason the Qt widget polls on a timer.

The Stop and Open-console buttons from the Qt version are deliberately not here:
they publish to a command topic that has no meaning without a broker.
"""

import asyncio
import contextlib

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from ..hub import HUB
from ..wire import AppStatusTable, send_state
import time

# Twice the reporting rate, so a status change shows up within about half a
# second and staleness is noticed promptly without polling hard.
PUSH_HZ = 2

router = APIRouter()


def status_message() -> AppStatusTable:
    return AppStatusTable(
        apps=HUB.statuses.table(),
        server_time=time.time(),
        replay=HUB.replay_status(),
    )


@router.websocket("/ws")
async def ws_endpoint(ws: WebSocket) -> None:
    await ws.accept()

    async def push() -> None:
        interval = 1 / PUSH_HZ
        while True:
            await send_state(ws, status_message())
            await asyncio.sleep(interval)

    pusher = asyncio.create_task(push())
    try:
        # Nothing is sent by this page, but the receive loop is what surfaces a
        # disconnect: without it a closed socket is only noticed on the next
        # send, and a client that goes away between ticks would linger.
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        pass
    except Exception:
        pass
    finally:
        pusher.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await pusher
