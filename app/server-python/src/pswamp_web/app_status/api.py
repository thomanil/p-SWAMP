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

import time

from fastapi import APIRouter, WebSocket

from ..hub import Hub, connected_hub
from ..pump import serve_ticks
from ..wire import AppStatusTable

# Twice the reporting rate, so a status change shows up within about half a
# second and staleness is noticed promptly without polling hard.
PUSH_HZ = 2

router = APIRouter()


def status_message(hub: Hub) -> AppStatusTable:
    return AppStatusTable(
        apps=hub.statuses.table(),
        server_time=time.time(),
        replay=hub.replay_status(),
    )


@router.websocket("/ws")
async def ws_endpoint(ws: WebSocket) -> None:
    async with connected_hub(ws) as hub:
        if hub is None:
            return
        await serve_ticks(ws, PUSH_HZ, lambda: status_message(hub))
