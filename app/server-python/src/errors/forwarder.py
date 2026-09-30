# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""``forward_errors``: an app's error topic, into the hub.

Every ``ErrorEvent`` of an app's pipelines -- the player's, a module's wherever
it runs, a refused command's -- is published on the app's error topic
(``<app>.error.event``) under the pipeline's key, which for a per-client
pipeline is the client id. One forwarder per app tails that topic across every
key and hands each event to the hub with the key and the app's slug, which is
all the hub needs. The transport is handed in, because this package must not
import ``shared`` (see ``__init__.py``).
"""

from __future__ import annotations

from pswamp_core.messages import ErrorEvent
from pswamp_core.subscription import Overflow
from pswamp_core.transport import Transport

from .hub import HUB, ErrorHub

__all__ = ["forward_errors"]


async def forward_errors(transport: Transport, app: str, hub: ErrorHub = HUB) -> None:
    """Hand every ``ErrorEvent`` on ``app``'s error topic to ``hub``, until cancelled."""
    with transport.subscribe(ErrorEvent, app=app, overflow=Overflow.GROW) as errors:
        async for key, event in errors:
            hub.publish(key, app, event)
