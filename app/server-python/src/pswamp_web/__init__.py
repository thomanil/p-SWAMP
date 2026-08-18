# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""A web front end for the p-SWAMP monitoring applications.

This package is the sibling of p-SWAMP's ``gui`` and ``visualization`` packages:
a third presentation adapter over the same Qt-free analysis core, this one
speaking WebSockets to a browser instead of drawing widgets. It is written to
live upstream as ``pswamp.web``, and is staged here only while the shape settles
-- so nothing inside it imports from this repo, and every import between its own
modules is relative. Moving it upstream is a directory move plus the import lines
in server.py that name it.

Unlike the app packages beside it, this one exports a ``lifespan`` but no
``router``: it owns the shared pipeline that its page packages -- app_status,
grid, time_window, phasors, islanding -- all read from. server.py enters it
before any of them.
"""

import asyncio
from contextlib import asynccontextmanager

from .hub import HUB

__all__ = ["HUB", "lifespan"]


@asynccontextmanager
async def lifespan(app):
    HUB.start(asyncio.get_running_loop())
    try:
        yield
    finally:
        # Off the loop: stopping joins application threads, and blocking the loop
        # during shutdown would stall the very tasks that need to finish first.
        await asyncio.to_thread(HUB.stop)
