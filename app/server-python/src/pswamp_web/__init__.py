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
``router``: it owns the registry of per-client pipelines that its page packages
-- app_status, grid, time_window, phasors, islanding, line_outage -- all draw
theirs from. server.py enters it before any of them.
"""

import asyncio
from contextlib import asynccontextmanager

from .hub import REGISTRY

__all__ = ["REGISTRY", "lifespan"]


@asynccontextmanager
async def lifespan(app):
    """Bind the registry to the loop, and drain it on the way out.

    Deliberately starts *no* pipeline: there is one per client now, built on
    first connect, so an idle server runs no replay and no application threads at
    all. What has to happen here is only the two things a pipeline cannot do for
    itself -- learn which loop the bus should hand results to, and get torn down
    when the process is going away.
    """
    REGISTRY.bind(asyncio.get_running_loop())
    try:
        yield
    finally:
        # Off the loop, and concurrently: stopping a pipeline joins its
        # application threads, so doing them in sequence would make shutdown
        # scale with the number of live clients.
        await REGISTRY.stop_all()
        REGISTRY.bind(None)
