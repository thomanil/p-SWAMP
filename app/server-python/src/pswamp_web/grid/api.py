# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""The grid topology, over plain HTTP.

The only endpoint in this package that is not a WebSocket, and deliberately so:
the topology is static, so a page fetches it once on mount and the browser is
free to cache it. Putting it on a socket would mean every map-drawing page's hook
had to handle a second message shape to receive something that never changes.
"""

from fastapi import APIRouter

from ..grid_model import load_grid_model
from ..wire import GridModel

router = APIRouter()


@router.get("/model", operation_id="grid_model")
async def grid_model() -> GridModel:
    """The static Nordic 44 topology: buses, branches, PMU sites and a bounding
    box. Fetched once per page mount and cacheable, as above."""
    return load_grid_model()
