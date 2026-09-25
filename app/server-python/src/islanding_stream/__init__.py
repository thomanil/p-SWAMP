"""The Islanding stream app: p-SWAMP's islanding detector as a core module,
under load -- in-process, or as its own service over a transport.

Same public surface as every app package — src/server.py uses nothing else:

  router      the endpoints, mounted under /api/islanding-stream
  WS_MESSAGE  the model this app pushes down its socket
  lifespan    binds the pipeline registry to the loop; drains it on shutdown

Note the spelling difference: this directory has to be a Python identifier,
while its URL prefix is hyphenated to match the web client's route —
`islanding_stream` vs `/api/islanding-stream`.
"""

from .api import IslandingStreamState, lifespan, router

WS_MESSAGE = IslandingStreamState

__all__ = ["WS_MESSAGE", "IslandingStreamState", "lifespan", "router"]
