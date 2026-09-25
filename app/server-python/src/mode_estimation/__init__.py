"""The Mode estimation app: p-SWAMP's N4SID mode estimation as a core module,
under load -- in-process, or as its own service over a transport.

Same public surface as every app package — src/server.py uses nothing else:

  router      the endpoints, mounted under /api/mode-estimation
  WS_MESSAGE  the model this app pushes down its socket
  lifespan    binds the pipeline registry to the loop; drains it on shutdown

Note the spelling difference: this directory has to be a Python identifier,
while its URL prefix is hyphenated to match the web client's route —
`mode_estimation` vs `/api/mode-estimation`.
"""

from .api import ModeEstimationState, lifespan, router

WS_MESSAGE = ModeEstimationState

__all__ = ["WS_MESSAGE", "ModeEstimationState", "lifespan", "router"]
