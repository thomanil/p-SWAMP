"""The Reference example app.

Same public surface as every app package — src/server.py uses nothing else:

  router      the endpoints, mounted under /api/reference-subapp
  WS_MESSAGE  optional; the model this app pushes down its socket
  lifespan    optional; startup/shutdown work — see src/pswamp_web/

Note the spelling difference: this directory has to be a Python identifier,
while its URL prefix is hyphenated to match the web client's route —
`reference_subapp` vs `/api/reference-subapp`.
"""

from .api import ReferenceSubappState, router

WS_MESSAGE = ReferenceSubappState

__all__ = ["WS_MESSAGE", "ReferenceSubappState", "router"]
