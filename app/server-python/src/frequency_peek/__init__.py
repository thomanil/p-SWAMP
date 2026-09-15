"""The Frequency peek app: a module over the core, and the page that shows it.

Same public surface as every app package — src/server.py uses nothing else:

  router      the endpoints, mounted under /api/frequency-peek
  WS_MESSAGE  the model this app pushes down its socket
  lifespan    binds the pipeline registry to the loop; drains it on shutdown

Note the spelling difference: this directory has to be a Python identifier,
while its URL prefix is hyphenated to match the web client's route —
`frequency_peek` vs `/api/frequency-peek`.
"""

from .api import FrequencyPeekState, lifespan, router

WS_MESSAGE = FrequencyPeekState

__all__ = ["WS_MESSAGE", "FrequencyPeekState", "lifespan", "router"]
