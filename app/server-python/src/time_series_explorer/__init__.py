"""The Time Series Explorer app: a range explorer over a query-answering provider.

Same public surface as every app package — src/server.py uses nothing else:

  router      the endpoints, mounted under /api/time-series-explorer
  WS_MESSAGE  the model this app pushes down its socket
  lifespan    binds the pipeline registry to the loop; drains it on shutdown

Note the spelling difference: this directory has to be a Python identifier,
while its URL prefix is hyphenated to match the web client's route —
`time_series_explorer` vs `/api/time-series-explorer`.
"""

from .api import TimeSeriesExplorerState, lifespan, router

WS_MESSAGE = TimeSeriesExplorerState

__all__ = ["WS_MESSAGE", "TimeSeriesExplorerState", "lifespan", "router"]
