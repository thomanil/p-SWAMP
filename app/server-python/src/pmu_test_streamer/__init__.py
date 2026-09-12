"""The PMU test streamer app: replays one PMU stream per client, through the data gateway.

Same public surface as every app package — server.py uses nothing else:

  router      the endpoints, mounted by server.py under /api/pmu-test-streamer
  WS_MESSAGE  the model this app pushes down its socket

No `lifespan` any more: the playback ticker this package used to run is gone.
Pacing is the gateway's replay cursor, one per connected client, started and
stopped by the socket handler. The data comes from the `pmu_data` service
package, which server.py enters before any app.

Note the spelling difference: this directory is `pmu_test_streamer` because
server.py imports it as a Python module, while its URL prefix is the hyphenated
`/api/pmu-test-streamer`, matching the web client's route.
"""

from .api import PmuStreamState, router

WS_MESSAGE = PmuStreamState

__all__ = ["WS_MESSAGE", "PmuStreamState", "router"]
