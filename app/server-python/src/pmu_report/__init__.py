"""The PMU report app: batch reports over the PMU stream, asked for by POST and
answered on the socket.

Same public surface as every app package — server.py uses nothing else:

  router      the endpoints, mounted under /api/pmu-report
  WS_MESSAGE  the model this app pushes down its socket
  lifespan    the task that consumes finished reports from the bus

Note the spelling difference: this directory is `pmu_report` because server.py
imports it as a Python module, while its URL prefix is `/api/pmu-report`.
"""

from .api import PmuReportState, lifespan, router

WS_MESSAGE = PmuReportState

__all__ = ["WS_MESSAGE", "PmuReportState", "lifespan", "router"]
