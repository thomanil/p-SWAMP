# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""Every message that crosses a topic, a socket or a process boundary.

One rule: **a message is a versioned pydantic ``DataModel``**, and its topic is
its class name. No pickle, no numpy arrays, no ``uuid.UUID`` or ``datetime``
objects hidden inside a dict -- so a message can be logged, inspected with
standard tooling, validated on receipt, and published into the browser-facing
OpenAPI contract without an adapter.

Two layers of message live here (STEP 1 A1, "two layers, not one"):

* **measurements** -- ``PmuHeader`` (the channel layout, sent once) and
  ``PmuFrame`` (one instant of every channel), in ``pmu``;
* **results and control** -- ``ResultEnvelope`` (what a module emits),
  ``AppStatusMessage``, ``Command``, ``PlayerStatus``, ``StreamChanged``, in
  ``results`` and ``control``; ``ErrorEvent`` (an operational failure, for the
  edge to show) in ``errors``.

Beside them, in ``time_series``, the two shapes a remote time-series store
speaks: ``TimeSeriesQuery`` (a range query going up over REST) and
``TimeSeriesResult`` (the envelope each answer rides in on a Kafka topic).
"""

from .control import Command, PlayerStatus, StreamChanged
from .data_model import DataModel, topic_from_name
from .errors import ErrorEvent
from .pmu import PmuFrame, PmuHeader
from .results import AppIdentity, AppStatus, AppStatusMessage, ResultEnvelope
from .time_series import TimeSeriesQuery, TimeSeriesResult

__all__ = [
    "AppIdentity",
    "AppStatus",
    "AppStatusMessage",
    "Command",
    "DataModel",
    "ErrorEvent",
    "PlayerStatus",
    "PmuFrame",
    "PmuHeader",
    "ResultEnvelope",
    "StreamChanged",
    "TimeSeriesQuery",
    "TimeSeriesResult",
    "topic_from_name",
]
