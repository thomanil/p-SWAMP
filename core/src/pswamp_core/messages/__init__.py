# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""Every message that crosses a topic, a socket or a process boundary.

One rule: **a message is a versioned pydantic ``DataModel``**, and its topic is
its class name. No pickle, no numpy arrays, no ``uuid.UUID`` or ``datetime``
objects hidden inside a dict -- so a message can be logged, inspected with
standard tooling, validated on receipt, and published into the browser-facing
OpenAPI contract without an adapter.

Two layers of message live here (STEP 1 A1, "two layers, not one"):

* **measurements** -- ``PmuFrame`` (one instant of every channel, carrying
  its ``PmuHeader``, the channel layout, inside it), in ``pmu``;
* **results and control** -- ``ResultEnvelope`` (what a module emits),
  ``AppStatusMessage``, ``Command``, ``PlayerStatus``, ``StreamChanged``, in
  ``results`` and ``control``; ``ErrorEvent`` (an operational failure, for the
  edge to show) in ``errors``.

Beside them, in ``remote_data``, the two shapes a remote data service
speaks: ``RemoteDataQuery`` (a range query going up over REST) and
``RemoteDataResult`` (the envelope each answer rides in on a Kafka topic).
"""

from .control import Command, PlayerStatus, StreamChanged
from .data_model import DataModel, sent_at, stamp_sent_at, topic_from_name
from .errors import ErrorEvent
from .pmu import PmuFrame, PmuHeader
from .remote_data import RemoteDataQuery, RemoteDataResult
from .results import AppIdentity, AppStatus, AppStatusMessage, ResultEnvelope

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
    "RemoteDataQuery",
    "RemoteDataResult",
    "ResultEnvelope",
    "StreamChanged",
    "sent_at",
    "stamp_sent_at",
    "topic_from_name",
]
