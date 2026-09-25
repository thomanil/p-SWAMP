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
  ``AppStatusMessage``, ``PlayerStatus``, ``StreamChanged``, in ``results``
  and ``control``; ``ErrorEvent`` (an operational failure, for the edge to
  show) in ``errors``;
* **commands** -- ``Command``, the typed base every operator action derives
  from, and the player's commands (``SeekCommand``, ``PlayCommand``, ...), in
  ``commands``. A module's own commands live beside the module.

Beside them, in ``remote_data``, the two shapes a remote data service
speaks: ``RemoteDataQuery`` (a range query going up over REST) and
``RemoteDataResult`` (one line of the streamed response that answers it).
"""

from .commands import (
    Command,
    GoLiveCommand,
    PauseCommand,
    PlayCommand,
    PlayerCommand,
    RefreshCommand,
    ReplayCommand,
    SeekCommand,
    SpeedCommand,
    StepCommand,
)
from .control import PlayerStatus, StreamChanged
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
    "GoLiveCommand",
    "PauseCommand",
    "PlayCommand",
    "PlayerCommand",
    "PlayerStatus",
    "PmuFrame",
    "PmuHeader",
    "RefreshCommand",
    "RemoteDataQuery",
    "RemoteDataResult",
    "ReplayCommand",
    "ResultEnvelope",
    "SeekCommand",
    "SpeedCommand",
    "StepCommand",
    "StreamChanged",
    "sent_at",
    "stamp_sent_at",
    "topic_from_name",
]
