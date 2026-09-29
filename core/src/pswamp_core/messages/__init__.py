# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""Every message that crosses a topic, a socket or a process boundary.

One rule: **a message is a versioned pydantic ``DataModel``, and its topic is
its class name.** So a message can be logged, validated on receipt, and
published into the browser-facing OpenAPI contract without an adapter.

* **measurements** -- ``PmuFrame``, carrying its ``PmuHeader`` (``pmu``);
* **commands** going up -- ``Command`` and the player's commands (``commands``);
* **results and control** coming down -- ``ResultEnvelope`` (``results``),
  ``PlayerStatus`` and ``StreamChanged`` (``control``), ``ErrorEvent`` (``errors``).
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
from .data_model import DataModel, topic_from_name
from .errors import ErrorEvent
from .pmu import PmuFrame, PmuHeader
from .results import AppIdentity, AppStatus, ResultEnvelope

__all__ = [
    "AppIdentity",
    "AppStatus",
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
    "ReplayCommand",
    "ResultEnvelope",
    "SeekCommand",
    "SpeedCommand",
    "StepCommand",
    "StreamChanged",
    "topic_from_name",
]
