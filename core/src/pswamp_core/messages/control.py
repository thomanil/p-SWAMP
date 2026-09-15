# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""Control messages: a command going up, and the player's state coming down.

``Command`` is how an operator's action reaches the pipeline. At the web edge a
``POST`` becomes one of these and is published on the client's bus; whoever it
is addressed to (``target``: the player, or a module's uuid) picks it up there.
The POST's reply is only an acknowledgement -- the *effect* arrives as the next
``PlayerStatus`` or result on the socket, so state keeps its one path.

``request_id`` is the correlation id STEP 1 A7 asked for: generated when the
command is built, logged with it, and carried on any result produced in answer.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal
from uuid import uuid4

from pydantic import Field

from .data_model import DataModel

__all__ = ["Command", "PlayerStatus", "StreamChanged"]


def _new_request_id() -> str:
    return uuid4().hex


class Command(DataModel):
    """One operator action, addressed to the player or to a module."""

    version: Literal["v1"] = "v1"
    request_id: str = Field(default_factory=_new_request_id)
    client_id: str | None = Field(default=None, description="The issuing client, at the edge.")
    target: str | None = Field(
        default=None, description="A module uuid, or None for the stream's player."
    )
    verb: str = Field(description="What to do: play, stop, step, seek, speed, ...")
    args: dict[str, Any] = Field(default_factory=dict)


class PlayerStatus(DataModel):
    """Where a stream's player is and what it can do -- what a client renders its
    controls from, so that no dead button ever appears (port doc §10.2)."""

    version: Literal["v1"] = "v1"
    timestamp: datetime
    mode: Literal["live", "replay"] = Field(
        description=(
            "Which stream is open: 'replay' paces a bounded stream over the history "
            "coverage and loops at its end; 'live' follows the source from now, with "
            "no transport controls."
        )
    )
    cursor: datetime | None = Field(description="The instant of the last frame played.")
    speed: float = Field(description="Replay speed multiplier; 1.0 is real time.")
    paused: bool
    loop: bool
    ended: bool = Field(description="The replay ran off the end and is not looping.")
    can_seek: bool = Field(
        description="Seek, step and speed apply: replay mode over a source with history."
    )
    can_go_live: bool = Field(
        default=False, description="A live source exists; the 'live' command switches to it."
    )
    coverage_start: datetime | None = Field(
        description="Earliest instant of the seekable history; null without a history source."
    )
    coverage_end: datetime | None = Field(
        description="Exclusive end of the seekable history; null without a history source."
    )
    frame_interval_s: float | None = Field(
        description="Seconds between frames, once two have been seen."
    )


class StreamChanged(DataModel):
    """The player opened a new stream (a seek, or a loop restart). Consumers with
    a window re-prime themselves from ``cursor``."""

    version: Literal["v1"] = "v1"
    timestamp: datetime
    cursor: datetime | None
