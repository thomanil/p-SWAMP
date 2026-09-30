# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""Control state: the player's status, its stream changes, a pipeline's end.

The commands going up are in :mod:`.commands`. ``PlayerStatus`` is what a
client renders its controls from; ``StreamChanged`` tells consumers with a
window that the player opened a new stream; ``PipelineClosed`` tells the hosts
of a pipeline's modules that its key is gone.
"""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import Field

from .data_model import DataModel

__all__ = ["PipelineClosed", "PlayerStatus", "StreamChanged"]


class PlayerStatus(DataModel):
    """Where a stream's player is and what it can do -- what a client renders its
    controls from, so that no dead button ever appears (port doc §10.2)."""

    version: Literal["v1"] = "v1"
    timestamp: datetime
    mode: Literal["live", "replay"] = Field(
        description=(
            "What the active source is: 'replay' paces a history source's coverage "
            "and loops at its end (a replay over an explicit range ends paused "
            "instead); 'live' follows a live source from now, with no transport "
            "controls."
        )
    )
    source: str | None = Field(default=None, description="The active source's name.")
    sources: list[str] = Field(
        default_factory=list,
        description="Every source the player can switch to, in configured order.",
    )
    cursor: datetime | None = Field(description="The instant of the last frame played.")
    speed: float = Field(description="Replay speed multiplier; 1.0 is real time.")
    paused: bool
    loop: bool
    ended: bool = Field(description="The replay ran off the end and is not looping.")
    can_seek: bool = Field(
        description="Seek, step and speed apply: replay mode over a source with history."
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
    range_end: datetime | None = Field(
        default=None,
        description=(
            "Exclusive end of a bounded replay ('replay' with an 'end'); null when the "
            "replay runs to the history end."
        ),
    )
    error: str | None = Field(
        default=None,
        description=(
            "Why the stream stopped, when it stopped on a provider failure ('Type: text'); "
            "null otherwise. Cleared by the next play or seek."
        ),
    )


class StreamChanged(DataModel):
    """The player opened a new stream (a seek, or a loop restart). Consumers with
    a window re-prime themselves from ``cursor``."""

    version: Literal["v1"] = "v1"
    timestamp: datetime
    cursor: datetime | None


class PipelineClosed(DataModel):
    """A pipeline stopped (idle, evicted, shut down): a host drops the module
    instances it runs for that key instead of waiting out their idle timeout."""

    version: Literal["v1"] = "v1"
    timestamp: datetime
    reason: str = Field(default="stopped", description="Why: idle, capacity, shutdown.")
