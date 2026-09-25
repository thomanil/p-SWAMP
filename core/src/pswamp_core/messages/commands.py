# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""Commands: an operator's action, as a typed message going up.

Every command is a subclass of ``Command``, and **its class is its address**: a
receiver (the player, a module) declares the command classes it handles, and
the pipeline routes each command to the one receiver that declared its class
(:mod:`pswamp_core.command_routing`). There is no verb string and no ``args``
dict -- the fields are the arguments, validated where the command is built, so
a seek is a ``SeekCommand`` from the POST that made it to the method that
applies it.

The player's commands are defined here, because the player is core. A module's
own commands live beside the module, exactly as its ``ResultEnvelope`` subclass
does (the time series explorer's ``CountRangeCommand`` is the worked example).

``target`` is optional and rarely needed: only when two receivers in one
pipeline handle the same class does it name which (a receiver's ``name``).
``request_id`` is generated when the command is built, logged with it, and
carried on whatever is produced in answer -- a module's result, an
``ErrorEvent`` when the command was refused.

The POST that published a command answers only with an acknowledgement; the
*effect* arrives as the next ``PlayerStatus`` or result on the socket, so state
keeps its one path.
"""

from __future__ import annotations

from datetime import datetime
from typing import ClassVar, Literal
from uuid import uuid4

from pydantic import Field, field_validator, model_validator

from ..util.time import ensure_utc
from .data_model import DataModel, topic_from_name

__all__ = [
    "Command",
    "GoLiveCommand",
    "PauseCommand",
    "PlayCommand",
    "PlayerCommand",
    "RefreshCommand",
    "ReplayCommand",
    "SeekCommand",
    "SpeedCommand",
    "StepCommand",
]


def _new_request_id() -> str:
    return uuid4().hex


def _utc_or_none(value: datetime | None) -> datetime | None:
    return None if value is None else ensure_utc(value)


class _CommandName:
    """``Command.name``: the class name minus ``Command``, dotted like a topic --
    ``SeekCommand`` is ``seek``, ``GoLiveCommand`` is ``go.live``. What logs
    and the POST's acknowledgement call the command."""

    def __get__(self, instance: object, owner: type[Command]) -> str:
        return topic_from_name(owner.__name__.removesuffix("Command") or owner.__name__)


class Command(DataModel):
    """One operator action. Subclass it; the subclass is the address."""

    version: Literal["v1"] = "v1"
    request_id: str = Field(default_factory=_new_request_id)
    client_id: str | None = Field(default=None, description="The issuing client, at the edge.")
    target: str | None = Field(
        default=None,
        description=(
            "The receiver's name, only when two receivers in one pipeline handle "
            "this class; null routes by class alone."
        ),
    )

    #: The command's short name (``seek``, ``count.range``); derived, not a field.
    name: ClassVar[_CommandName] = _CommandName()


# --- the player's commands -------------------------------------------------------


class PlayerCommand(Command):
    """Base of every command the player handles."""


class PlayCommand(PlayerCommand):
    """Start or resume the replay."""


class PauseCommand(PlayerCommand):
    """Pause the replay where it is."""


class StepCommand(PlayerCommand):
    """Play ``n`` frames at once, unpaced; negative ``n`` steps back."""

    n: int = Field(default=1, description="Frames to step; negative steps back.")


class SeekCommand(PlayerCommand):
    """Reposition the replay: at an instant, or an offset into the history."""

    to: datetime | None = Field(default=None, description="The instant to seek to.")
    offset_s: float | None = Field(
        default=None, ge=0, description="Seconds from the start of the history."
    )

    @field_validator("to")
    @classmethod
    def _utc(cls, value: datetime | None) -> datetime | None:
        return _utc_or_none(value)

    @model_validator(mode="after")
    def _one_position(self) -> SeekCommand:
        if (self.to is None) == (self.offset_s is None):
            raise ValueError("a seek needs exactly one of 'to' and 'offset_s'")
        return self


class SpeedCommand(PlayerCommand):
    """Change the replay speed."""

    speed: float = Field(gt=0, description="Replay speed multiplier; 1 is real time.")


class GoLiveCommand(PlayerCommand):
    """Switch to the live stream."""


class ReplayCommand(PlayerCommand):
    """Switch to (or restart) the replay, optionally bounded to a range.

    With neither ``start`` nor ``offset_s`` it starts at the beginning of the
    history; with ``end`` or ``end_offset_s`` it ends paused there instead of
    running on (or looping). It lands paused unless ``play``.
    """

    start: datetime | None = Field(default=None, description="Where to start.")
    offset_s: float | None = Field(
        default=None, ge=0, description="Where to start, in seconds from the history start."
    )
    end: datetime | None = Field(default=None, description="Exclusive end of a bounded replay.")
    end_offset_s: float | None = Field(
        default=None, ge=0, description="Exclusive end, in seconds from the history start."
    )
    play: bool = Field(default=False, description="Start playing rather than land paused.")

    @field_validator("start", "end")
    @classmethod
    def _utc(cls, value: datetime | None) -> datetime | None:
        return _utc_or_none(value)

    @model_validator(mode="after")
    def _at_most_one_each(self) -> ReplayCommand:
        if self.start is not None and self.offset_s is not None:
            raise ValueError("a replay takes 'start' or 'offset_s', not both")
        if self.end is not None and self.end_offset_s is not None:
            raise ValueError("a replay takes 'end' or 'end_offset_s', not both")
        if self.start is not None and self.end is not None and self.end <= self.start:
            raise ValueError("a replay's end must be after its start")
        return self


class RefreshCommand(PlayerCommand):
    """Ask the gateway again what it holds; plays nothing."""
