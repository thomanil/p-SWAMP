# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""What a module emits: a result envelope.

A module subclasses ``ResultEnvelope`` with a concrete ``result`` type, and
**that subclass's name is its topic**::

    class FrameStats(BaseModel): ...
    class FrameStatsResult(ResultEnvelope[FrameStats]): ...   # topic frame.stats.result

``request_id`` is set when the result answers a ``Command``, so a result on a
shared bus can be routed back to whoever asked.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, Generic, Literal, TypeVar

from pydantic import BaseModel, Field

from .data_model import DataModel

__all__ = ["AppIdentity", "AppStatus", "ResultEnvelope"]


class AppStatus(str, Enum):
    """The status vocabulary of a module. ``str`` so it serialises plainly."""

    OK = "OK"
    ALERT = "Alert"
    EMERGENCY = "Emergency"
    INITIALIZING = "Initializing..."
    UNDEFINED = "Undefined"


class AppIdentity(BaseModel):
    """Which module instance produced a message."""

    name: str = Field(description="The module's name, as registered.")
    uuid: str = Field(description="This instance's id, unique per process.")


T = TypeVar("T", bound=BaseModel)


class ResultEnvelope(DataModel, Generic[T]):
    """The shared envelope around a module's declared result model."""

    version: Literal["v1"] = "v1"
    timestamp: datetime = Field(description="The instant the result is about.")
    app: AppIdentity
    parameters: dict[str, Any] = Field(
        default_factory=dict, description="The module's settings, for the record."
    )
    request_id: str | None = Field(
        default=None, description="Set when this result answers a Command."
    )
    result: T
