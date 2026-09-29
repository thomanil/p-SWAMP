# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""The error topic: what anything in a pipeline publishes when something
*operational* has gone wrong.

A provider failed mid-stream, a module's ``process`` raised, a command was
refused after it was accepted. Each is also a log line; ``ErrorEvent`` is that
line as a message on the pipeline's bus, so the edge can pass it to the person
whose pipeline it was. It is not a grid alarm: an alarm is the *result* of an
application running correctly.
"""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import Field

from .data_model import DataModel

__all__ = ["ErrorEvent"]


class ErrorEvent(DataModel):
    """An operational failure in a pipeline (topic ``error.event``)."""

    version: Literal["v1"] = "v1"
    timestamp: datetime = Field(description="When the failure was seen.")
    source: str = Field(description="Who saw it: 'player', a module's name, a client's name.")
    message: str = Field(description="One line for a person: what stopped, or what failed.")
    detail: str | None = Field(
        default=None, description="The exception, as 'Type: text', or another specific cause."
    )
    request_id: str | None = Field(
        default=None, description="The Command this failure answers, when there was one."
    )
