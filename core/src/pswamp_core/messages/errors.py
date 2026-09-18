# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""The error topic: one message class anything in a pipeline may publish when
something *operational* has gone wrong.

A provider timed out, a module's ``process`` raised, a query was refused by a
remote service -- each is already a ``logger.error`` line in the process that
saw it. ``ErrorEvent`` is that line as a message on the pipeline's bus, so the
edge can show it to the person whose pipeline it was, on whatever page they
are looking at, rather than only to whoever reads the server log. The log stays
the source of truth; this is a copy of it addressed to the client.

It is not an alarm. Grid alarms (an islanding, a line outage) are the *result*
of an application running correctly and belong to the grid monitor's stores;
an ``ErrorEvent`` says a piece of the machinery is not running correctly.

Who publishes, today: ``Player`` when a provider fails mid-stream, ``Module.run``
when ``process`` raises, and any module that wants to say more (a batch module
whose query failed, carrying the ``request_id`` of the command that asked).
Providers do not: a ``DataClient`` has no bus, and its failures reach the
player or a module, which publish.
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
