# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""The one message the errors socket pushes."""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field

__all__ = ["ErrorNotice"]


class ErrorNotice(BaseModel):
    """An ``ErrorEvent`` from one of the client's pipelines, tagged with which app's.

    ``type`` is ``"state"`` like every other socket message in this backend --
    the web client's socket hook forwards only those -- even though a notice is
    an event rather than a snapshot. ``id`` lets the tray dismiss one and
    de-duplicate a replay after a reconnect.
    """

    type: Literal["state"] = "state"
    id: str = Field(description="Unique per notice; the tray keys and dismisses by it.")
    app: str = Field(description="The app slug whose pipeline raised it, e.g. 'time-series-explorer'.")
    source: str = Field(description="Who saw it: 'player', a module's name.")
    message: str = Field(description="One line for a person.")
    detail: str | None = Field(default=None, description="The exception, as 'Type: text'.")
    request_id: str | None = Field(default=None, description="The Command it answers, if any.")
    timestamp: datetime = Field(description="When the failure was seen.")
