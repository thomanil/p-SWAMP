# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""The result envelope: what a module publishes.

STEP3 §4.3. The desktop convention ``{time_stamp, info{app_name, uuid},
parameters, result}`` as a declared, JSON-native model. A module's output is a
subclass of :class:`Result` (streaming) or :class:`Report` (batch); the topic
comes from the subclass name, and ``request_id`` is what ties a batch report back
to the command that asked for it (STEP3 §8.4, the request/response slot).
"""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field

from .base import DataModel

__all__ = ["ModuleRef", "Report", "Result"]

ParameterValue = float | int | str | bool | None


class ModuleRef(BaseModel):
    """Which module produced a result."""

    name: str
    uuid: str


class Result(DataModel):
    """Base for every module output. Subclasses pin ``version`` and add fields."""

    module: ModuleRef
    parameters: dict[str, ParameterValue] = Field(default_factory=dict)
    request_id: str | None = Field(
        default=None,
        description="Set when this result answers a specific command (a job).",
    )


class Report(Result):
    """Base for batch outputs: one result over a bounded range of the source.

    Raw samples never leave the backend (the rig document's K3 rule); what a
    browser sees is this derived model, with the range it was computed over.
    """

    range_start: datetime | None = None
    range_end: datetime | None = None
    n_samples: int = Field(description="How many source rows the report is over.")
