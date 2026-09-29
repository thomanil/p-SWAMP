# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""The wire shapes between the core and a remote data service.

A deployment's own data service -- whatever store it fronts -- is reached
through a ``DataClient``
(:class:`pswamp_core.datagateway.clients.remote_data.RemoteDataClient`).
The client and the service exchange two shapes over one HTTP call, and both are
defined here so the service's implementer and the client share one spelling:

* **``RemoteDataQuery``** goes *up*, as the JSON body of ``POST /v1/queries``:
  "give me ``model`` between ``start`` and ``end``", tagged with a ``query_id``
  the client made up, which exists only so both sides' logs can name the same
  query.
* **``RemoteDataResult``** comes *down*, as one line of that call's streamed
  NDJSON response body. It is an *envelope*: the PMU record itself rides inside
  as plain JSON (``record``), beside a ``kind`` that says whether this line is a
  record, the end of the query, or a failure.

Why an envelope rather than bare ``PmuFrame`` lines: the response's status code
is sent before the first record, so a failure half way through the range, and
the difference between "the query is over" and "the connection broke", can only
be said *in the body*. That is all the envelope carries -- the connection
itself says which query a line answers, so there is no correlation id or
sequence number on a line. The record stays a ``model_dump()`` of the real
model, so the client validates it against that model's schema on the way in and
nothing about ``PmuFrame`` is repeated here.

Rules the client depends on, and the service must keep:

* One JSON object per line (``\\n``-terminated), UTF-8.
* ``record`` lines in ascending record ``timestamp`` order.
* Exactly one ``end`` or ``error`` line closes the body, and nothing follows it.
  A body that stops without one is a broken connection, not an empty answer.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator, model_validator

from ..util.time import ensure_utc
from .data_model import DataModel

__all__ = ["ResultKind", "RemoteDataQuery", "RemoteDataResult"]

ResultKind = Literal["record", "end", "error"]


class RemoteDataQuery(BaseModel):
    """A range query, as the client POSTs it to the service.

    ``model`` is the *topic string* of the message class wanted (``pmu.frame``),
    which is how the core names a message class on the wire.
    A ``None`` bound is open: ``start`` ``None`` is "from the earliest you
    hold", ``end`` ``None`` is "to the latest you hold". The window is
    half-open, ``[start, end)``.
    """

    version: Literal["v1"] = "v1"
    query_id: str = Field(
        description="Client-generated id, for correlating the two sides' logs. Not echoed on the lines."
    )
    model: str = Field(description="Topic string of the message class wanted, e.g. 'pmu.frame'.")
    start: datetime | None = Field(default=None, description="Inclusive start; null is unbounded.")
    end: datetime | None = Field(default=None, description="Exclusive end; null is unbounded.")
    mrid: list[str] | None = Field(
        default=None, description="Restrict to these stream identities; null is every one."
    )

    @field_validator("start", "end")
    @classmethod
    def _utc(cls, value: datetime | None) -> datetime | None:
        return None if value is None else ensure_utc(value)


class RemoteDataResult(BaseModel):
    """One line of the streamed response to ``POST /v1/queries``.

    Exactly one of three shapes, by ``kind``:

    * ``record`` -- ``model`` names the record's class by topic string and
      ``record`` is its ``model_dump(mode="json")``.
    * ``end`` -- the query is complete; ``count`` is how many records it sent.
    * ``error`` -- the query failed; ``error`` says why. Nothing follows.

    Serialised with ``exclude_none``, so a record line carries no ``count`` or
    ``error`` and a terminal line no ``record``.
    """

    kind: ResultKind
    model: str | None = Field(default=None, description="'record' only: the record's topic string.")
    record: dict[str, Any] | None = Field(
        default=None, description="'record' only: the record as JSON, validated by the client."
    )
    count: int | None = Field(default=None, ge=0, description="'end' only: records sent.")
    error: str | None = Field(default=None, description="'error' only: what went wrong.")

    @model_validator(mode="after")
    def _shape_matches_kind(self) -> RemoteDataResult:
        if self.kind == "record" and (self.model is None or self.record is None):
            raise ValueError("a 'record' line needs 'model' and 'record'")
        if self.kind == "end" and self.count is None:
            raise ValueError("an 'end' line needs 'count'")
        if self.kind == "error" and not self.error:
            raise ValueError("an 'error' line needs 'error'")
        return self

    # -- constructors the service side uses --------------------------------------

    @classmethod
    def for_record(cls, message: DataModel) -> RemoteDataResult:
        return cls(kind="record", model=type(message).topic, record=message.model_dump(mode="json"))

    @classmethod
    def ended(cls, count: int) -> RemoteDataResult:
        return cls(kind="end", count=count)

    @classmethod
    def failed(cls, error: str) -> RemoteDataResult:
        return cls(kind="error", error=error)

    def to_line(self) -> bytes:
        """This envelope as one NDJSON line, newline included."""
        return self.model_dump_json(exclude_none=True).encode() + b"\n"
