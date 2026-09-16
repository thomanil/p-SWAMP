# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""The wire shapes between the core and a remote time-series store.

A time-series database behind a deployment's own REST api is a ``DataClient``
(:class:`pswamp_core.datagateway.clients.time_series_database.TimeSeriesDatabaseClient`).
The client speaks two things to that service, and both are defined here so the
service's implementer and the client share one spelling:

* **``TimeSeriesQuery``** goes *up*, as the JSON body of ``POST /v1/queries``:
  "give me ``model`` between ``start`` and ``end``", tagged with a ``query_id``
  the client made up.
* **``TimeSeriesResult``** comes *down*, as the value of every record the
  service publishes on the results Kafka topic in answer. It is an *envelope*:
  the PMU record itself rides inside as plain JSON (``record``), beside the
  ``query_id`` it answers, a per-query sequence number, and a ``kind`` that
  says whether this is a record, the end of the query, or a failure.

Why an envelope rather than the bare ``PmuFrame`` on the topic: a topic is a
shared channel, and the client needs three things a bare record cannot carry
-- *which query* a record answers (several pipelines query one service at
once), *that the query is over* (a bounded ``consume`` has to return), and
*that it failed* (rather than time out). The record stays a
``model_dump()`` of the real model, so the client validates it against that
model's schema on the way in and nothing about ``PmuFrame`` is repeated here.

Ordering rules the client depends on, and the service must keep:

* One results topic, **one partition** (or one partition per ``query_id``, if
  keyed): Kafka orders within a partition only, and the gateway's watermark
  drops out-of-order records.
* The Kafka **record key is the ``query_id``**.
* ``seq`` starts at 0 and increases by one per envelope of a query; records
  are in ascending ``timestamp`` order; exactly one ``end`` or ``error``
  envelope closes a query, and nothing follows it.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator, model_validator

from ..util.time import ensure_utc
from .data_model import DataModel

__all__ = ["ResultKind", "TimeSeriesQuery", "TimeSeriesResult"]

ResultKind = Literal["record", "end", "error"]


class TimeSeriesQuery(BaseModel):
    """A range query, as the client POSTs it to the service.

    ``model`` is the *topic string* of the message class wanted (``pmu.frame``,
    ``pmu.header``), which is how the core names a message class on the wire.
    A ``None`` bound is open: ``start`` ``None`` is "from the earliest you
    hold", ``end`` ``None`` is "to the latest you hold". The window is
    half-open, ``[start, end)``.
    """

    version: Literal["v1"] = "v1"
    query_id: str = Field(description="Client-generated correlation id; the results carry it.")
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


class TimeSeriesResult(DataModel):
    """One envelope on the results topic (topic ``time.series.result``).

    Exactly one of three shapes, by ``kind``:

    * ``record`` -- ``model`` names the record's class by topic string and
      ``record`` is its ``model_dump(mode="json")``.
    * ``end`` -- the query is complete; ``count`` is how many records it sent.
    * ``error`` -- the query failed; ``error`` says why. Nothing follows.

    ``timestamp`` is when the service published the envelope, not the record's
    own time -- that is inside ``record``.
    """

    version: Literal["v1"] = "v1"
    timestamp: datetime = Field(description="When the service published this envelope.")
    query_id: str = Field(description="The query this answers; also the Kafka record key.")
    seq: int = Field(ge=0, description="Position within the query, from 0, one per envelope.")
    kind: ResultKind
    model: str | None = Field(default=None, description="'record' only: the record's topic string.")
    record: dict[str, Any] | None = Field(
        default=None, description="'record' only: the record as JSON, validated by the client."
    )
    count: int | None = Field(default=None, ge=0, description="'end' only: records sent.")
    error: str | None = Field(default=None, description="'error' only: what went wrong.")

    @model_validator(mode="after")
    def _shape_matches_kind(self) -> TimeSeriesResult:
        if self.kind == "record" and (self.model is None or self.record is None):
            raise ValueError("a 'record' envelope needs 'model' and 'record'")
        if self.kind == "end" and self.count is None:
            raise ValueError("an 'end' envelope needs 'count'")
        if self.kind == "error" and not self.error:
            raise ValueError("an 'error' envelope needs 'error'")
        return self

    # -- constructors the service side uses --------------------------------------

    @classmethod
    def for_record(
        cls, query_id: str, seq: int, message: DataModel, *, timestamp: datetime
    ) -> TimeSeriesResult:
        return cls(
            timestamp=timestamp,
            query_id=query_id,
            seq=seq,
            kind="record",
            model=type(message).topic,
            record=message.model_dump(mode="json"),
        )

    @classmethod
    def ended(cls, query_id: str, seq: int, count: int, *, timestamp: datetime) -> TimeSeriesResult:
        return cls(timestamp=timestamp, query_id=query_id, seq=seq, kind="end", count=count)

    @classmethod
    def failed(cls, query_id: str, seq: int, error: str, *, timestamp: datetime) -> TimeSeriesResult:
        return cls(timestamp=timestamp, query_id=query_id, seq=seq, kind="error", error=error)
