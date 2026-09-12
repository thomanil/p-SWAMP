# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""Batch jobs: a bounded query, an analysis, a published report.

STEP3 §8.4, the request/response slot inside a commands-up, state-down contract.
A job is not a new message direction: the command's reply is a
:class:`~..models.commands.JobAck`, and the answer is a
:class:`~..models.results.Report` *produced* through the gateway like any other
module output — so it reaches the bus, an archive, a broker, and whichever page
is consuming that report type, carrying the ``request_id`` that ties it back.

Two rules a batch job keeps:

- **The range is always bounded.** An open end is resolved to *now* before the
  query, so a job over a live-capable client cannot tail forever.
- **Analysis runs off the loop.** The query is async; the analysis is a plain
  blocking function, run on a worker thread, because that is the execution
  model every p-SWAMP module has (STEP3 principle 5).
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import Callable, Sequence
from datetime import datetime
from typing import TYPE_CHECKING, TypeVar

from ..time import utcnow

if TYPE_CHECKING:
    from ..models.base import DataModel
    from ..models.results import Report
    from .client import MRIDFilter
    from .gateway import DataGateway

__all__ = ["new_job_id", "run_batch_job"]

R = TypeVar("R", bound="Report")


def new_job_id() -> str:
    """A short, unique job id — fine for logs and correlation, not a secret."""
    return uuid.uuid4().hex[:12]


async def run_batch_job(
    gateway: DataGateway,
    model: type[DataModel],
    analyse: Callable[[Sequence[DataModel]], R],
    *,
    start: datetime | None = None,
    end: datetime | None = None,
    mRID: MRIDFilter = None,
    request_id: str | None = None,
) -> R:
    """Query ``[start, end)`` of ``model``, analyse it, produce and return the report.

    ``analyse`` receives every payload in the range, in order, and returns the
    report. The runner fills in ``request_id`` and, when the analysis left them
    unset, the range bounds actually covered — so a report always says what it
    is over.
    """
    if end is None:
        end = utcnow()

    stream = gateway.consume(model, start, end, mRID)
    payloads = [payload async for payload in stream]

    report = await asyncio.to_thread(analyse, payloads)

    report.request_id = request_id
    if report.range_start is None and payloads:
        report.range_start = payloads[0].timestamp
    if report.range_end is None and payloads:
        report.range_end = payloads[-1].timestamp

    await gateway.produce(report)
    return report
