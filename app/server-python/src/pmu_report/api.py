"""The PMU report api: request/response inside commands-up, state-down.

STEP3 §8.4 in its smallest form. A report is asked for with a POST and answered
on the socket -- there is no second message direction and the POST's reply
carries no data, only a :class:`~pswamp.data.JobAck` with the ``request_id``
the report will wear when it arrives. Between the two, the job is an ordinary
module run: query a bounded range through the gateway, analyse it off the
loop, ``produce`` the :class:`~.model.VoltageReport`.

What ties the report back to the browser that asked is worth spelling out,
because it is the one place this package touches the bus at all:

- The POST remembers ``request_id -> client_id`` and returns.
- The job ``produce``s its report through the gateway; the bus client fans it
  out to whoever is consuming ``VoltageReport``.
- This package's lifespan task is such a consumer. It looks up who asked, files
  the report under that client, and wakes that client's open sockets.

So the page never sees a raw sample (the rig document's K3 rule), the module
never knows a browser exists, and a report produced by *anything* on the bus --
another process, one day -- would reach the page by the same path.

Everything here runs on the one event loop; the analysis itself does not.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections import deque
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Literal

from fastapi import APIRouter, FastAPI, WebSocket
from pmu_data import STREAM_ID, coverage_of, gateway, stream_header
from pswamp.data import JobAck, Sample, new_job_id, run_batch_job, utcnow
from pydantic import BaseModel, Field
from shared import (
    ClientId,
    SessionRegistry,
    get_logger,
    offer,
    read_client_id,
    serve_updates,
)

from .model import VoltageReport, mean_voltage

logger = get_logger("pmu-report")
router = APIRouter()

#: Reports kept per client. A report is small; ten is a page, not a store.
MAX_REPORTS = 10


# --- authoritative in-memory state ------------------------------------------


class JobError(BaseModel):
    request_id: str
    message: str


@dataclass
class ClientReports:
    """What one client has asked for and what came back, newest first."""

    reports: deque[VoltageReport] = field(default_factory=lambda: deque(maxlen=MAX_REPORTS))
    pending: list[str] = field(default_factory=list)
    errors: list[JobError] = field(default_factory=list)


stores: dict[str, ClientReports] = {}

#: Which client asked for which report, by request id, until it arrives.
owners: dict[str, str] = {}

#: Running jobs, held so the event loop does not garbage-collect them.
jobs: set[asyncio.Task] = set()

#: Each client's open sockets, as the queue that wakes their push task.
sessions: SessionRegistry[asyncio.Queue] = SessionRegistry()


def get_store(client_id: str) -> ClientReports:
    store = stores.get(client_id)
    if store is None:
        store = stores[client_id] = ClientReports()
    return store


def nudge(client_id: str) -> None:
    """Wake every open view this client has; each rebuilds from the store."""
    for queue in sessions.of(client_id):
        offer(queue)


# --- the wire ---------------------------------------------------------------


class PmuReportState(BaseModel):
    """Everything the report page shows: results, what is still running, failures."""

    type: Literal["state"] = "state"
    reports: list[VoltageReport] = Field(description="Newest first, at most MAX_REPORTS.")
    pending: list[str] = Field(description="Request ids of jobs still running.")
    errors: list[JobError] = Field(description="Jobs that failed, newest last.")


def state_message(store: ClientReports) -> PmuReportState:
    return PmuReportState(reports=list(store.reports), pending=list(store.pending), errors=list(store.errors))


# --- the job ----------------------------------------------------------------


class RunMeanVoltage(BaseModel):
    """Which part of the source to report over. Both bounds unset = all of it."""

    start_s: float | None = Field(default=None, ge=0, description="Seconds into the source, inclusive.")
    end_s: float | None = Field(default=None, ge=0, description="Seconds into the source, exclusive.")
    request_id: str | None = Field(
        default=None,
        max_length=64,
        description="Caller's correlation id; the report carries it back. Defaults to the job id.",
    )


async def run_job(client_id: str, request_id: str, body: RunMeanVoltage) -> None:
    """One job: resolve the range, run the batch, and let the bus carry the answer."""
    try:
        header = await stream_header(STREAM_ID)
        coverage = await coverage_of(Sample, STREAM_ID)
        if coverage is None or coverage.range.start is None or coverage.range.end is None:
            raise RuntimeError(f"no client serves Sample for stream {STREAM_ID!r}")
        origin = coverage.range.start
        start = origin if body.start_s is None else origin + timedelta(seconds=body.start_s)
        end = coverage.range.end if body.end_s is None else origin + timedelta(seconds=body.end_s)
        await run_batch_job(
            gateway(),
            Sample,
            lambda samples: mean_voltage(header, samples),
            start=start,
            end=min(end, coverage.range.end),
            mRID=STREAM_ID,
            request_id=request_id,
        )
    except Exception as exc:
        logger.exception("client %s: job %s failed", client_id, request_id)
        owners.pop(request_id, None)
        store = get_store(client_id)
        if request_id in store.pending:
            store.pending.remove(request_id)
        store.errors.append(JobError(request_id=request_id, message=f"{type(exc).__name__}: {exc}"))
        nudge(client_id)


@router.post("/mean-voltage/run", operation_id="pmu_report_run_mean_voltage")
async def run_mean_voltage(client_id: ClientId, body: RunMeanVoltage) -> JobAck:
    """Start a mean-voltage report over a range of the source. The result arrives on the socket."""
    job_id = new_job_id()
    request_id = body.request_id or job_id
    store = get_store(client_id)
    store.pending.append(request_id)
    owners[request_id] = client_id

    task = asyncio.create_task(run_job(client_id, request_id, body))
    jobs.add(task)
    task.add_done_callback(jobs.discard)

    logger.info(
        "client %s: mean-voltage job %s (request %s) over [%s, %s)",
        client_id, job_id, request_id, body.start_s, body.end_s,
    )
    nudge(client_id)
    return JobAck(applied="mean-voltage", job_id=job_id, request_id=request_id)


# --- reports arrive on the bus ----------------------------------------------


async def collect_reports() -> None:
    """Consume every VoltageReport the bus carries and file it under whoever asked."""
    while True:
        try:
            stream = gateway().consume(VoltageReport, start=utcnow())
            async for report in stream:
                client_id = owners.pop(report.request_id or "", None)
                if client_id is None:
                    logger.info("report %s arrived for no client of this process", report.request_id)
                    continue
                store = get_store(client_id)
                if report.request_id in store.pending:
                    store.pending.remove(report.request_id)
                store.reports.appendleft(report)
                logger.info("client %s: report %s ready (%d samples)", client_id, report.request_id, report.n_samples)
                nudge(client_id)
            # The stream ended: no client serves reports live. Log and retry
            # after a pause rather than die silently.
            logger.warning("no live source of VoltageReport on the gateway; retrying")
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("report collector failed; retrying")
        await asyncio.sleep(2.0)


@contextlib.asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(collect_reports())
    try:
        yield
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


# --- websocket endpoint (downstream only) -----------------------------------


@router.websocket("/ws")
async def ws_endpoint(ws: WebSocket) -> None:
    client_id = read_client_id(ws)
    if client_id is None:
        await ws.close(code=1008)
        return

    await ws.accept()
    queue: asyncio.Queue = asyncio.Queue(maxsize=8)
    with sessions.registered(client_id, queue):
        logger.info("client %s: connected", client_id)
        await serve_updates(ws, queue, lambda _event: state_message(get_store(client_id)))
    logger.info("client %s: disconnected", client_id)
