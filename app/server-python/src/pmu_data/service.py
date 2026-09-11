"""The process's data gateway: one per process, built at startup, shared by all.

STEP3 §8.1: *"Source: the process's DataGateway and its clients. One per
process. Shared, read-only from the modules' point of view."* This module is
that. It is a **service package** in ``server.py``'s sense -- it has no url
surface of its own and is entered through ``SERVICES`` before any app package
handles a request -- exactly as ``pswamp_web`` is for the monitor's registry.

Which clients it runs is configuration (STEP3 §5.5). With nothing set, the dev
laptop and CI get :func:`default_clients`: the committed sample file as the
source and the in-memory bus for results. A deployment names its own::

    PSWAMP_CLIENTS=tsdb,bus
    TSDB_TYPE=my_tso.pswamp_clients:TimescaleClient   TSDB_DSN=...
    BUS_TYPE=in_memory

and the same app packages run unchanged over it.

Why the bus does *not* carry samples here: a live-capable client covers "now",
so if it also declared ``Sample`` the planner would, at the end of the sample
file, hand the streamer's replay over to the bus to tail for samples that never
come -- and the replay would never reach the end of its pass, so it would never
loop. A recording replay must run out; a live feed must not. Which models a
client declares is what decides that, and it is decided here.
"""

from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI
from pswamp.data import (
    BUILTIN_CLIENTS,
    BUS_CAPABILITIES,
    Coverage,
    DataClient,
    DataGateway,
    DataModel,
    InMemoryClient,
    MRIDFilter,
    Report,
    Sample,
    StreamHeader,
    TimeRange,
    build_gateway_from_env,
)
from shared import get_logger

from .sample_file import STREAM_ID, SampleFileClient

__all__ = [
    "MODELS",
    "REGISTRY",
    "STREAM_ID",
    "coverage_of",
    "default_clients",
    "gateway",
    "lifespan",
    "stream_header",
]

# The core's own loggers have no handler by default (see pswamp_web/log.py for
# why that matters); binding one here makes planner/gateway warnings visible.
logger = get_logger("pmu_data")
get_logger("pswamp.data")

#: Every message type a configured client is offered; a client's ``from_env``
#: narrows it to what it serves. ``Report`` stands for every report subclass.
MODELS: list[type[DataModel]] = [StreamHeader, Sample, Report]

#: Built-in short names ``<NAME>_TYPE`` may use, plus this deployment's own.
REGISTRY: dict[str, type[DataClient]] = {**BUILTIN_CLIENTS, "sample_file": SampleFileClient}


def default_clients() -> list[DataClient]:
    """What runs when ``PSWAMP_CLIENTS`` is unset: the sample file and a bus."""
    return [
        SampleFileClient("source"),
        InMemoryClient(
            "bus",
            [Report],
            capabilities=BUS_CAPABILITIES,
            max_records=1000,
            queue_size=256,
        ),
    ]


_gateway: DataGateway | None = None
_headers: dict[str, StreamHeader] = {}


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Build and open the gateway for as long as the process is up."""
    global _gateway
    _gateway = build_gateway_from_env(MODELS, defaults=default_clients, registry=REGISTRY)
    await _gateway.open()
    logger.info("data gateway up: %s", ", ".join(_gateway.clients))
    try:
        yield
    finally:
        await _gateway.close()
        _gateway = None
        _headers.clear()


def gateway() -> DataGateway:
    """The process's gateway. Only valid while the lifespan is running."""
    if _gateway is None:
        raise RuntimeError("the data gateway is not running; is pmu_data in server.SERVICES?")
    return _gateway


async def stream_header(stream_id: str = STREAM_ID) -> StreamHeader:
    """The header of one stream, read once through the gateway and kept."""
    header = _headers.get(stream_id)
    if header is None:
        stream = gateway().consume(StreamHeader, mRID=stream_id)
        async for payload in stream:
            header = payload
            break
        await stream.aclose()
        if header is None:
            raise RuntimeError(f"no client serves a StreamHeader for stream {stream_id!r}")
        _headers[stream_id] = header
    return header


async def coverage_of(model: type[DataModel], mRID: MRIDFilter = None) -> Coverage | None:
    """The union of every client's coverage for ``model`` -- what a replay can span.

    The gateway itself has no such call: it plans one segment at a time, which
    is right for streaming and wrong for a scrub bar. This is the one place a
    page asks "how long is the recording".
    """
    starts, ends, live = [], [], False
    for client in gateway().clients.values():
        if not client.supports(model):
            continue
        coverage = await client.coverage(model, mRID)
        if coverage is None or coverage.range.start is None or coverage.range.end is None:
            continue
        starts.append(coverage.range.start)
        ends.append(coverage.range.end)
        live = live or coverage.live
    if not starts:
        return None
    return Coverage(TimeRange(min(starts), max(ends)), live=live)
