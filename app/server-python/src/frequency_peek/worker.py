# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""The frequency module as its own process, connected over topics.

A plain asyncio process from the same image, with no port: a bus of its own,
the ``FrequencyModule`` on it, and a ``TopicBridge`` that tails the frames the
web server produces and produces the results the module publishes::

    broker ── pmu.frame ──▶ TopicBridge ──▶ bus ──▶ FrequencyModule ──▶ bus ──▶ TopicBridge ── frequency.result ──▶ broker

The module needs the stream's header before its first frame. The web side
primes ``pmu.header`` on the broker (re-stamped, and again every half minute);
this process reads the newest one with a bounded read, serves it to the
module's own ``setup`` from an in-memory client, and keeps listening for a
later one on the bus. So the module's code, its ``setup`` and its ``run`` are
exactly what they are in-process.

Run from the server's ``src/`` directory, as the image does::

    FREQUENCY_PEEK_BUS_CLIENTS=bus:pswamp_core.datagateway.clients.kafka:KafkaClient \\
    BUS_BOOTSTRAP_SERVERS=redpanda:9092 python -m frequency_peek.worker

Liveness is a heartbeat file (``FREQUENCY_PEEK_WORKER_HEARTBEAT``, default
``/tmp/frequency-worker-heartbeat``) touched every few seconds while the loop
runs; the probes check its age. Losing the broker is logged and reconnected by
the bridge, not something a restart would help, so it does not fail the probe.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import signal
import sys
import time
from pathlib import Path

from pswamp_core.bridge import TopicBridge, newest
from pswamp_core.bus import InProcessBus
from pswamp_core.datagateway import DataGateway, MissingSettingError, gateway_from_env
from pswamp_core.datagateway.clients import InMemoryClient
from pswamp_core.log import get_logger
from pswamp_core.messages import PmuFrame, PmuHeader

from .api import BUS_CLIENTS_VARIABLE
from .frequency_module import FrequencyModule, FrequencyResult

logger = get_logger("frequency_peek.worker")

HEARTBEAT_VARIABLE = "FREQUENCY_PEEK_WORKER_HEARTBEAT"
DEFAULT_HEARTBEAT = Path("/tmp/frequency-worker-heartbeat")
HEARTBEAT_SECONDS = 5.0
HEADER_POLL_SECONDS = 2.0


async def wait_for_header(bus_gateway: DataGateway, poll: float) -> PmuHeader:
    """The newest primed header on the broker, polling until one is there."""
    waiting = False
    while True:
        try:
            header = await newest(bus_gateway, PmuHeader)
        except Exception as exc:
            header = None
            if not waiting:
                logger.warning("cannot read %s yet: %s", PmuHeader.topic, exc)
        if isinstance(header, PmuHeader):
            logger.info("header %s: %d stations at %g fps", header.header_id, len(header.stations), header.data_rate)
            return header
        if not waiting:
            logger.info("waiting for a %s on the broker (is the web server's pipeline running?)", PmuHeader.topic)
            waiting = True
        await asyncio.sleep(poll)


async def _heartbeat(path: Path | None) -> None:
    if path is None:
        return
    while True:
        with contextlib.suppress(OSError):
            path.touch()
        await asyncio.sleep(HEARTBEAT_SECONDS)


async def serve(
    bus_gateway: DataGateway,
    *,
    header_poll: float = HEADER_POLL_SECONDS,
    heartbeat: Path | None = None,
) -> None:
    """Run one ``FrequencyModule`` over the broker until cancelled."""
    module = FrequencyModule()
    bus = InProcessBus()
    bus.bind(asyncio.get_running_loop())
    bridge = TopicBridge(
        bus_gateway,
        outbound=[FrequencyResult],
        inbound=[PmuFrame, PmuHeader],
        name="frequency@worker",
    )
    # The loop is alive from here on, header or no header: a worker waiting for
    # a web server that has not opened the page yet is healthy, not stuck.
    tasks: list[asyncio.Task] = [asyncio.create_task(_heartbeat(heartbeat), name="frequency.heartbeat")]
    try:
        # The bridge has nothing to prime here; its setup opens the broker gateway.
        await bridge.setup(DataGateway([InMemoryClient("setup", [PmuHeader])]), bus)
        header = await wait_for_header(bus_gateway, header_poll)
        await module.setup(DataGateway([InMemoryClient("setup", [PmuHeader], records=[header])]), bus)
        # A re-primed or changed header arrives on the bus; the module re-reads it.
        bus.add_listener(PmuHeader, module.use_header)
        tasks += [
            asyncio.create_task(module.run(bus), name="frequency.run"),
            asyncio.create_task(bridge.run(bus), name="frequency.bridge"),
        ]
        logger.info("serving %s over %s", module.name, list(bus_gateway.clients))
        await asyncio.gather(*tasks)
    finally:
        for task in tasks:
            task.cancel()
        for task in tasks:
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task
        bus.bind(None)


def main() -> None:
    try:
        bus_gateway = gateway_from_env(None, variable=BUS_CLIENTS_VARIABLE)
    except MissingSettingError as exc:
        print(f"frequency worker: {exc}", file=sys.stderr)
        print(f"set {BUS_CLIENTS_VARIABLE} to the broker's provider spec, e.g.", file=sys.stderr)
        print("  bus:pswamp_core.datagateway.clients.kafka:KafkaClient  (plus BUS_BOOTSTRAP_SERVERS)", file=sys.stderr)
        sys.exit(2)
    heartbeat = Path(os.environ.get(HEARTBEAT_VARIABLE) or DEFAULT_HEARTBEAT)

    async def run() -> None:
        task = asyncio.create_task(serve(bus_gateway, heartbeat=heartbeat))
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, task.cancel)
        with contextlib.suppress(asyncio.CancelledError):
            await task
        logger.info("stopped")

    started = time.monotonic()
    asyncio.run(run())
    logger.info("ran for %.0f s", time.monotonic() - started)


if __name__ == "__main__":
    main()
