# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""A worker: hosts the modules of the pipeline families it is told to.

``python -m pswamp_core.worker``, from a directory where the families import
(the image's ``src/``), with the same transport variable the server reads::

    PSWAMP_TRANSPORT=kafka:pswamp_core.transport.kafka:KafkaTransport
    KAFKA_BOOTSTRAP_SERVERS=kafka:9092
    PSWAMP_WORKER_FAMILIES=pmu_test_streamer.family:FAMILY,islanding_stream.family:FAMILY

One process may host every family, or a family may get a worker of its own (a
heavy algorithm, with its own CPU limit) by naming only it. A worker is
configured like the server in one more way: a module that reads the gateway
gets one built from the same ``*_DATA_CLIENTS`` variables, so those go to the
worker too.

Exits 2, saying why, when the transport is the in-memory one (there is
nothing to reach the server over; the server hosts the modules itself then) or
no families are named. Stops cleanly on SIGINT/SIGTERM.
"""

from __future__ import annotations

import asyncio
import importlib
import os
import signal
import sys

from .host import hosts_for, serve_hosts
from .log import get_logger
from .pipeline import PipelineFamily
from .transport import TRANSPORT_VARIABLE, transport_from_env

__all__ = ["FAMILIES_VARIABLE", "load_families", "main"]

logger = get_logger("pswamp_core.worker")

#: Comma-separated ``module.path:NAME`` references to ``PipelineFamily`` objects.
FAMILIES_VARIABLE = "PSWAMP_WORKER_FAMILIES"


def load_families(spec: str) -> list[PipelineFamily]:
    """The families ``spec`` names, imported.

    Raises:
        ValueError: A malformed reference, or one that is not a ``PipelineFamily``.
    """
    families = []
    for reference in (part.strip() for part in spec.split(",")):
        if not reference:
            continue
        module_path, _, name = reference.partition(":")
        if not module_path or not name:
            raise ValueError(f"{FAMILIES_VARIABLE}: {reference!r} is not module.path:NAME")
        family = getattr(importlib.import_module(module_path), name, None)
        if not isinstance(family, PipelineFamily):
            raise ValueError(f"{FAMILIES_VARIABLE}: {reference} is not a PipelineFamily")
        families.append(family)
    return families


def main() -> int:
    """Host the named families until SIGINT/SIGTERM; the process exit code."""
    transport = transport_from_env()
    if transport.in_process:
        print(f"{TRANSPORT_VARIABLE} is unset: there is no broker to reach the server over.", file=sys.stderr)
        print("Without one the server hosts the modules itself. For a worker, set it, e.g.", file=sys.stderr)
        print(
            f"  {TRANSPORT_VARIABLE}=kafka:pswamp_core.transport.kafka:KafkaTransport  (plus KAFKA_BOOTSTRAP_SERVERS)",
            file=sys.stderr,
        )
        return 2
    families = load_families(os.environ.get(FAMILIES_VARIABLE, ""))
    if not families:
        print(f"{FAMILIES_VARIABLE} names no pipeline family to host, e.g.", file=sys.stderr)
        print(f"  {FAMILIES_VARIABLE}=pmu_test_streamer.family:FAMILY", file=sys.stderr)
        return 2

    async def run() -> None:
        hosts = [host for family in families for host in hosts_for(family, transport)]
        task = asyncio.create_task(serve_hosts(hosts))
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, task.cancel)
        logger.info("worker hosting %s", ", ".join(f"{h.app}/{h.name}" for h in hosts))
        try:
            # Until a signal's cancel has stopped the hosts. Not a
            # suppress(CancelledError) around `await task`, which would also
            # swallow a cancellation of run() itself; a crash still raises.
            await asyncio.wait([task])
            if not task.cancelled():
                task.result()
        finally:
            await transport.close()
        logger.info("worker stopped")

    asyncio.run(run())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
