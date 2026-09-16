# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""A module in another process: ``RemoteModule`` on the pipeline's side,
``ModuleHost`` on the worker's.

A module is the analysis and two class attributes -- what it consumes, what it
publishes -- connected to its pipeline by a bus (:mod:`pswamp_core.modules`).
To run it as its own service, nothing in the module changes; two things stand
on either side of the process boundary and a :class:`~pswamp_core.transport.Transport`
carries the topics between them::

    the pipeline's process                                      the worker process
    bus ─▶ RemoteModule(M, key=k) ─publish(input, key=k)─▶ topic ─▶ ModuleHost(M): one M + bus per key
    bus ◀─ RemoteModule           ◀─subscribe(output, key=k)─ topic ◀─ ... M publishes its result

**``RemoteModule`` takes the module's slot.** It is a ``Module`` with the same
``name``, ``input_model`` and ``output_model`` as the class it stands in for, so
the pipeline's module list reads ``[RemoteModule(FrameStatsModule, transport,
key)]`` where it read ``[FrameStatsModule()]``, and the socket endpoint that
subscribes to the result class notices nothing. Its ``setup`` publishes what the
module's own ``setup`` would have read from the gateway (``setup_models``),
retained under the pipeline key, so the worker can hand the real ``setup`` the
same records; its ``run`` drains the input off the local bus onto the topic and
puts results from the topic back on the bus.

**``ModuleHost`` runs one module instance per key it sees.** It subscribes to
the module's input topic across every key and to each ``setup_models`` topic
(retained, so a worker that starts late still gets the newest header per key).
The first *input* for a key builds that key's bus, module and forwarder (a
retained setup record alone is remembered, not acted on: the header topic
replays every key ever seen); a key that goes quiet for ``idle_seconds`` is
evicted and rebuilt on its next input. A pipeline keyed per client therefore costs one module instance per
client on the worker, exactly as it does in-process; a pipeline keyed per
stream costs one.

Setup records that arrive after ``setup`` ran -- a header a late worker reads
after its first frame, or a changed layout -- are published on the key's local
bus, so a module that also listens for them there re-primes itself; that is
the one thing a module does to be host-independent, and ``FrameStatsModule``
in the streamer is the example.

``main`` is the body of a worker entrypoint: ``python -m <app>.worker`` reads
the same transport variable the server reads, serves until SIGINT/SIGTERM, and
exits 2 with a usage message when the variable is unset.
"""

from __future__ import annotations

import asyncio
import contextlib
import signal
import sys
import time
from collections import deque
from collections.abc import Callable
from typing import TYPE_CHECKING, ClassVar

from .bus import InProcessBus, Overflow
from .datagateway import DataGateway
from .datagateway.clients import InMemoryClient
from .log import get_logger
from .messages import DataModel, ResultEnvelope
from .modules import Module
from .transport import Transport, transport_from_env

if TYPE_CHECKING:
    from .bus import Bus

__all__ = ["ModuleHost", "RemoteModule", "main"]

logger = get_logger("pswamp_core.remote")

#: Log the first few publish failures, then one in every this-many.
_LOG_FIRST = 3
_LOG_EVERY = 50

DEFAULT_IDLE_SECONDS = 300.0


class RemoteModule(Module):
    """The module's stand-in in a pipeline whose module runs elsewhere.

    Args:
        module_cls: The module class this stands in for; its ``name``,
            ``input_model``, ``output_model`` and ``setup_models`` are read.
        transport: The process's shared transport; opened here (idempotently).
        key: The pipeline key every record is published and filtered under.
        overflow, maxsize: The outbox's policy, as for any module.
    """

    input_model: ClassVar[type[DataModel]] = DataModel  # replaced per instance below
    output_model: ClassVar[type[ResultEnvelope]] = ResultEnvelope

    def __init__(
        self,
        module_cls: type[Module],
        transport: Transport,
        key: str,
        *,
        overflow: Overflow = Overflow.DROP_OLDEST,
        maxsize: int = 64,
    ) -> None:
        self.name = module_cls.name
        self.input_model = module_cls.input_model  # type: ignore[misc]
        self.output_model = module_cls.output_model  # type: ignore[misc]
        self.setup_models = module_cls.setup_models  # type: ignore[misc]
        self.overflow = overflow  # type: ignore[misc]
        self.maxsize = maxsize  # type: ignore[misc]
        super().__init__()
        self.module_cls = module_cls
        self.transport = transport
        self.key = key
        self.published = 0
        self.dropped = 0
        self.received = 0
        self.parameters = {
            "remote": True,
            "key": key,
            "input": self.input_model.topic,
            "output": self.output_model.topic,
        }

    async def setup(self, gateway: DataGateway, bus: Bus) -> None:
        """Open the transport and send ahead what the module's ``setup`` reads."""
        await self.transport.open()
        for cls in self.setup_models:
            async for record in gateway.consume(cls):
                await self.transport.publish(record, self.key, retained=True)
        logger.info(
            "%s@%s: input %s and %s out, %s back, over %s",
            self.name, self.key, self.input_model.topic,
            [cls.topic for cls in self.setup_models], self.output_model.topic, self.transport.name,
        )

    async def process(self, message: DataModel) -> None:
        raise NotImplementedError("a RemoteModule forwards; the module processes elsewhere")

    async def run(self, bus: Bus) -> None:
        tasks = [
            asyncio.create_task(self._outbox(bus), name=f"{self.name}@{self.key}.out"),
            asyncio.create_task(self._inbox(bus), name=f"{self.name}@{self.key}.in"),
        ]
        try:
            await asyncio.gather(*tasks)
        finally:
            for task in tasks:
                task.cancel()
            for task in tasks:
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await task

    async def _outbox(self, bus: Bus) -> None:
        with bus.subscribe(self.input_model, overflow=self.overflow, maxsize=self.maxsize) as inputs:
            async for message in inputs:
                try:
                    await self.transport.publish(message, self.key)
                except Exception as exc:
                    self.dropped += 1
                    if self.dropped <= _LOG_FIRST or self.dropped % _LOG_EVERY == 0:
                        logger.warning(
                            "%s@%s: could not publish %s (%d dropped so far): %s",
                            self.name, self.key, type(message).__name__, self.dropped, exc,
                        )
                    continue
                self.published += 1

    async def _inbox(self, bus: Bus) -> None:
        with self.transport.subscribe(self.output_model, self.key) as results:
            async for _key, result in results:
                self.received += 1
                self.last_result = result  # type: ignore[assignment]
                bus.publish(result)


class _Slot:
    """One key's bus, module and tasks on the host."""

    def __init__(self, key: str) -> None:
        self.key = key
        self.bus = InProcessBus()
        self.module: Module | None = None
        self.context: dict[type[DataModel], DataModel] = {}
        self.tasks: list[asyncio.Task] = []
        self.seen = time.monotonic()
        self.started = False
        # Input that arrives while the module is still in setup: held, bounded,
        # and flushed once it runs -- the module's own overflow policy, applied
        # before it has a subscription to apply it.
        self.pending: deque[DataModel] = deque(maxlen=64)

    def publish(self, message: DataModel) -> None:
        if self.started:
            self.bus.publish(message)
        else:
            self.pending.append(message)


class ModuleHost:
    """Runs one module instance per pipeline key, over a transport.

    Args:
        module_factory: Builds a fresh module instance; usually the class.
        transport: The worker's transport; opened by ``serve``.
        idle_seconds: How long a key may go without input before its module is
            evicted (and rebuilt on the next message).
    """

    def __init__(
        self,
        module_factory: Callable[[], Module],
        transport: Transport,
        *,
        idle_seconds: float = DEFAULT_IDLE_SECONDS,
    ) -> None:
        self._factory = module_factory
        self.transport = transport
        self.idle_seconds = idle_seconds
        template = module_factory()
        self.name = template.name
        self.input_model = template.input_model
        self.output_model = template.output_model
        self.setup_models = tuple(template.setup_models)
        self._slots: dict[str, _Slot] = {}
        # Setup records per key, kept whether or not that key has a module: a
        # retained topic replays every key ever seen, and a header alone is no
        # reason to build a module -- the first input is.
        self._contexts: dict[str, dict[type[DataModel], DataModel]] = {}
        self.forwarded = 0

    def keys(self) -> list[str]:
        """The keys with a live module instance."""
        return list(self._slots)

    async def serve(self) -> None:
        """Consume the topics and run modules until cancelled."""
        await self.transport.open()
        tasks = [asyncio.create_task(self._inputs(), name=f"{self.name}.host.inputs")]
        for cls in self.setup_models:
            tasks.append(asyncio.create_task(self._context(cls), name=f"{self.name}.host.{cls.topic}"))
        tasks.append(asyncio.create_task(self._sweep(), name=f"{self.name}.host.sweep"))
        logger.info(
            "hosting %s: %s in, %s out, setup from %s, over %s",
            self.name, self.input_model.topic, self.output_model.topic,
            [cls.topic for cls in self.setup_models], self.transport.name,
        )
        try:
            await asyncio.gather(*tasks)
        finally:
            for task in tasks:
                task.cancel()
            for task in tasks:
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await task
            for key in list(self._slots):
                await self._evict(key, "shutdown")

    # --- the topics ----------------------------------------------------------------

    async def _inputs(self) -> None:
        with self.transport.subscribe(self.input_model) as inputs:
            async for key, message in inputs:
                slot = self._slot(key)
                slot.seen = time.monotonic()
                slot.publish(message)

    async def _context(self, cls: type[DataModel]) -> None:
        with self.transport.subscribe(cls, retained=True) as records:
            async for key, record in records:
                self._contexts.setdefault(key, {})[type(record)] = record
                slot = self._slots.get(key)
                if slot is not None:
                    slot.context[type(record)] = record
                    if slot.started:
                        slot.bus.publish(record)

    def _slot(self, key: str) -> _Slot:
        """This key's module, built on its first input."""
        slot = self._slots.get(key)
        if slot is None:
            slot = _Slot(key)
            slot.context.update(self._contexts.get(key, {}))
            self._slots[key] = slot
            slot.tasks.append(asyncio.create_task(self._start(slot), name=f"{self.name}@{key}.start"))
        return slot

    async def _start(self, slot: _Slot) -> None:
        module = self._factory()
        # What the module's setup reads: the records carried across so far.
        client = InMemoryClient(
            "context", self.setup_models or [DataModel], records=list(slot.context.values())
        )
        slot.bus.bind(asyncio.get_running_loop())
        await module.setup(DataGateway([client]), slot.bus)
        slot.module = module
        slot.tasks += [
            asyncio.create_task(module.run(slot.bus), name=f"{self.name}@{slot.key}.run"),
            asyncio.create_task(self._forward(slot), name=f"{self.name}@{slot.key}.forward"),
        ]
        # One turn of the loop, so the run and forward tasks hold their
        # subscriptions before anything is published to them.
        await asyncio.sleep(0)
        slot.started = True
        # Anything that arrived while setup ran goes on the bus: the setup
        # records first, so a module that listens for them applies the newest,
        # then the input that was waiting.
        for record in slot.context.values():
            slot.bus.publish(record)
        while slot.pending:
            slot.bus.publish(slot.pending.popleft())
        logger.info("%s: module started for key %s (%d live)", self.name, slot.key, len(self._slots))

    async def _forward(self, slot: _Slot) -> None:
        with slot.bus.subscribe(self.output_model, overflow=Overflow.DROP_OLDEST, maxsize=64) as results:
            async for result in results:
                try:
                    await self.transport.publish(result, slot.key)
                except Exception as exc:
                    logger.warning("%s@%s: could not publish %s: %s", self.name, slot.key, type(result).__name__, exc)
                    continue
                self.forwarded += 1

    async def _sweep(self) -> None:
        interval = max(0.05, min(self.idle_seconds / 2, 5.0))
        while True:
            await asyncio.sleep(interval)
            cutoff = time.monotonic() - self.idle_seconds
            for key in [k for k, s in self._slots.items() if s.seen < cutoff]:
                await self._evict(key, "idle")

    async def _evict(self, key: str, reason: str) -> None:
        slot = self._slots.pop(key, None)
        if slot is None:
            return
        for task in slot.tasks:
            task.cancel()
        for task in slot.tasks:
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task
        slot.bus.bind(None)
        logger.info("%s: module evicted for key %s (%s), %d live", self.name, key, reason, len(self._slots))


def main(
    module_factory: Callable[[], Module],
    variable: str,
    *,
    idle_seconds: float = DEFAULT_IDLE_SECONDS,
    argv: list[str] | None = None,
) -> int:
    """Run a module host from the environment until SIGINT/SIGTERM.

    Returns the exit code: 0 after a clean stop, 2 when ``variable`` names no
    transport. A worker entrypoint is ``raise SystemExit(main(MyModule, VAR))``.
    """
    transport = transport_from_env(variable)
    if transport is None:
        print(f"{variable} is unset: nothing to connect the module to.", file=sys.stderr)
        print("Set it to the transport's spec, the same value the server reads, e.g.", file=sys.stderr)
        print(
            f"  {variable}=kafka:pswamp_core.transport.kafka:KafkaTransport  (plus KAFKA_BOOTSTRAP_SERVERS)",
            file=sys.stderr,
        )
        return 2

    async def run() -> None:
        host = ModuleHost(module_factory, transport, idle_seconds=idle_seconds)
        task = asyncio.create_task(host.serve())
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, task.cancel)
        try:
            with contextlib.suppress(asyncio.CancelledError):
                await task
        finally:
            await transport.close()
        logger.info("%s: stopped", host.name)

    asyncio.run(run())
    return 0
