# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""A module in another process: ``RemoteModule`` on the pipeline's side,
``ModuleHost`` on the worker's.

Nothing in the module changes. Two stand-ins sit either side of the process
boundary, and a :class:`~pswamp_core.transport.Transport` carries the topics
between them::

    the pipeline's process                                   the worker process
    bus ─▶ RemoteModule(M, key=k) ─publish(input, k)─▶ topic ─▶ ModuleHost(M): one M + bus per key
    bus ◀─ RemoteModule           ◀─subscribe(output, k)─ topic ◀─ ... M publishes its result

**``RemoteModule`` takes the module's slot** in the pipeline's module list. It
has the module's ``name``, ``input_model`` and ``output_model``, so whoever
subscribes to the result class notices nothing. Its ``run`` drains the input
off the local bus onto the topic and puts results from the topic back on the
bus.

**``ModuleHost`` runs one module instance per key it sees.** The first input
for a key builds that key's bus and module; a key idle for ``idle_seconds`` is
evicted and rebuilt on its next input. A per-client pipeline therefore costs
one module instance per client in the worker, as it does in-process.

**Errors cross back too.** What the hosted module publishes as an
``ErrorEvent`` goes back under the key, and the ``RemoteModule`` for that key
puts it on its pipeline's bus.

**So do commands.** For a module that declares ``commands``, the
``RemoteModule`` is the pipeline's receiver: its ``handle`` publishes each
command on its class's topic under the key, the host puts it on that key's
bus, and the hosted module's own inbox checks and applies it. The stand-in's
``validate`` accepts everything, since the module's state is in the worker; a
refusal made there comes back as an ``ErrorEvent`` carrying the command's
``request_id``. Declared command classes must be concrete: a topic carries
one class, so a base class's topic would hear none of its subclasses.

``main`` is the body of a worker entrypoint: it reads the same transport
variable the server reads, serves until SIGINT/SIGTERM, and exits 2 with a
usage message when the variable is unset.
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
from .log import get_logger
from .messages import Command, DataModel, ErrorEvent, ResultEnvelope
from .modules import Module
from .transport import Transport, transport_from_env

if TYPE_CHECKING:
    from .bus import Bus
    from .command_routing import CommandInbox

__all__ = ["ModuleHost", "RemoteModule", "main"]

logger = get_logger("pswamp_core.remote")

#: Log the first few publish failures, then one in every this-many.
_LOG_FIRST = 3
_LOG_EVERY = 50

DEFAULT_IDLE_SECONDS = 300.0


def _check_hostable(module_cls: type[Module]) -> None:
    """Refuse, with the reason, a module whose commands cannot cross a topic."""
    for command in module_cls.commands:
        if command.__subclasses__():
            raise ValueError(
                f"{module_cls.__name__} declares {command.__name__}, which has subclasses; "
                "list the concrete command classes, since a topic carries one class"
            )


class RemoteModule(Module):
    """The module's stand-in in a pipeline whose module runs elsewhere.

    Args:
        module_cls: The module class this stands in for; its ``name``,
            ``input_model``, ``output_model`` and ``commands`` are read.
        transport: The process's shared transport; opened here (idempotently).
        key: The pipeline key every record is published and filtered under.
        overflow, maxsize: The outbox's policy, as for any module.
    """

    input_model: ClassVar[type[DataModel] | None] = DataModel  # replaced per instance below
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
        _check_hostable(module_cls)
        self.name = module_cls.name
        self.input_model = module_cls.input_model  # type: ignore[misc]
        self.output_model = module_cls.output_model  # type: ignore[misc]
        self.commands = module_cls.commands  # type: ignore[misc]
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
            "input": self.input_model.topic if self.input_model else None,
            "output": self.output_model.topic,
            "commands": [command.topic for command in self.commands],
        }

    async def process(self, message: DataModel) -> None:
        raise NotImplementedError("a RemoteModule forwards; the module processes elsewhere")

    def validate(self, command: Command) -> None:
        """Accept: the module's state is in the worker, which checks it there and
        reports a refusal as an ``ErrorEvent`` carrying the ``request_id``."""
        return

    async def handle(self, command: Command) -> None:
        """Send the command to the worker; its answer returns as a result."""
        await self.transport.open()
        await self.transport.publish(command, self.key)

    async def run(self, bus: Bus) -> None:
        await self.transport.open()
        logger.info(
            "%s@%s: %s out, %s back, commands %s, over %s",
            self.name, self.key, self.input_model.topic if self.input_model else "nothing",
            self.output_model.topic, [c.topic for c in self.commands] or "none", self.transport.name,
        )
        tasks = [
            asyncio.create_task(self._outbox(bus), name=f"{self.name}@{self.key}.out"),
            asyncio.create_task(self._inbox(bus), name=f"{self.name}@{self.key}.in"),
            asyncio.create_task(self._errors(bus), name=f"{self.name}@{self.key}.errors"),
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
        if self.input_model is None:
            return
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

    async def _errors(self, bus: Bus) -> None:
        """The hosted module's ``ErrorEvent``s for this key, onto the pipeline's bus.

        Filtered on ``source``: the error topic is shared by every hosted
        module, and a pipeline with two remote modules must not hear each
        other's errors.
        """
        with self.transport.subscribe(ErrorEvent, self.key) as errors:
            async for _key, error in errors:
                if error.source == self.name:
                    bus.publish(error)


class _Slot:
    """One key's bus, module and tasks on the host."""

    def __init__(self, key: str) -> None:
        self.key = key
        self.bus = InProcessBus()
        self.module: Module | None = None
        self.inbox: CommandInbox | None = None
        self.tasks: list[asyncio.Task] = []
        self.seen = time.monotonic()
        self.started = False
        # Input that arrives before the module holds its subscription: held,
        # bounded, and flushed once it runs.
        self.pending: deque[DataModel] = deque(maxlen=64)
        # Commands, likewise -- but never dropped.
        self.pending_commands: deque[Command] = deque()

    def publish(self, message: DataModel) -> None:
        if self.started:
            self.bus.publish(message)
        elif isinstance(message, Command):
            self.pending_commands.append(message)
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
        if isinstance(module_factory, type):
            _check_hostable(module_factory)
        self._factory = module_factory
        self.transport = transport
        self.idle_seconds = idle_seconds
        template = module_factory()
        self.name = template.name
        self.input_model = template.input_model
        self.output_model = template.output_model
        self.commands = template.commands
        self._slots: dict[str, _Slot] = {}
        self.forwarded = 0

    def keys(self) -> list[str]:
        """The keys with a live module instance."""
        return list(self._slots)

    async def serve(self) -> None:
        """Consume the topics and run modules until cancelled."""
        await self.transport.open()
        tasks = [asyncio.create_task(self._sweep(), name=f"{self.name}.host.sweep")]
        if self.input_model is not None:
            tasks.append(asyncio.create_task(self._inputs(), name=f"{self.name}.host.inputs"))
        tasks += [
            asyncio.create_task(self._commands(command), name=f"{self.name}.host.{command.topic}")
            for command in self.commands
        ]
        logger.info(
            "hosting %s: %s in, %s out, commands %s, over %s",
            self.name, self.input_model.topic if self.input_model else "nothing",
            self.output_model.topic, [c.topic for c in self.commands] or "none", self.transport.name,
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

    async def _inputs(self) -> None:
        with self.transport.subscribe(self.input_model) as inputs:
            async for key, message in inputs:
                slot = self._slot(key)
                slot.seen = time.monotonic()
                slot.publish(message)

    async def _commands(self, command_cls: type[Command]) -> None:
        # Commands across every key; each to its key's slot, built if new.
        with self.transport.subscribe(command_cls, overflow=Overflow.GROW) as commands:
            async for key, command in commands:
                slot = self._slot(key)
                slot.seen = time.monotonic()
                slot.publish(command)

    def _slot(self, key: str) -> _Slot:
        """This key's module, built on its first input."""
        slot = self._slots.get(key)
        if slot is None:
            slot = _Slot(key)
            self._slots[key] = slot
            slot.tasks.append(asyncio.create_task(self._start(slot), name=f"{self.name}@{key}.start"))
        return slot

    async def _start(self, slot: _Slot) -> None:
        module = self._factory()
        slot.bus.bind(asyncio.get_running_loop())
        slot.module = module
        if module.commands:
            slot.inbox = module.command_inbox(slot.bus)
            slot.inbox.start()
        slot.tasks += [
            asyncio.create_task(module.run(slot.bus), name=f"{self.name}@{slot.key}.run"),
            asyncio.create_task(self._forward(slot), name=f"{self.name}@{slot.key}.forward"),
        ]
        # One turn of the loop, so the run and forward tasks hold their
        # subscriptions before anything is published to them.
        await asyncio.sleep(0)
        slot.started = True
        while slot.pending:
            slot.bus.publish(slot.pending.popleft())
        while slot.pending_commands:
            slot.bus.publish(slot.pending_commands.popleft())
        logger.info("%s: module started for key %s (%d live)", self.name, slot.key, len(self._slots))

    async def _forward(self, slot: _Slot) -> None:
        # Results, and the module's ErrorEvents: both go back under the key.
        with slot.bus.subscribe(
            self.output_model, ErrorEvent, overflow=Overflow.DROP_OLDEST, maxsize=64
        ) as results:
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
        if slot.inbox is not None:
            await slot.inbox.stop()
        slot.bus.bind(None)
        logger.info("%s: module evicted for key %s (%s), %d live", self.name, key, reason, len(self._slots))


def main(
    module_factory: Callable[[], Module],
    variable: str,
    *,
    idle_seconds: float = DEFAULT_IDLE_SECONDS,
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
