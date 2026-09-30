# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""Where a module runs: ``ModuleHost``, one instance per pipeline key.

A pipeline never holds its modules. It publishes their input on the transport,
under its key, and listens for their results; a host on the other side of the
transport runs the module::

    topic <app>.<input>, key k   ─▶ ModuleHost(M): the instance for key k ─▶ M.process
    topic <app>.<command>, key k ─▶                   its command inbox  ─▶ M.handle
    topic <app>.<result>, key k  ◀─ what M publishes, and its ErrorEvents on <app>.error.event

**One instance per key**, built on the first message for that key: a
per-client pipeline costs one module instance per client wherever the host
runs. The instance is dropped when its pipeline says ``PipelineClosed``, or --
as a backstop, if that message was lost -- after ``idle_seconds`` with nothing
for it.

**Where the host runs is the deployment's choice.** With the in-memory
transport the server starts the hosts in its own process
(:func:`serve_hosts`, from its lifespan); with a broker, a worker process does
(:mod:`pswamp_core.worker`). The module cannot tell the difference.

**A hosted module may read the gateway.** Each instance's ``setup`` gets a
gateway of its own, built by the family's factory from the same configuration
the pipeline's player reads -- so a batch module (the explorer's row count)
asks the provider itself, wherever it runs. The host does not open it: a
history client needs no opening, and a live feed opened for every key a worker
hosts would be a feed per key that nobody reads.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable, Sequence
from typing import TYPE_CHECKING

from .command_routing import CommandInbox, concrete_commands
from .datagateway.data_gateway import DataGateway
from .log import get_logger
from .messages.control import PipelineClosed
from .subscription import Overflow, Subscription
from .transport import Outbox
from .util.tasks import cancel_and_wait, finish

if TYPE_CHECKING:
    from .modules import Module
    from .pipeline import PipelineFamily
    from .transport import Transport, TransportSubscription

__all__ = ["DEFAULT_IDLE_SECONDS", "ModuleHost", "hosts_for", "serve_hosts"]

logger = get_logger("pswamp_core.host")

DEFAULT_IDLE_SECONDS = 300.0

#: The shared feed of one input topic, across every key: generous, since it is
#: only a hand-off to each key's own queue (which applies the module's policy).
_FEED_MAXSIZE = 1024


class _Local:
    """The owner of a slot's queues: the host offers into them directly."""

    def _detach(self, subscription: Subscription) -> None:
        return


_LOCAL = _Local()


class _Slot:
    """One key's module instance, its queues, its outbox and its tasks."""

    def __init__(self, key: str, module: Module, transport: Transport, app: str) -> None:
        self.key = key
        self.module = module
        # The queues exist from the moment the slot does, so nothing that
        # arrives while the module is still in setup is lost: the module's own
        # overflow policy applies to its input, and commands are never dropped.
        self.inputs = Subscription(
            _LOCAL,
            (module.input_model,) if module.input_model is not None else (),
            module.overflow,
            module.maxsize,
        )
        self.commands = Subscription(_LOCAL, module.commands, Overflow.GROW, 0)
        self.out = Outbox(transport, app=app, key=key)
        self.gateway: DataGateway | None = None
        self.inbox: CommandInbox | None = None
        self.tasks: list[asyncio.Task] = []
        self.seen = time.monotonic()


class ModuleHost:
    """Runs one module instance per pipeline key, off the transport.

    Args:
        module: The module class, or a factory building a fresh instance.
        transport: The process's transport; opened by ``serve``.
        app: The app whose topics the module is reached on.
        gateway: Builds a gateway for each instance's ``setup``; an empty one
            by default.
        idle_seconds: How long a key may go without a message before its
            instance is dropped, should its ``PipelineClosed`` never come.
    """

    def __init__(
        self,
        module: type[Module] | Callable[[], Module],
        transport: Transport,
        *,
        app: str,
        gateway: Callable[[], DataGateway] = DataGateway,
        idle_seconds: float = DEFAULT_IDLE_SECONDS,
    ) -> None:
        self._factory = module
        self.transport = transport
        self.app = app
        self._gateway = gateway
        self.idle_seconds = idle_seconds
        template = module()
        self.name = template.name
        self.input_model = template.input_model
        self.output_model = template.output_model
        self.commands = concrete_commands(type(template).__name__, template.commands)
        self._slots: dict[str, _Slot] = {}

    def keys(self) -> list[str]:
        """The keys with a live module instance."""
        return list(self._slots)

    async def serve(self) -> None:
        """Consume the topics and run instances until cancelled.

        The subscriptions are made before the first await that can suspend,
        so a message published once this has had one turn of the loop is
        never missed."""
        await self.transport.open()
        app, feeds = self.app, []
        feeds.append(self._closed(self.transport.subscribe(PipelineClosed, app=app, overflow=Overflow.GROW)))
        if self.input_model is not None:
            feeds.append(self._inputs(self.transport.subscribe(self.input_model, app=app, maxsize=_FEED_MAXSIZE)))
        if self.commands:
            feeds.append(self._commands(self.transport.subscribe(*self.commands, app=app, overflow=Overflow.GROW)))
        tasks = [asyncio.create_task(self._sweep(), name=f"{self.name}.host.sweep")]
        tasks += [asyncio.create_task(feed, name=f"{self.name}.host.feed") for feed in feeds]
        logger.info(
            "hosting %s for %s: %s in, %s out, commands %s, over %s",
            self.name, app, self.input_model.topic if self.input_model else "nothing",
            self.output_model.topic, [c.topic for c in self.commands] or "none", self.transport.name,
        )
        try:
            await asyncio.gather(*tasks)
        finally:
            await cancel_and_wait(*tasks, ignore=(Exception,))
            for key in list(self._slots):
                await self._evict(key, "shutdown")

    # --- the topics ----------------------------------------------------------------

    async def _inputs(self, feed: TransportSubscription) -> None:
        with feed:
            async for key, message in feed:
                slot = self._slot(key)
                slot.seen = time.monotonic()
                slot.inputs.offer(message)

    async def _commands(self, feed: TransportSubscription) -> None:
        with feed:
            async for key, command in feed:
                slot = self._slot(key)
                slot.seen = time.monotonic()
                slot.commands.offer(command)

    async def _closed(self, feed: TransportSubscription) -> None:
        with feed:
            async for key, closed in feed:
                await self._evict(key, closed.reason)

    def _slot(self, key: str) -> _Slot:
        """This key's instance, built on its first message."""
        slot = self._slots.get(key)
        if slot is None:
            slot = _Slot(key, self._factory(), self.transport, self.app)
            self._slots[key] = slot
            slot.tasks.append(asyncio.create_task(self._start(slot), name=f"{self.name}@{key}.start"))
        return slot

    async def _start(self, slot: _Slot) -> None:
        module = slot.module
        slot.out.start()
        slot.gateway = self._gateway()
        await module.setup(slot.gateway, slot.out)
        if module.commands:
            slot.inbox = module.command_inbox(slot.commands, slot.out)
            slot.inbox.start()
        slot.tasks.append(
            asyncio.create_task(module.run(slot.inputs, slot.out), name=f"{self.name}@{slot.key}.run")
        )
        logger.info("%s: module started for key %s (%d live)", self.name, slot.key, len(self._slots))

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
        # To its end even if this host is cancelled (shut down) meanwhile; the
        # cancellation moves on once the key is fully dropped.
        await finish(self._drop(slot, reason))

    async def _drop(self, slot: _Slot, reason: str) -> None:
        if slot.inbox is not None:
            await slot.inbox.stop()
        await cancel_and_wait(*slot.tasks, ignore=(Exception,))
        slot.inputs.close()
        slot.commands.close()
        await slot.out.close()
        if slot.gateway is not None:
            await slot.gateway.close()
        logger.info("%s: module dropped for key %s (%s), %d live", self.name, slot.key, reason, len(self._slots))


def hosts_for(
    family: PipelineFamily, transport: Transport, *, idle_seconds: float = DEFAULT_IDLE_SECONDS
) -> list[ModuleHost]:
    """One host per module class of ``family``."""
    return [
        ModuleHost(module, transport, app=family.app, gateway=family.gateway, idle_seconds=idle_seconds)
        for module in family.modules
    ]


async def serve_hosts(hosts: Sequence[ModuleHost]) -> None:
    """Serve every host until cancelled."""
    if hosts:
        await asyncio.gather(*(host.serve() for host in hosts))
