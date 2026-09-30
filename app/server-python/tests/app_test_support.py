# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""Helpers for the app tests (a plain module with a name of its own, so it
never shadows the core tests' ``support`` or a conftest).

``Watch`` lets a test see, by class, everything a pipeline's local view takes
in -- what its player publishes and what its modules answer -- the way an
endpoint does, but as queues a test can ``take`` from. ``Tap`` is a sink to
hand a module as its ``out`` (or to feed its commands from), which keeps what
it is given and offers it to subscriptions by class. ``fresh_transport``
gives each test a process transport of its own (the in-memory one), so no
pipeline or host leaks from one test into the next.
"""

from __future__ import annotations

import contextlib

import shared

from pswamp_core.messages import DataModel
from pswamp_core.pipeline import Pipeline
from pswamp_core.subscription import Overflow, Subscription
from pswamp_core.transport import InMemoryTransport


class Watch:
    """Every message a pipeline remembers, offered to subscriptions by class."""

    def __init__(self, pipeline: Pipeline) -> None:
        self._subscriptions: list[Subscription] = []
        remember = pipeline._remember

        def tee(message: DataModel) -> None:
            remember(message)
            for subscription in list(self._subscriptions):
                if isinstance(message, subscription.models):
                    subscription.offer(message)

        pipeline._remember = tee  # type: ignore[method-assign]

    def subscribe(
        self, *models: type[DataModel], overflow: Overflow = Overflow.GROW, maxsize: int = 256
    ) -> Subscription:
        subscription = Subscription(self, models, overflow, maxsize)
        self._subscriptions.append(subscription)
        return subscription

    def _detach(self, subscription: Subscription) -> None:
        if subscription in self._subscriptions:
            self._subscriptions.remove(subscription)


class Tap:
    """A sink that keeps what it is given and offers it to subscriptions by class."""

    def __init__(self) -> None:
        self._subscriptions: list[Subscription] = []
        self.published: list[DataModel] = []

    def publish(self, message: DataModel) -> None:
        self.published.append(message)
        for subscription in list(self._subscriptions):
            if isinstance(message, subscription.models):
                subscription.offer(message)

    def subscribe(
        self, *models: type[DataModel], overflow: Overflow = Overflow.GROW, maxsize: int = 256
    ) -> Subscription:
        subscription = Subscription(self, models, overflow, maxsize)
        self._subscriptions.append(subscription)
        return subscription

    def _detach(self, subscription: Subscription) -> None:
        if subscription in self._subscriptions:
            self._subscriptions.remove(subscription)


@contextlib.contextmanager
def fresh_transport(monkeypatch):
    """A new in-memory transport as the process's, for one test."""
    monkeypatch.delenv("PSWAMP_TRANSPORT", raising=False)
    monkeypatch.setattr(shared, "_TRANSPORT", InMemoryTransport())
    yield shared.transport()
