# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.
# Lifted from Louis Pauchet's data-gateway draft (SINTEF, 2026), see STEP3 §13.

"""Contract 2: the provider seam, as a capability-declared client.

A :class:`DataClient` wraps one backend (a recording, a CSV archive, a Kafka
topic, a time-series database, a live PDC) and answers three questions: what do
you *hold* (:meth:`~DataClient.coverage`), stream me that window
(:meth:`~DataClient.consume`), and store this (:meth:`~DataClient.produce`).
What it *can* do is a declaration (:class:`Capability`), so "cannot seek" is a
reported fact rather than a method implemented as ``pass``.

The gateway relies on ``coverage`` being evaluated afresh on every call, since
most backends have now-relative windows: a temporal database may hold
``[2020-01-01, now - 10min]`` while a Kafka topic holds ``[now - 20min, now]``
and can follow live.

This replaces the nine-method io duck type STEP1 §2.3 found implemented seven
times and declared nowhere.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator, Iterable, Sequence
from enum import Flag, auto
from typing import TYPE_CHECKING, ClassVar, Self

from ..models.base import DataModel, MRIDType
from .config import EnvSetting, format_settings

if TYPE_CHECKING:
    from .time_range import Coverage, TimeRange

__all__ = [
    "Capability",
    "DataClient",
    "MRIDFilter",
    "ModelSelector",
    "normalise_models",
    "normalise_mrid_filter",
]

#: Identifier filter accepted by consume calls.
MRIDFilter = MRIDType | Sequence[MRIDType] | None

#: Models a client serves: one class, or several. A parent class covers all of
#: its subclasses.
ModelSelector = type[DataModel] | Iterable[type[DataModel]]


def normalise_models(models: ModelSelector) -> set[type[DataModel]]:
    """Turn a model selector into a set of classes.

    Subclasses are matched at lookup time rather than expanded here, so models
    defined later are still covered.
    """
    if isinstance(models, type):
        return {models}

    return set(models)


def normalise_mrid_filter(mRID: MRIDFilter) -> set[str] | None:
    """Turn an identifier filter into a set of strings, or ``None`` for "all"."""
    if mRID is None:
        return None

    if isinstance(mRID, (str, bytes)) or not isinstance(mRID, Sequence):
        return {str(mRID)}

    return {str(item) for item in mRID}


class Capability(Flag):
    """What a client is allowed to do."""

    LIVE_CONSUME = auto()
    HISTORY_CONSUME = auto()
    PRODUCE = auto()


class DataClient(ABC):
    """Abstract data source.

    Attributes:
        name: Unique identifier of the client within a gateway.
        capabilities: Operations the client supports.
        supported_models: Model classes the client can store or stream. A parent
            class stands for its whole subclass family.
        priority: Preference when several clients cover the same instant.
            Higher wins.

    Class Attributes:
        env_settings: Environment variables the client's ``from_env`` reads.
            Rendered by :meth:`show_config`.
    """

    name: str
    capabilities: Capability
    supported_models: set[type[DataModel]]
    priority: int = 0

    env_settings: ClassVar[tuple[EnvSetting, ...]] = ()

    @classmethod
    def from_env(cls, name: str, models: ModelSelector) -> Self:
        """Build a client called ``name`` from its ``<NAME>_*`` environment.

        Every client that can be selected by configuration overrides this; the
        default refuses, so a client type that took no thought about
        deployment cannot be silently instantiated with nothing configured.
        """
        raise NotImplementedError(
            f"{cls.__name__} cannot be configured from the environment; "
            f"construct it in code or implement from_env()"
        )

    @classmethod
    def show_config(cls, name: str = "<name>") -> None:
        """Print the environment variables that configure this client."""
        print(format_settings(cls.__name__, name, cls.env_settings))

    def supports(
        self,
        model: type[DataModel],
        capability: Capability | None = None,
    ) -> bool:
        """Whether this client handles ``model``, optionally for a given capability."""
        if not any(issubclass(model, declared) for declared in self.supported_models):
            return False

        return capability is None or capability in self.capabilities

    async def open(self) -> None:
        """Acquire backend resources. Called once by the gateway."""
        return

    async def close(self) -> None:
        """Release backend resources. Called once by the gateway."""
        return

    @abstractmethod
    async def coverage(
        self,
        model: type[DataModel],
        mRID: MRIDFilter = None,
    ) -> Coverage | None:
        """Time window this client currently holds for ``model``.

        Implementations must recompute this on every call rather than caching,
        because the gateway uses it to decide when to hand over between sources.

        Returns:
            The current coverage, or ``None`` when the client holds nothing.
        """

    @abstractmethod
    def consume(
        self,
        model: type[DataModel],
        time_range: TimeRange,
        mRID: MRIDFilter = None,
    ) -> AsyncIterator[DataModel]:
        """Stream stored records for ``model`` within ``time_range``.

        Implementations must honour three rules the gateway depends on:

        1. Every yielded payload carries a non-``None`` ``timestamp``.
        2. Timestamps are non-decreasing.
        3. The iterator stops once ``time_range.end`` is reached. When the range
           is open and the client has :attr:`Capability.LIVE_CONSUME`, it instead
           keeps yielding indefinitely until the consumer closes it.
        """

    @abstractmethod
    async def produce(self, data: DataModel) -> None:
        """Write a payload to the backend. Its ``timestamp`` is already set."""
