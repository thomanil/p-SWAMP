# SPDX-FileCopyrightText: 2026 Louis Pauchet <louis.pauchet@sintef.no>
# SPDX-License-Identifier: Apache-2.0

"""
The provider contract: what a data source plugged into the gateway implements.

This is all a deployment's own data source has to implement, and all it has
to import. A ``DataClient`` wraps one backend (a recording, a broker, a
time-series database, a remote service) and answers two questions: what time
window does it currently hold (``coverage``), and how does it stream that window
back (``consume``). ``coverage`` is evaluated afresh on every call, since most
backends have now-relative windows.

Providers **declare capabilities**, and the core never asks for what was not
declared: a client without ``HISTORY_CONSUME`` is never asked for a past window,
one without ``LIVE_CONSUME`` is never asked to tail.

A client is configured from the environment: it declares ``env_settings`` and
``from_env`` reads its ``{NAME}_{SETTING}`` block (:mod:`pswamp_core.settings`).
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import replace
from collections.abc import AsyncIterator, Iterable, Sequence
from enum import Flag, auto
from typing import TYPE_CHECKING, Any, ClassVar

from ..messages.data_model import DataModel
from ..settings import EnvSetting, MissingSettingError, env_key, format_settings, read_setting

if TYPE_CHECKING:
    from .time_range import Coverage, TimeRange

__all__ = [
    "Capability",
    "DataClient",
    "MRIDFilter",
    "ModelSelector",
    "can_consume",
    "normalise_models",
    "normalise_mrid_filter",
    "parse_capabilities",
]

#: Identifier filter accepted by consume calls.
MRIDFilter = str | Sequence[str] | None

#: Models a client serves: one class, or several. A parent class covers all of
#: its subclasses.
ModelSelector = type[DataModel] | Iterable[type[DataModel]]


def normalise_models(models: ModelSelector) -> set[type[DataModel]]:
    """
    Turn a model selector into a set of classes.

    Args:
        models: A single model class or an iterable of them.

    Returns:
        The declared classes. Subclasses are matched at lookup time rather than
        expanded here, so models defined later are still covered.
    """
    if isinstance(models, type):
        return {models}

    return set(models)


def normalise_mrid_filter(mRID: MRIDFilter) -> set[str] | None:
    """
    Turn an identifier filter into a set of strings.

    Args:
        mRID: A single identifier, a sequence of them, or ``None``.

    Returns:
        The identifiers to keep, or ``None`` when nothing should be filtered.
    """
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


def parse_capabilities(key: str, names: Iterable[str]) -> Capability:
    """``HISTORY_CONSUME,PRODUCE`` as a flag; ``key`` names the variable in errors."""
    resolved = Capability(0)
    for item in names:
        try:
            resolved |= Capability[item.strip().upper()]
        except KeyError as error:
            allowed = ", ".join(flag.name for flag in Capability if flag.name)
            raise MissingSettingError(
                f"{key} has unknown capability {item!r}; expected one of {allowed}"
            ) from error
    return resolved


def _read_client_setting(name: str, setting: EnvSetting) -> Any:
    """``read_setting``, plus the one kind only a client has: ``capabilities``."""
    if setting.kind != "capabilities":
        return read_setting(name, setting)
    names = read_setting(name, replace(setting, kind="list"))
    return None if names is None else parse_capabilities(env_key(name, setting.setting), names)


class DataClient(ABC):
    """
    Abstract data source.

    Attributes:
        name: Unique identifier of the client within a gateway.
        capabilities: Operations the client supports.
        supported_models: Model classes the client can store or stream. A parent
            class stands for its whole subclass family.
        priority: Preference when several clients cover the same instant.
            Higher wins.

    Class Attributes:
        env_settings: Environment variables ``from_env`` reads, each parsed per
            its ``kind`` and passed to the constructor as the lower-cased setting
            name. Rendered by :meth:`show_config`.
    """

    name: str
    capabilities: Capability
    supported_models: set[type[DataModel]]
    priority: int = 0

    env_settings: ClassVar[tuple[EnvSetting, ...]] = ()

    @classmethod
    def show_config(cls, name: str = "<name>") -> None:
        """
        Print the environment variables that configure this client.

        Args:
            name: Client name the variables would be prefixed with, so the
                printed keys match what you would actually set.
        """
        print(format_settings(cls.__name__, name, cls.env_settings))

    @classmethod
    def from_env(cls, name: str, **overrides: Any):
        """
        Build a client from its ``{NAME}_{SETTING}`` environment block.

        Every entry in ``env_settings`` is read per its ``kind`` and
        passed to the constructor as ``setting.lower()``; explicit ``overrides``
        win over the environment. A client whose constructor needs something the
        environment cannot express (model classes, say) takes it as an override.

        Raises:
            MissingSettingError: When a required variable is unset or malformed.
        """
        settings: dict[str, Any] = {}
        for setting in cls.env_settings:
            keyword = setting.setting.lower()
            if keyword in overrides:
                continue
            value = _read_client_setting(name, setting)
            if value is not None:
                settings[keyword] = value
        return cls(name=name, **settings, **overrides)

    def supports(
        self,
        model: type[DataModel],
        capability: Capability | None = None,
    ) -> bool:
        """
        Whether this client handles ``model``, optionally for a given capability.

        Args:
            model: Model class to check. Matches when it is, or descends from,
                one of the declared models.
            capability: Required capability, or ``None`` to ignore capabilities.

        Returns:
            ``True`` when the client can serve the request.
        """
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
        """
        Time window this client currently holds for ``model``.

        Implementations must recompute this on every call rather than caching,
        because the gateway uses it to decide when to hand over between sources.

        Args:
            model: Model class being queried.
            mRID: Optional identifier filter.

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
        """
        Stream stored records for ``model`` within ``time_range``.

        Implementations must honour three rules the gateway depends on:

        1. Every yielded payload carries a non-``None`` ``timestamp``.
        2. Timestamps are non-decreasing.
        3. The iterator stops once ``time_range.end`` is reached. When the range
           is open and the client has :attr:`Capability.LIVE_CONSUME`, it instead
           keeps yielding indefinitely until the consumer closes it.

        Args:
            model: Model class to stream.
            time_range: Window to stream, already clipped by the gateway.
            mRID: Optional identifier filter.

        Returns:
            An async iterator over the matching payloads.
        """

    @abstractmethod
    async def produce(self, data: DataModel) -> None:
        """
        Write a payload to the backend.

        Args:
            data: Payload to store. Its ``timestamp`` is already set.
        """


def can_consume(
    client: DataClient, model: type[DataModel], capability: Capability | None = None
) -> bool:
    """
    Whether ``client`` can be *read* for ``model``.

    With ``capability`` given, exactly that one; with ``None``, either consume
    capability. A ``PRODUCE``-only client is never a source. The gateway's
    ``coverage`` and the planner's offers both go through this, so the two can
    never disagree about which clients count as sources.
    """
    if capability is not None:
        return client.supports(model, capability)
    return client.supports(model, Capability.HISTORY_CONSUME) or client.supports(
        model, Capability.LIVE_CONSUME
    )
