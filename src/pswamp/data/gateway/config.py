# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.
# Lifted from Louis Pauchet's data-gateway draft (SINTEF, 2026), see STEP3 §13;
# ``build_gateway_from_env`` and ``resolve_client_type`` added (STEP3 §5.5).

"""Environment-variable configuration for data clients.

Every setting is namespaced by the client's own name, so several clients of the
same type coexist without colliding::

    ARCHIVE_DIRECTORY=/var/lib/pswamp/archive
    ARCHIVE_PRIORITY=10
    BUS_BOOTSTRAP_SERVERS=kafka-1:9092,kafka-2:9092

Model classes are never read from the environment: they are types, so they stay
in code and are passed to ``from_env`` directly.

Which clients a process runs is itself configuration (STEP3 §5.5)::

    PSWAMP_CLIENTS=source,bus                 # ordered client names
    SOURCE_TYPE=my_tso.clients:TimescaleClient # a module:Class path …
    BUS_TYPE=in_memory                         # … or a built-in name

With ``PSWAMP_CLIENTS`` unset, :func:`build_gateway_from_env` uses whatever
defaults the caller passes — on the dev laptop, the committed sample data and the
in-process bus. That is how a deployment names its own provider without touching
the repo (STEP1 A8).
"""

from __future__ import annotations

import importlib
import os
import re
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass
from datetime import timedelta
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ..models.base import DataModel
    from .client import Capability, DataClient
    from .gateway import DataGateway

__all__ = [
    "CLIENTS_SETTING",
    "EnvSetting",
    "MissingSettingError",
    "build_gateway_from_env",
    "env_bool",
    "env_capabilities",
    "env_float",
    "env_int",
    "env_key",
    "env_list",
    "env_required",
    "env_seconds",
    "env_str",
    "format_capabilities",
    "format_settings",
    "resolve_client_type",
]

#: The one process-level variable: which named clients to build, in order.
CLIENTS_SETTING = "PSWAMP_CLIENTS"


class MissingSettingError(RuntimeError):
    """Raised when a client is built from an environment that lacks a setting."""


@dataclass(frozen=True, slots=True)
class EnvSetting:
    """One environment variable a client understands.

    Attributes:
        setting: Suffix appended to the client name, such as ``"DIRECTORY"``.
        description: One-line explanation shown by ``show_config``.
        required: Whether the client refuses to start without it.
        default: Value used when the variable is unset, rendered as text.
    """

    setting: str
    description: str
    required: bool = False
    default: str | None = None


def env_key(name: str, setting: str) -> str:
    """Environment variable backing ``setting`` for the client called ``name``.

    ``env_key("archive", "DIRECTORY")`` is ``ARCHIVE_DIRECTORY``.
    """
    normalised = re.sub(r"[^A-Za-z0-9]+", "_", name).strip("_").upper()

    return f"{normalised}_{setting.upper()}"


def env_str(name: str, setting: str, default: str | None = None) -> str | None:
    """Read a setting as text, falling back to ``default`` when unset or blank."""
    value = os.environ.get(env_key(name, setting), "").strip()

    return value or default


def env_required(name: str, setting: str) -> str:
    """Read a setting that has no sensible default.

    Raises:
        MissingSettingError: When the variable is unset or blank.
    """
    value = env_str(name, setting)

    if value is None:
        raise MissingSettingError(
            f"{env_key(name, setting)} is required to configure {name!r}"
        )

    return value


def env_list(
    name: str, setting: str, default: list[str] | None = None
) -> list[str] | None:
    """Read a comma-separated setting."""
    value = env_str(name, setting)

    if value is None:
        return default

    return [item.strip() for item in value.split(",") if item.strip()]


def env_int(name: str, setting: str, default: int) -> int:
    """Read a setting as an integer."""
    value = env_str(name, setting)

    if value is None:
        return default

    try:
        return int(value)
    except ValueError as error:
        raise MissingSettingError(f"{env_key(name, setting)} must be an integer") from error


def env_float(name: str, setting: str, default: float) -> float:
    """Read a setting as a number."""
    value = env_str(name, setting)

    if value is None:
        return default

    try:
        return float(value)
    except ValueError as error:
        raise MissingSettingError(f"{env_key(name, setting)} must be a number") from error


def env_bool(name: str, setting: str, default: bool) -> bool:
    """Read a setting as a boolean (``true/false``, ``1/0``, ``yes/no``)."""
    value = env_str(name, setting)

    if value is None:
        return default

    lowered = value.lower()
    if lowered in ("1", "true", "yes", "on"):
        return True
    if lowered in ("0", "false", "no", "off"):
        return False
    raise MissingSettingError(f"{env_key(name, setting)} must be a boolean")


def env_seconds(name: str, setting: str, default: timedelta) -> timedelta:
    """Read a setting expressed in seconds as a duration."""
    value = env_str(name, setting)

    if value is None:
        return default

    try:
        return timedelta(seconds=float(value))
    except ValueError as error:
        raise MissingSettingError(
            f"{env_key(name, setting)} must be a number of seconds"
        ) from error


def env_capabilities(name: str, setting: str, default: Capability) -> Capability:
    """Read a comma-separated list of capability names, e.g. ``HISTORY_CONSUME,PRODUCE``."""
    from .client import Capability

    names = env_list(name, setting)

    if names is None:
        return default

    resolved = Capability(0)

    for item in names:
        try:
            resolved |= Capability[item.upper()]
        except KeyError as error:
            allowed = ", ".join(flag.name for flag in Capability)
            raise MissingSettingError(
                f"{env_key(name, setting)} has unknown capability {item!r}; "
                f"expected one of {allowed}"
            ) from error

    return resolved


def format_capabilities(capabilities: Capability) -> str:
    """Render a capability flag the way ``{NAME}_CAPABILITIES`` expects it."""
    return ",".join(flag.name for flag in type(capabilities) if flag in capabilities)


def format_settings(client: str, name: str, settings: Iterable[EnvSetting]) -> str:
    """Render the environment variables a client understands as a table."""
    rows = [
        (
            env_key(name, setting.setting),
            "required" if setting.required else f"default: {setting.default}",
            setting.description,
        )
        for setting in settings
    ]

    if not rows:
        return f"{client} takes no environment configuration."

    key_width = max(len(row[0]) for row in rows)
    state_width = max(len(row[1]) for row in rows)

    lines = [f"{client} configuration for client {name!r}:", ""]
    lines += [
        f"  {key:<{key_width}}  {state:<{state_width}}  {description}"
        for key, state, description in rows
    ]

    return "\n".join(lines)


# --- which clients to build ------------------------------------------------


def resolve_client_type(
    spec: str, registry: dict[str, type[DataClient]]
) -> type[DataClient]:
    """Turn a ``<NAME>_TYPE`` value into a client class.

    ``spec`` is either a key of ``registry`` (the built-in short names) or a
    ``package.module:ClassName`` path to a class a deployment ships itself. The
    latter is the whole out-of-repo provider story: the class imports the
    contract from ``pswamp.data`` and nothing else, and is named by configuration.
    """
    if spec in registry:
        return registry[spec]

    module_name, sep, attr = spec.partition(":")
    if not sep or not module_name or not attr:
        known = ", ".join(sorted(registry)) or "(none)"
        raise MissingSettingError(
            f"client type {spec!r} is neither a built-in name ({known}) nor a "
            f"module:Class path"
        )

    try:
        module = importlib.import_module(module_name)
    except ImportError as error:
        raise MissingSettingError(f"cannot import {module_name!r} for client type {spec!r}") from error

    client_type = getattr(module, attr, None)
    if client_type is None:
        raise MissingSettingError(f"{module_name!r} has no attribute {attr!r}")
    return client_type


def build_gateway_from_env(
    models: Sequence[type[DataModel]],
    *,
    defaults: Callable[[], list[DataClient]],
    registry: dict[str, type[DataClient]],
) -> DataGateway:
    """The process's gateway, from ``PSWAMP_CLIENTS`` or from ``defaults()``.

    Args:
        models: The message types every configured client is offered. A client's
            ``from_env`` narrows that to what it actually serves.
        defaults: Builds the client list used when ``PSWAMP_CLIENTS`` is unset —
            the dev/CI experience, with nothing configured.
        registry: Built-in short names a ``<NAME>_TYPE`` may use.
    """
    from .gateway import DataGateway

    names = [item.strip() for item in os.environ.get(CLIENTS_SETTING, "").split(",") if item.strip()]

    if not names:
        return DataGateway(defaults())

    clients = []
    for name in names:
        spec = env_required(name, "TYPE")
        client_type = resolve_client_type(spec, registry)
        clients.append(client_type.from_env(name, models))

    return DataGateway(clients)
