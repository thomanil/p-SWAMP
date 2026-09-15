# SPDX-FileCopyrightText: 2026 Louis Pauchet <louis.pauchet@sintef.no>
# SPDX-License-Identifier: Apache-2.0

"""
Environment-variable configuration for data clients.

Lifted from the test_pswamp draft (``core/datagateway/config.py``). Every setting
is namespaced by the client's own name, so several clients of the same type
coexist without colliding::

    ARCHIVE_DIRECTORY=/var/lib/pswamp/archive
    ARCHIVE_PRIORITY=10
    BUS_BOOTSTRAP_SERVERS=kafka-1:9092,kafka-2:9092
    BUS_RETENTION_SECONDS=1200

Model classes are never read from the environment: they are types, so they stay
in code.

Adapted from the draft, two additions:

* ``EnvSetting.kind`` says how a variable is parsed, which is what lets
  ``DataClient.from_env`` live once on the base class (the draft wrote one
  ``from_env`` per client) -- see :func:`read_setting`.
* :func:`gateway_from_env` composes a whole gateway from ``PSWAMP_DATA_CLIENTS``,
  so a deployment names *which* clients to run, not only how to configure each.
  The draft composed the gateway in code.
"""

from __future__ import annotations

import importlib
import os
import re
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

if TYPE_CHECKING:
    from collections.abc import Iterable

    from .data_client_model import Capability, DataClient
    from .data_gateway import DataGateway

__all__ = [
    "DATA_CLIENTS_VARIABLE",
    "EnvSetting",
    "MissingSettingError",
    "SettingKind",
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
    "gateway_from_env",
    "parse_client_specs",
    "read_setting",
]

#: The variable naming the clients a gateway is built from; see gateway_from_env.
DATA_CLIENTS_VARIABLE = "PSWAMP_DATA_CLIENTS"

#: How a setting's text is parsed. ``path`` yields a ``pathlib.Path``.
SettingKind = Literal["str", "int", "float", "seconds", "list", "capabilities", "path"]


class MissingSettingError(RuntimeError):
    """Raised when a client is built from an environment that lacks a setting."""


@dataclass(frozen=True, slots=True)
class EnvSetting:
    """
    One environment variable a client understands.

    Attributes:
        setting: Suffix appended to the client name, such as ``"DIRECTORY"``. The
            lower-cased suffix is also the constructor keyword ``from_env`` passes.
        description: One-line explanation shown by ``show_config``.
        required: Whether the client refuses to start without it.
        default: Value used when the variable is unset, rendered as text.
        kind: How the text is parsed before it reaches the constructor.
    """

    setting: str
    description: str
    required: bool = False
    default: str | None = None
    kind: SettingKind = "str"


def env_key(name: str, setting: str) -> str:
    """
    Environment variable backing ``setting`` for the client called ``name``.

    Args:
        name: Client name, in any case or with separators.
        setting: Setting name, such as ``"DIRECTORY"``.

    Returns:
        The variable name, for example ``ARCHIVE_DIRECTORY``.
    """
    normalised = re.sub(r"[^A-Za-z0-9]+", "_", name).strip("_").upper()

    return f"{normalised}_{setting.upper()}"


def env_str(name: str, setting: str, default: str | None = None) -> str | None:
    """Read a setting as text, falling back to ``default`` when unset or blank."""
    value = os.environ.get(env_key(name, setting), "").strip()

    return value or default


def env_required(name: str, setting: str) -> str:
    """
    Read a setting that has no sensible default.

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
    """
    Read a setting as an integer.

    Raises:
        MissingSettingError: When the value is not a valid integer.
    """
    value = env_str(name, setting)

    if value is None:
        return default

    try:
        return int(value)
    except ValueError as error:
        raise MissingSettingError(
            f"{env_key(name, setting)} must be an integer"
        ) from error


def env_float(name: str, setting: str, default: float) -> float:
    """
    Read a setting as a number.

    Raises:
        MissingSettingError: When the value is not a valid number.
    """
    value = env_str(name, setting)

    if value is None:
        return default

    try:
        return float(value)
    except ValueError as error:
        raise MissingSettingError(
            f"{env_key(name, setting)} must be a number"
        ) from error


def env_seconds(name: str, setting: str, default: timedelta) -> timedelta:
    """
    Read a setting expressed in seconds as a duration.

    Raises:
        MissingSettingError: When the value is not a valid number.
    """
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
    """
    Read a comma-separated list of capability names.

    Args:
        name: Client name.
        setting: Setting name, usually ``"CAPABILITIES"``.
        default: Value to use when the variable is unset.

    Returns:
        The combined capability flag, for example ``HISTORY_CONSUME,PRODUCE``.

    Raises:
        MissingSettingError: When a name does not match a known capability.
    """
    names = env_list(name, setting)

    if names is None:
        return default

    return _parse_capabilities(name, setting, names)


def _parse_capabilities(name: str, setting: str, names: list[str]) -> Capability:
    from .data_client_model import Capability

    resolved = Capability(0)

    for item in names:
        try:
            resolved |= Capability[item.upper()]
        except KeyError as error:
            allowed = ", ".join(flag.name for flag in Capability if flag.name)
            raise MissingSettingError(
                f"{env_key(name, setting)} has unknown capability {item!r}; expected one of {allowed}"
            ) from error

    return resolved


def format_capabilities(capabilities: Capability) -> str:
    """Render a capability flag the way ``{NAME}_CAPABILITIES`` expects it."""
    return ",".join(
        flag.name for flag in type(capabilities) if flag.name and flag in capabilities
    )


def read_setting(name: str, setting: EnvSetting) -> Any:
    """
    Read one declared setting for the client ``name``, parsed per its ``kind``.

    A missing required setting raises; a missing optional one yields the parsed
    ``default`` (or ``None`` when there is none). This is what
    ``DataClient.from_env`` calls for every entry in ``env_settings``.

    Raises:
        MissingSettingError: When the value is missing and required, or malformed.
    """
    if setting.required:
        env_required(name, setting.setting)
    raw = env_str(name, setting.setting, setting.default)

    if raw is None:
        return None

    key = env_key(name, setting.setting)
    kind = setting.kind
    try:
        if kind == "str":
            return raw
        if kind == "int":
            return int(raw)
        if kind == "float":
            return float(raw)
        if kind == "seconds":
            return timedelta(seconds=float(raw))
        if kind == "list":
            return [item.strip() for item in raw.split(",") if item.strip()]
        if kind == "capabilities":
            return _parse_capabilities(
                name, setting.setting, [item.strip() for item in raw.split(",") if item.strip()]
            )
        if kind == "path":
            return Path(raw)
    except ValueError as error:
        raise MissingSettingError(f"{key} must be a valid {kind}: {raw!r}") from error

    raise MissingSettingError(f"{key}: unknown setting kind {kind!r}")


def format_settings(client: str, name: str, settings: Iterable[EnvSetting]) -> str:
    """
    Render the environment variables a client understands as a table.

    Args:
        client: Class name, used as the heading.
        name: Client name the variables would be prefixed with.
        settings: Declared settings.

    Returns:
            The table as text, ready to print.
    """
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


# --- composing a gateway from the environment --------------------------------


def parse_client_specs(spec: str) -> list[tuple[str, str, str]]:
    """
    Split a ``PSWAMP_DATA_CLIENTS`` value into ``(name, module, class)`` triples.

    The format is ``name:module.path:ClassName``, comma-separated::

        PSWAMP_DATA_CLIENTS="sample:pmu_test_streamer.sample_client:SampleRecordingClient"
        PSWAMP_DATA_CLIENTS="live:acme_tso.pmu:KafkaFeed,history:acme_tso.pmu:TimescaleClient"

    Raises:
        MissingSettingError: On a malformed entry.
    """
    specs: list[tuple[str, str, str]] = []
    for entry in (item.strip() for item in spec.split(",")):
        if not entry:
            continue
        parts = entry.split(":")
        if len(parts) != 3 or not all(parts):
            raise MissingSettingError(
                f"{DATA_CLIENTS_VARIABLE} entry {entry!r} is not name:module.path:ClassName"
            )
        specs.append((parts[0], parts[1], parts[2]))
    if not specs:
        raise MissingSettingError(f"{DATA_CLIENTS_VARIABLE} names no clients")
    return specs


def load_client_class(module_path: str, class_name: str) -> type[DataClient]:
    """Import ``class_name`` from ``module_path`` and check it is a ``DataClient``."""
    from .data_client_model import DataClient

    try:
        module = importlib.import_module(module_path)
    except ImportError as error:
        raise MissingSettingError(
            f"{DATA_CLIENTS_VARIABLE}: cannot import module {module_path!r}: {error}"
        ) from error
    cls = getattr(module, class_name, None)
    if cls is None or not isinstance(cls, type) or not issubclass(cls, DataClient):
        raise MissingSettingError(
            f"{DATA_CLIENTS_VARIABLE}: {module_path}.{class_name} is not a DataClient"
        )
    return cls


def gateway_from_env(
    default: str | None = None,
    *,
    variable: str = DATA_CLIENTS_VARIABLE,
    **gateway_options: Any,
) -> DataGateway:
    """
    Build a ``DataGateway`` from the clients the environment names.

    Reads ``variable`` (``PSWAMP_DATA_CLIENTS`` by default), falling back to
    ``default`` when it is unset, and constructs each named client with its own
    ``from_env(name)`` -- so every client then reads its ``{NAME}_{SETTING}``
    block. This is the whole mechanism by which a deployment plugs in its own
    provider: an image with one extra package, and one variable naming it.

    Args:
        default: The spec to use when the variable is unset; the repo's own
            sample provider, typically.
        variable: The environment variable to read.
        **gateway_options: Passed to ``DataGateway`` (``on_gap``, ...).

    Raises:
        MissingSettingError: When neither the variable nor ``default`` names a
            client, or a client cannot be imported or configured.
    """
    from .data_gateway import DataGateway

    spec = os.environ.get(variable, "").strip() or default
    if not spec:
        raise MissingSettingError(f"{variable} is unset and no default was given")

    clients = [
        load_client_class(module_path, class_name).from_env(name)
        for name, module_path, class_name in parse_client_specs(spec)
    ]
    return DataGateway(clients, **gateway_options)
