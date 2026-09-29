# SPDX-FileCopyrightText: 2026 Louis Pauchet <louis.pauchet@sintef.no>
# SPDX-License-Identifier: Apache-2.0

"""Configuration from the environment, for anything plugged in by name.

A transport (and later a provider) is **named** by one variable and
**configured** by a block of its own, every setting prefixed by its name::

    PMU_TEST_STREAMER_MODULE_TRANSPORT=kafka:pswamp_core.transport.kafka:KafkaTransport
    KAFKA_BOOTSTRAP_SERVERS=kafka:9092

The class declares its settings as ``EnvSetting``s; :func:`read_setting` parses
each one per its ``kind``, and :func:`format_settings` prints them, so a
deployment can ask a class what it needs. Types (message classes) are never
read from the environment: they stay in code.
"""

from __future__ import annotations

import importlib
import os
import re
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, TypeVar

if TYPE_CHECKING:
    from collections.abc import Iterable

__all__ = [
    "EnvSetting",
    "MissingSettingError",
    "SettingKind",
    "env_key",
    "format_settings",
    "load_class",
    "parse_spec",
    "read_setting",
]

#: How a setting's text is parsed. ``path`` yields a ``pathlib.Path``;
#: ``capabilities`` is a provider's, parsed by ``DataClient.from_env``.
SettingKind = Literal["str", "int", "float", "seconds", "list", "path", "capabilities"]

T = TypeVar("T")


class MissingSettingError(RuntimeError):
    """Raised when something is built from an environment that lacks a setting."""


@dataclass(frozen=True, slots=True)
class EnvSetting:
    """One environment variable a class understands.

    ``setting`` is the suffix after the name (``"DIRECTORY"``); lower-cased, it
    is also the constructor keyword ``from_env`` passes.
    """

    setting: str
    description: str
    required: bool = False
    default: str | None = None
    kind: SettingKind = "str"


def env_key(name: str, setting: str) -> str:
    """The variable backing ``setting`` for ``name``: ``("cold-archive", "DIRECTORY")``
    is ``COLD_ARCHIVE_DIRECTORY``."""
    normalised = re.sub(r"[^A-Za-z0-9]+", "_", name).strip("_").upper()
    return f"{normalised}_{setting.upper()}"


def _env_str(name: str, setting: str, default: str | None = None) -> str | None:
    value = os.environ.get(env_key(name, setting), "").strip()
    return value or default


def read_setting(name: str, setting: EnvSetting) -> Any:
    """Read one declared setting for ``name``, parsed per its ``kind``.

    A missing required setting raises; a missing optional one yields the parsed
    ``default`` (or ``None``).
    """
    key = env_key(name, setting.setting)
    raw = _env_str(name, setting.setting, setting.default)
    if raw is None:
        if setting.required:
            raise MissingSettingError(f"{key} is required to configure {name!r}")
        return None

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
        if kind == "path":
            return Path(raw)
    except ValueError as error:
        raise MissingSettingError(f"{key} must be a valid {kind}: {raw!r}") from error
    raise MissingSettingError(f"{key}: unknown setting kind {kind!r}")


def format_settings(owner: str, name: str, settings: Iterable[EnvSetting]) -> str:
    """The environment variables ``owner`` (a class name) reads for ``name``, as a table."""
    rows = [
        (
            env_key(name, setting.setting),
            "required" if setting.required else f"default: {setting.default}",
            setting.description,
        )
        for setting in settings
    ]
    if not rows:
        return f"{owner} takes no environment configuration."
    key_width = max(len(row[0]) for row in rows)
    state_width = max(len(row[1]) for row in rows)
    lines = [f"{owner} configuration for {name!r}:", ""]
    lines += [
        f"  {key:<{key_width}}  {state:<{state_width}}  {description}"
        for key, state, description in rows
    ]
    return "\n".join(lines)


def parse_spec(variable: str, entry: str) -> tuple[str, str, str]:
    """Split one ``name:module.path:ClassName`` entry into its three parts."""
    parts = entry.strip().split(":")
    if len(parts) != 3 or not all(parts):
        raise MissingSettingError(f"{variable} entry {entry!r} is not name:module.path:ClassName")
    return parts[0], parts[1], parts[2]


def load_class(variable: str, module_path: str, class_name: str, base: type[T]) -> type[T]:
    """Import ``class_name`` from ``module_path`` and check it subclasses ``base``."""
    try:
        module = importlib.import_module(module_path)
    except ImportError as error:
        raise MissingSettingError(
            f"{variable}: cannot import module {module_path!r}: {error}"
        ) from error
    cls = getattr(module, class_name, None)
    if cls is None or not isinstance(cls, type) or not issubclass(cls, base):
        raise MissingSettingError(f"{variable}: {module_path}.{class_name} is not a {base.__name__}")
    return cls
