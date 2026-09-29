# SPDX-FileCopyrightText: 2026 Louis Pauchet <louis.pauchet@sintef.no>
# SPDX-License-Identifier: Apache-2.0

"""The base of every message: ``DataModel``.

The wire format is validated, versioned and language-neutral: pydantic's own
JSON is the whole codec (``model_dump_json`` out, ``model_validate_json`` in),
the same serialiser the browser edge uses. No pickle, no numpy on the wire.

What a ``DataModel`` carries:

* ``version`` -- the *schema* version. A subclass pins it with a literal
  (``version: Literal["v1"] = "v1"``), so a ``"v2"`` payload fails validation
  instead of being half-read.
* ``mRID`` -- the identity of what the message is about (a station, a stream),
  in CIM's vocabulary. Optional: a command has none.
* ``timestamp`` -- when the message refers to, always UTC-aware (naive input
  is taken as UTC). Measurement models make it required.
* ``topic`` -- derived from the class name, never configured: ``PmuFrame`` is
  ``pmu.frame``. So the set of ``DataModel`` subclasses *is* the topic
  catalogue. A class may override it with ``topic: ClassVar[str] = "..."``.
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import ClassVar

from pydantic import BaseModel, field_validator

from ..util.time import ensure_utc

__all__ = ["DataModel", "TopicDescriptor", "topic_from_name"]

# Words of a CamelCase name: runs of capitals that end a word (an acronym),
# capitalised words, and digit runs -- each becomes one dotted segment.
_CAMEL_WORDS = re.compile(r"[A-Z]+(?=[A-Z][a-z]|\d|$)|[A-Z]?[a-z]+|\d+")


def topic_from_name(name: str) -> str:
    """The topic a class called ``name`` publishes on: ``PmuFrame`` → ``pmu.frame``."""
    words = _CAMEL_WORDS.findall(name)
    if not words:
        return name.lower()
    return ".".join(word.lower() for word in words)


class TopicDescriptor:
    """Resolves ``Model.topic`` (on the class and on an instance) from the class name."""

    def __get__(self, instance: object, owner: type[DataModel]) -> str:
        return owner.topic_name()


class DataModel(BaseModel):
    """Base class for every message. See the module docstring for the fields."""

    version: str
    mRID: str | None = None
    timestamp: datetime | None = None

    #: The topic this model is published on. Derived; override with a ``ClassVar[str]``.
    topic: ClassVar[TopicDescriptor] = TopicDescriptor()

    @field_validator("timestamp")
    @classmethod
    def _normalise_timestamp(cls, value: datetime | None) -> datetime | None:
        """Coerce naive datetimes to UTC so time ranges compare consistently."""
        if value is None:
            return None
        return ensure_utc(value)

    @classmethod
    def topic_name(cls) -> str:
        """The topic for this class: an explicit ``ClassVar`` override, else derived."""
        override = cls.__dict__.get("topic")
        if isinstance(override, str):
            return override
        for base in cls.__mro__[1:]:
            candidate = base.__dict__.get("topic")
            if isinstance(candidate, str):
                return candidate
            if isinstance(candidate, TopicDescriptor):
                break
        return topic_from_name(cls.__name__)
