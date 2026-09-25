# SPDX-FileCopyrightText: 2026 Louis Pauchet <louis.pauchet@sintef.no>
# SPDX-License-Identifier: Apache-2.0

"""The base of every message: ``DataModel``.

Lifted from the test_pswamp draft (``core/models/data_model.py``), which made the
case that the wire format has to be *validated, versioned and language-neutral*:
with pickle, "the receiver doesn't completely know what will be received, and it
presents a security risk". pydantic's own JSON is the whole codec --
``model_dump_json`` out, ``model_validate_json`` in -- and it is the same
serialiser the browser edge already uses, so one codec spans core and edge.

What a ``DataModel`` carries, and why:

* ``version`` -- the *schema* version, required. A subclass pins it with a
  literal (``version: Literal["v1"] = "v1"``), so a ``"v2"`` payload fails
  validation against a ``v1`` model instead of being half-read.
* ``mRID`` -- the identity of what the message is about (a station, a stream, a
  module), in CIM's vocabulary. Optional: a command has none.
* ``timestamp`` -- when the message refers to, always UTC-aware (naive input is
  coerced). Optional on the base so that a producer of an *event* may leave it
  for the gateway to stamp; measurement models make it required, because a
  measurement's time is the PMU's time and never "now".
* ``topic`` -- derived from the class name, never configured: ``PmuFrame`` is
  ``pmu.frame``. So the set of ``DataModel`` subclasses *is* the topic catalogue,
  with the schema attached. A class may still override it with
  ``topic: ClassVar[str] = "..."``.

And one thing it carries that is *not* a field: when a transport delivered it,
the wall-clock time the record was sent (:func:`sent_at`). That is transport
metadata -- it never serialises, never appears in the contract, and is ``None``
on anything that did not cross a transport -- and it is what lets a module in
another process tell how far behind its input topic it is running.

Adapted from the draft: the per-message ``branch`` field and its splice into the
topic name are dropped (STEP 3 §4.1: environments are deployed separately, and
*whose* stream a topic belongs to is a configured namespace, not something an
instance carries); the CamelCase splitter keeps acronyms and digits together
(``PmuFrame`` → ``pmu.frame``, ``N44Frame`` → ``n.44.frame``, not ``p.m.u``).
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import ClassVar

from pydantic import BaseModel, PrivateAttr, field_validator

from ..util.time import ensure_utc

__all__ = ["DataModel", "TopicDescriptor", "sent_at", "stamp_sent_at", "topic_from_name"]

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
    """Resolves ``Model.topic`` (and ``instance.topic``) from the class name.

    A plain descriptor rather than a property, so that it works on the class as
    well as on an instance; and a ``ClassVar`` on the model, so that pydantic
    leaves it alone.
    """

    def __get__(self, instance: object, owner: type[DataModel]) -> str:
        return owner.topic_name()


class DataModel(BaseModel):
    """Base class for every message. See the module docstring for the fields."""

    version: str
    mRID: str | None = None
    timestamp: datetime | None = None

    #: The topic this model is published on. Derived; override with a ``ClassVar[str]``.
    topic: ClassVar[TopicDescriptor] = TopicDescriptor()

    #: When a transport's producer sent this message (epoch seconds), set on
    #: receipt; ``None`` for a message that never crossed a transport.
    _sent_at: float | None = PrivateAttr(default=None)

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


def sent_at(message: DataModel) -> float | None:
    """When ``message`` was sent over a transport (epoch seconds), or ``None``.

    Set by the transport that delivered it -- the broker record's own timestamp
    for Kafka, the publish call for the in-memory transport -- so ``time.time()
    - sent_at(message)`` is how long the message has been in flight and queued
    by the time its consumer reads it. Not a field: it never serialises.
    """
    return message._sent_at


def stamp_sent_at(message: DataModel, when: float) -> None:
    """Record when ``message`` was sent; for transports, on receipt."""
    message._sent_at = when
