# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.
# Lifted from Louis Pauchet's data-gateway draft (SINTEF, 2026), see STEP3 §13;
# ``branch`` renamed to ``namespace`` (STEP3 §15.4) and ``catalogue()`` added.

"""The envelope every message in the system wears — contract 1 of STEP3.

Every payload that travels on a topic — a measurement sample, a stream header, a
module's result, a batch report — is a subclass of :class:`DataModel`. That buys
three things the old pickled dicts never had:

- **a schema** — pydantic validates on read and ``model_dump_json`` is the one
  wire encoding, so a broker topic can be read by anything that speaks JSON;
- **a version** — subclasses pin ``version: Literal["v1"] = "v1"``, so a payload
  written against a later schema fails validation loudly instead of quietly
  misreading;
- **a topic** — derived from the class name and the namespace, so the topic
  catalogue is simply the set of :class:`DataModel` subclasses the process
  imports (:func:`catalogue`), not a config table kept in step by hand.

Topics follow ``<Word1>.<namespace>.<Word2>.<Word3>…`` from the CamelCase class
name: ``VoltageReport`` in the default namespace is ``voltage.live.report``; with
``namespace="no"`` it is ``voltage.no.report``. The namespace is the deployment-
level prefix (a TSO, a feature branch) that keeps two deployments sharing one
broker from colliding; the in-process bus ignores it.
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import ClassVar
from uuid import UUID

from pydantic import BaseModel, field_validator

from ..time import ensure_utc

__all__ = ["DEFAULT_NAMESPACE", "DataModel", "MRIDType", "TopicDescriptor", "catalogue"]

#: Namespace used when none is given: the deployment's "real" data.
DEFAULT_NAMESPACE = "live"

#: Type alias used for message resource identifiers.
MRIDType = str | UUID


class TopicDescriptor:
    """Resolves a model's topic from either the class or an instance.

    ``VoltageReport.topic`` is the default-namespace topic; ``report.topic`` on an
    instance honours that instance's ``namespace``.
    """

    def __get__(self, instance, owner):
        if instance is None:
            return owner.topic_for_namespace()

        return owner.topic_for_namespace(instance.namespace)


class DataModel(BaseModel):
    """Base class for every payload that travels on a topic.

    Attributes:
        version: Schema version. Subclasses pin it with a ``Literal``.
        namespace: Deployment-level prefix inserted into the topic name.
        mRID: Optional resource identifier — the stream id on a sample, the
            module uuid on a result.
        timestamp: Instant the payload refers to, always stored UTC-aware.
            Producers may leave it ``None``; the gateway stamps it at produce
            time, and everything read back through the gateway carries one,
            since time-range routing depends on it.
    """

    version: str
    namespace: str = DEFAULT_NAMESPACE
    mRID: str | None = None
    timestamp: datetime | None = None

    topic: ClassVar[TopicDescriptor] = TopicDescriptor()

    @field_validator("timestamp")
    @classmethod
    def _normalise_timestamp(cls, value: datetime | None) -> datetime | None:
        """Coerce naive datetimes to UTC so ranges compare consistently."""
        if value is None:
            return None

        return ensure_utc(value)

    @classmethod
    def topic_for_namespace(cls, namespace: str = DEFAULT_NAMESPACE) -> str:
        """The topic this model publishes on, for a given namespace."""
        words = [word.lower() for word in re.findall(r"[A-Z][a-z0-9]*", cls.__name__)]
        words.insert(1, namespace)

        return ".".join(words)


def catalogue() -> list[type[DataModel]]:
    """Every concrete message type the process knows, sorted by topic.

    The topic catalogue as data: whatever subclasses of :class:`DataModel` have
    been imported. Abstract bases (a subclass that pins no ``version``) are
    excluded, since nothing is ever published *as* them.
    """
    found: list[type[DataModel]] = []

    def walk(base: type[DataModel]) -> None:
        for sub in base.__subclasses__():
            if _pins_version(sub):
                found.append(sub)
            walk(sub)

    walk(DataModel)
    return sorted(found, key=lambda model: model.topic)


def _pins_version(model: type[DataModel]) -> bool:
    field = model.model_fields.get("version")
    return field is not None and field.default is not None and not field.is_required()
