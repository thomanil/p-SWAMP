# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""Enrichment: what the gateway adds to a payload between the provider and the pipeline.

Providers deliver what their store or feed holds. Some things a pipeline
wants are not in any provider. An **enricher** is the gateway's hook for that:
the gateway opens and closes it with its clients, and its ``DataStream`` passes
every payload through it just before handing it on. So every reader of the
gateway sees the same enriched payload, and no provider, module, bus or
transport changes.

``enrich`` is synchronous and must not do I/O: it runs once per frame on the
event loop. Anything slow happens in ``open``.

:class:`CimReferenceEnricher` is the **stub for the CIM reference**: it stamps
``PmuHeader.cimReferenceId``, an id for the grid (CIM) data that applies to a
frame, early in the pipeline, and anything later -- a module in-process or in
a worker, since the header travels with the frame -- reads it off the frame.
The stub answers every layout with one configured placeholder. The real thing
overrides ``reference_for`` (a lookup against a CIM model, loaded in ``open``)
and nothing else changes. More enrichers slot in beside it, in order.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

from ..messages.pmu import PmuFrame

if TYPE_CHECKING:
    from ..messages.data_model import DataModel
    from ..messages.pmu import PmuHeader

__all__ = ["CimReferenceEnricher", "Enricher"]


class Enricher(ABC):
    """Adds to payloads on their way out of the gateway."""

    name: str = "enricher"

    async def open(self) -> None:
        """Load what ``enrich`` needs. Called with the gateway's clients."""

    async def close(self) -> None:
        """Release it."""

    @abstractmethod
    def enrich(self, payload: DataModel) -> DataModel:
        """The payload, enriched -- or unchanged. Synchronous; no I/O."""


class CimReferenceEnricher(Enricher):
    """Stamps ``cimReferenceId`` on each ``PmuFrame``'s header. A stub, for now.

    ``reference_for`` is asked once per layout (``header_id``); the stamped
    header is shared by every frame with that layout, so per frame this is a
    dictionary lookup and a shallow copy. A layout it answers ``None`` for
    passes through unstamped.

    **The stub:** ``reference_for`` returns the ``reference`` it was built
    with, whatever the layout. A CIM-backed enricher subclasses this and
    overrides ``reference_for`` (and ``open``, to load its model).

    Providers never set the reference: a frame arrives with
    ``cimReferenceId=None`` and is stamped here, along the way. One that
    already carries a reference has been enriched before (a second enricher,
    a frame read back off a topic) and is passed on untouched, as is anything
    that is not a ``PmuFrame``.
    """

    def __init__(self, reference: str | None) -> None:
        self.reference = reference
        self.name = f"cim-reference:{reference}"
        self._headers: dict[str, PmuHeader | None] = {}
        self.enriched = 0

    def reference_for(self, header: PmuHeader) -> str | None:
        """The CIM reference for frames with this layout, or ``None``.

        Stub: the configured placeholder, for every layout.
        """
        return self.reference

    def enrich(self, payload: DataModel) -> DataModel:
        if not isinstance(payload, PmuFrame) or payload.header.cimReferenceId is not None:
            return payload
        layout = payload.header.header_id
        if layout not in self._headers:
            reference = self.reference_for(payload.header)
            self._headers[layout] = (
                None if reference is None else payload.header.model_copy(update={"cimReferenceId": reference})
            )
        header = self._headers[layout]
        if header is None:
            return payload
        self.enriched += 1
        return payload.model_copy(update={"header": header})
