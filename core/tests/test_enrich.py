# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""The gateway stamps a ``cimReferenceId`` on every frame early in the
pipeline (the stub enricher); every reader of a stream sees the same stamped
frames, and the reference travels with the frame to whatever reads it later."""

from __future__ import annotations

import json

from support import Measurement, at, measurements, take

from pswamp_core.datagateway import CimReferenceEnricher, DataGateway
from pswamp_core.datagateway.clients import InMemoryClient
from pswamp_core.messages import PmuFrame, PmuHeader
from pswamp_core.transport import InMemoryTransport

HEADER = PmuHeader(
    station=["A", "A", "B", "C"],
    channel=["V", "f", "f", "f"],
    measurement=["V_Magnitude", "f", "f", "f"],
    units=["kV", "Hz", "Hz", "Hz"],
    data_rate=50.0,
)


def frames(n: int, header: PmuHeader = HEADER) -> list[PmuFrame]:
    return [
        PmuFrame(timestamp=at(i * 0.02), mRID="s", header=header, values=[400.0, 50.0, 50.01, 49.99])
        for i in range(n)
    ]


class PerLayout(CimReferenceEnricher):
    """What a real one looks like: the reference worked out per layout."""

    def __init__(self) -> None:
        super().__init__(None)
        self.asked = 0

    def reference_for(self, header: PmuHeader) -> str | None:
        self.asked += 1
        return None if "Z" in header.stations else "ref-" + "".join(header.stations)


async def read_all(gateway: DataGateway, model=PmuFrame) -> list:
    async with gateway:
        stream = gateway.consume(model, at(0), None)
        return [payload async for payload in stream]


async def test_the_gateway_stamps_the_reference_on_every_frame_and_keeps_the_layout_hash():
    assert HEADER.cimReferenceId is None  # optional: a provider's frame has none
    gateway = DataGateway([InMemoryClient("mem", PmuFrame, frames(5))], enrichers=[CimReferenceEnricher("ref-1")])
    out = await read_all(gateway)
    assert len(out) == 5 and all(f.header.cimReferenceId == "ref-1" for f in out)
    assert out[0].header.header_id == HEADER.header_id  # the layout is the same layout
    assert all(f.header is out[0].header for f in out)  # one stamped header per layout, shared
    assert out[0].values == [400.0, 50.0, 50.01, 49.99]


async def test_the_reference_is_decided_once_per_layout():
    # Built, not model_copy'd: a copy carries the cached header_id of the original.
    other = PmuHeader(**{**HEADER.model_dump(exclude={"header_id", "cimReferenceId"}), "station": ["B", "B", "C", "A"]})
    unknown = PmuHeader(**{**HEADER.model_dump(exclude={"header_id", "cimReferenceId"}), "station": ["Z", "Z", "Z", "Z"]})
    enricher = PerLayout()
    first = [enricher.enrich(f) for f in frames(3)]
    second = enricher.enrich(frames(1, other)[0])
    skipped = enricher.enrich(frames(1, unknown)[0])
    assert {f.header.cimReferenceId for f in first} == {"ref-ABC"}
    assert second.header.cimReferenceId == "ref-BCA"
    assert skipped.header.cimReferenceId is None  # no reference for this layout: unstamped
    assert enricher.asked == 3  # once per layout, not per frame


async def test_what_is_not_a_frame_or_is_already_stamped_passes_untouched():
    enricher = CimReferenceEnricher("ref-1")
    reading = measurements(1)[0]
    assert enricher.enrich(reading) is reading
    # Already enriched along the way (by another enricher, or read back off a topic).
    already = frames(1, HEADER.model_copy(update={"cimReferenceId": "stamped-before"}))[0]
    assert enricher.enrich(already) is already
    # And through a gateway over a non-frame model, nothing happens at all.
    gateway = DataGateway([InMemoryClient("m", Measurement, measurements(3))], enrichers=[enricher])
    out = await read_all(gateway, Measurement)
    assert [m.value for m in out] == [0.0, 1.0, 2.0]


async def test_without_an_enricher_the_reference_stays_none():
    out = await read_all(DataGateway([InMemoryClient("mem", PmuFrame, frames(3))]))
    assert all(f.header.cimReferenceId is None for f in out)


async def test_the_reference_survives_json_and_a_transport_hop():
    frame = CimReferenceEnricher("ref-1").enrich(frames(1)[0])
    again = PmuFrame.model_validate_json(frame.model_dump_json())
    assert again.header.cimReferenceId == "ref-1"
    assert again.header.header_id == frame.header.header_id
    broker = InMemoryTransport()
    with broker.subscribe(PmuFrame) as feed:
        await broker.publish(frame, "k")
        ((_, received),) = await take(feed, 1)
    assert received.header.cimReferenceId == "ref-1"


def test_a_reader_ignores_fields_it_does_not_know():
    payload = json.loads(CimReferenceEnricher("ref-1").enrich(frames(1)[0]).model_dump_json())
    payload["header"]["something_new"] = {"x": 1}  # a newer writer's extra field
    assert PmuFrame.model_validate(payload).header.cimReferenceId == "ref-1"
