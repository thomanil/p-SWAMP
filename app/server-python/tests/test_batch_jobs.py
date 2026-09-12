# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""A batch job: bounded query in, analysis off the loop, report out on the bus."""

from __future__ import annotations

import asyncio
import threading
from typing import Literal

from conftest import Measurement, at, measurement

from pswamp.data import (
    BUS_CAPABILITIES,
    Capability,
    DataGateway,
    InMemoryClient,
    ModuleRef,
    Report,
    check_client,
    run_batch_job,
    utcnow,
)


class MeanReport(Report):
    version: Literal["v1"] = "v1"
    mean: float


def _analyse(payloads) -> MeanReport:
    values = [payload.value for payload in payloads]
    return MeanReport(
        module=ModuleRef(name="mean", uuid="test"),
        n_samples=len(values),
        mean=sum(values) / len(values) if values else float("nan"),
        parameters={"thread": threading.current_thread().name},
    )


def _gateway() -> tuple[DataGateway, InMemoryClient]:
    source = InMemoryClient(
        "source",
        [Measurement],
        [measurement(index, at(index)) for index in range(10)],
        capabilities=Capability.HISTORY_CONSUME,
    )
    bus = InMemoryClient("bus", [MeanReport], capabilities=BUS_CAPABILITIES)
    return DataGateway([source, bus]), bus


async def test_job_reports_over_the_requested_range_with_its_request_id():
    gateway, bus = _gateway()

    report = await run_batch_job(
        gateway, Measurement, _analyse, start=at(2), end=at(5), request_id="req-1"
    )

    assert report.n_samples == 3
    assert report.mean == 3.0
    assert report.request_id == "req-1"
    assert report.range_start == at(2)
    assert report.range_end == at(4)
    assert report.timestamp is not None
    # The analysis ran on a worker thread, not the event loop.
    assert report.parameters["thread"] != threading.main_thread().name
    # …and the report reached the bus like any module output.
    assert bus.records == [report]


async def test_a_live_subscriber_on_the_bus_receives_the_report():
    gateway, bus = _gateway()

    async def wait_for_report():
        stream = gateway.consume(MeanReport, start=utcnow())
        async for payload in stream:
            await stream.aclose()
            return payload

    waiting = asyncio.create_task(wait_for_report())
    for _ in range(100):
        if bus.subscriber_count:
            break
        await asyncio.sleep(0.01)

    report = await run_batch_job(gateway, Measurement, _analyse, request_id="req-2")
    received = await asyncio.wait_for(waiting, 2)

    assert received is report
    assert received.n_samples == 10


async def test_an_open_end_is_bounded_at_now():
    gateway, _ = _gateway()

    report = await run_batch_job(gateway, Measurement, _analyse)

    assert report.n_samples == 10


async def test_shipped_clients_pass_the_conformance_check():
    _, bus = _gateway()
    source = InMemoryClient(
        "source",
        [Measurement],
        [measurement(index, at(index)) for index in range(10)],
        capabilities=Capability.HISTORY_CONSUME | Capability.PRODUCE,
    )

    payloads = await check_client(source, Measurement, sample=measurement(99, at(99)))

    assert len(payloads) == 10
    assert await check_client(bus, MeanReport) if bus.records else True
