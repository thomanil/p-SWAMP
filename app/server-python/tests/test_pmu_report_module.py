# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""The mean-voltage module, alone and as a batch job over the gateway."""

from __future__ import annotations

import asyncio
from datetime import timedelta

from pmu_data.sample_file import RECORDING_EPOCH, STREAM_ID, SampleFileClient
from pmu_report.model import VoltageReport, mean_voltage
from pswamp.data import (
    BUS_CAPABILITIES,
    DataGateway,
    InMemoryClient,
    Report,
    Sample,
    run_batch_job,
    utcnow,
)


def test_mean_voltage_selects_channels_by_measurement_key():
    client = SampleFileClient("source")

    report = mean_voltage(client.header, client.samples)

    assert report.n_samples == 60
    assert [s.station for s in report.stations] == ["3000", "3245", "5100", "6500", "7000"]
    # Volts, not kV: 420 kV at three stations and 300 kV at two, so ~372 kV overall.
    assert 360_000 < report.mean_voltage < 380_000
    assert all(290_000 < s.mean_voltage < 430_000 for s in report.stations)
    assert report.parameters == {"measurement": "v_Magnitude", "channels": 5}
    assert report.topic == "voltage.live.report"


def test_mean_voltage_over_nothing_is_null_not_an_error():
    client = SampleFileClient("source")

    report = mean_voltage(client.header, [])

    assert report.n_samples == 0
    assert report.mean_voltage is None
    assert all(s.mean_voltage is None for s in report.stations)


async def test_as_a_job_the_report_reaches_a_bus_subscriber_with_its_request_id():
    source = SampleFileClient("source")
    bus = InMemoryClient("bus", [Report], capabilities=BUS_CAPABILITIES)
    gateway = DataGateway([source, bus])

    async def wait_for_report():
        stream = gateway.consume(VoltageReport, start=utcnow())
        async for payload in stream:
            await stream.aclose()
            return payload

    waiting = asyncio.create_task(wait_for_report())
    for _ in range(100):
        if bus.subscriber_count:
            break
        await asyncio.sleep(0.01)

    origin = RECORDING_EPOCH
    report = await run_batch_job(
        gateway,
        Sample,
        lambda samples: mean_voltage(source.header, samples),
        start=origin + timedelta(seconds=1),
        end=origin + timedelta(seconds=2),
        mRID=STREAM_ID,
        request_id="click-7",
    )
    received = await asyncio.wait_for(waiting, 2)

    assert received is report
    assert report.request_id == "click-7"
    assert report.n_samples == 20
    assert report.range_start == origin + timedelta(seconds=1)
