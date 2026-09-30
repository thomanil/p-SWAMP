# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""A pipeline end to end over the in-memory transport, the single-process
deployment: data down to a hosted module and back as a result, commands up to
the player or a module, and the local view an endpoint reads."""

from __future__ import annotations

import asyncio
import contextlib
from datetime import timedelta

import pytest
from support import Halver, HalveCommand, Measurement, Number, NumberResult, measurements

from pswamp_core.command_routing import CommandRefused, NoReceiver
from pswamp_core.datagateway import Capability, Coverage, DataGateway, TimeRange
from pswamp_core.datagateway.clients import InMemoryClient
from pswamp_core.host import hosts_for, serve_hosts
from pswamp_core.messages import (
    Command,
    ErrorEvent,
    PlayerStatus,
    SeekCommand,
    SwitchSourceCommand,
    sent_at,
)
from pswamp_core.modules import Module
from pswamp_core.pipeline import Pipeline, PipelineFamily
from pswamp_core.subscription import Overflow
from pswamp_core.transport import InMemoryTransport
from pswamp_core.util.time import utcnow


class Doubler(Module):
    name = "doubler"
    input_model = Measurement
    output_model = NumberResult

    async def process(self, message: Measurement) -> Number:
        return Number(value=message.value * 2)


class GoLiveAtThree(Module):
    """A module that commands the player: at the third measurement it asks for
    the live source, the way the edge would."""

    name = "go-live-at-three"
    input_model = Measurement
    output_model = NumberResult

    async def setup(self, gateway, out) -> None:
        self.out = out

    async def process(self, message: Measurement) -> None:
        if message.value == 3:
            self.out.publish(SwitchSourceCommand(source="live"))


def history() -> InMemoryClient:
    return InMemoryClient("history", Measurement, measurements(10), capabilities=Capability.HISTORY_CONSUME)


def live() -> InMemoryClient:
    return InMemoryClient(
        "live",
        Measurement,
        capabilities=Capability.LIVE_CONSUME,
        coverage_fn=lambda: Coverage(TimeRange(utcnow() - timedelta(seconds=1), None), live=True),
    )


@contextlib.asynccontextmanager
async def running(family: PipelineFamily, broker: InMemoryTransport, key: str = "k", **player):
    """The family's modules hosted, and one pipeline over it, started."""
    hosts = hosts_for(family, broker)
    task = asyncio.create_task(serve_hosts(hosts))
    await asyncio.sleep(0)
    pipeline = Pipeline(key, family, broker, model=Measurement, paced=False, **player)
    await pipeline.start()
    try:
        yield pipeline, hosts
    finally:
        await pipeline.stop()
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


async def wait_until(predicate, pipeline: Pipeline, timeout: float = 2.0) -> None:
    with pipeline.changes() as changes:
        async with asyncio.timeout(timeout):
            while not predicate():
                await changes.wait()


async def test_a_module_runs_over_the_in_memory_transport_with_json_and_exact_topics():
    broker = InMemoryTransport()
    family = PipelineFamily("a", lambda: DataGateway([history()]), (Doubler,))
    with broker.subscribe(NumberResult, app="b", key="k") as other_app:
        async with running(family, broker, autoplay=True) as (pipeline, _):
            await wait_until(lambda: pipeline.latest.get(NumberResult) is not None
                             and pipeline.latest.get(NumberResult).result.value == 18, pipeline)
            result = pipeline.latest.get(NumberResult)
    assert sent_at(result) is not None  # it crossed the transport
    assert pipeline.latest.get(Measurement) is not None  # the player's own, remembered here
    assert other_app.get_nowait() is None  # another app, same key: nothing


async def test_dispatch_checks_player_commands_here_and_sends_module_commands_on():
    broker = InMemoryTransport()
    family = PipelineFamily("a", lambda: DataGateway([history()]), (Halver,))
    with broker.subscribe(ErrorEvent, app="a", key="k", overflow=Overflow.GROW) as errors:
        async with running(family, broker) as (pipeline, _):
            with pytest.raises(CommandRefused, match="outside the history"):
                pipeline.dispatch(SeekCommand(offset_s=60))
            with pytest.raises(CommandRefused, match="no source named 'nope'"):
                pipeline.dispatch(SwitchSourceCommand(source="nope"))
            with pytest.raises(NoReceiver):
                pipeline.dispatch(Command())
            refused, answered = HalveCommand(value=-1), HalveCommand(value=8)
            pipeline.dispatch(refused)  # accepted here: the module checks it where it runs
            pipeline.dispatch(answered)
            await wait_until(lambda: pipeline.latest.get(NumberResult) is not None, pipeline)
            pipeline.dispatch(SeekCommand(offset_s=3))
            await wait_until(lambda: pipeline.player.cursor is not None, pipeline)
            (_, error) = await asyncio.wait_for(errors.get(), 2)
    assert pipeline.latest.get(NumberResult).request_id == answered.request_id
    assert (error.source, error.request_id, error.detail) == ("halver", refused.request_id, "no negatives")


async def test_a_module_switches_the_source_by_command():
    broker = InMemoryTransport()
    family = PipelineFamily("a", lambda: DataGateway([history(), live()]), (GoLiveAtThree,))
    async with running(family, broker, autoplay=True) as (pipeline, _):
        await wait_until(lambda: pipeline.player.mode == "live", pipeline)
        status = pipeline.latest.get(PlayerStatus)
    assert status is not None and status.source == "live"


async def test_stop_says_pipeline_closed_and_the_host_drops_the_key():
    broker = InMemoryTransport()
    family = PipelineFamily("a", lambda: DataGateway([history()]), (Doubler,))
    async with running(family, broker, autoplay=True) as (pipeline, hosts):
        await wait_until(lambda: pipeline.latest.get(NumberResult) is not None, pipeline)
        assert hosts[0].keys() == ["k"]
        await pipeline.stop()
        await asyncio.sleep(0.01)
        assert hosts[0].keys() == []


async def test_changes_coalesce_and_latest_is_current_when_a_reader_wakes():
    broker = InMemoryTransport()
    family = PipelineFamily("a", lambda: DataGateway([history()]), ())
    pipeline = Pipeline("k", family, broker, model=Measurement, paced=False)
    with pipeline.changes() as changes:
        for m in measurements(5):
            pipeline.publish(m)
        assert await changes.wait(0.1) is True
        assert pipeline.latest.get(Measurement).mRID == "m4"
        assert await changes.wait(0.05) is False  # five changes, one wake-up
