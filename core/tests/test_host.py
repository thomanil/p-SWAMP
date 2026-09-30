# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""Where a module runs: a ``ModuleHost`` runs one instance per pipeline key,
off the transport, and a worker hosts the families it is told to."""

from __future__ import annotations

import asyncio
import contextlib

import pytest
from support import Halver, HalveCommand, Measurement, Number, NumberResult, at, measurement, take

from pswamp_core.datagateway import DataGateway
from pswamp_core.datagateway.clients import InMemoryClient
from pswamp_core.host import ModuleHost
from pswamp_core.messages import Command, ErrorEvent, PipelineClosed, PlayerCommand
from pswamp_core.modules import Module
from pswamp_core.subscription import Overflow
from pswamp_core.transport import InMemoryTransport
from pswamp_core.util.time import utcnow
from pswamp_core.worker import load_families, main


class Doubler(Module):
    name = "doubler"
    input_model = Measurement
    output_model = NumberResult

    async def process(self, message: Measurement) -> Number | None:
        return Number(value=message.value * 2)


class Counter(Module):
    """Reads the gateway it is given: answers a HalveCommand with how many
    measurements its gateway holds."""

    name = "counter"
    input_model = None
    output_model = NumberResult
    commands = (HalveCommand,)

    async def setup(self, gateway, out) -> None:
        self.gateway = gateway

    async def handle(self, command: HalveCommand) -> Number:
        return Number(value=len([m async for m in self.gateway.consume(Measurement)]))


@contextlib.asynccontextmanager
async def hosting(module, transport, **kwargs):
    host = ModuleHost(module, transport, app="t", **kwargs)
    task = asyncio.create_task(host.serve())
    await asyncio.sleep(0)
    try:
        yield host
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


async def test_one_instance_per_key_and_each_key_hears_only_its_own_results():
    broker = InMemoryTransport()
    async with hosting(Doubler, broker) as host:
        with broker.subscribe(NumberResult, app="t", key="k1") as k1, broker.subscribe(
            NumberResult, app="t", key="k2"
        ) as k2:
            await broker.publish(measurement(1, at(1)), app="t", key="k1")
            await broker.publish(measurement(10, at(2)), app="t", key="k2")
            ((_, one),) = await take(k1, 1)
            ((_, ten),) = await take(k2, 1)
        assert sorted(host.keys()) == ["k1", "k2"]
    assert one.result.value == 2 and ten.result.value == 20
    assert host.keys() == []  # all dropped on shutdown


async def test_a_pipeline_closed_drops_the_key_and_silence_does_too():
    broker = InMemoryTransport()
    async with hosting(Doubler, broker, idle_seconds=0.1) as host:
        await broker.publish(measurement(1, at(1)), app="t", key="closed")
        await broker.publish(measurement(1, at(1)), app="t", key="quiet")
        await asyncio.sleep(0.01)
        assert sorted(host.keys()) == ["closed", "quiet"]
        await broker.publish(PipelineClosed(timestamp=utcnow(), reason="idle"), app="t", key="closed")
        await asyncio.sleep(0.01)
        assert host.keys() == ["quiet"]
        await asyncio.sleep(0.3)
        assert host.keys() == []


async def test_commands_cross_and_a_refusal_comes_back_with_its_request_id():
    broker = InMemoryTransport()
    async with hosting(Halver, broker):
        with broker.subscribe(NumberResult, ErrorEvent, app="t", key="k", overflow=Overflow.GROW) as back:
            ok, refused = HalveCommand(value=8), HalveCommand(value=-1)
            await broker.publish(ok, app="t", key="k")
            await broker.publish(refused, app="t", key="k")
            (_, answer), (_, error) = await take(back, 2)
    assert isinstance(answer, NumberResult) and answer.result.value == 4
    assert answer.request_id == ok.request_id
    assert isinstance(error, ErrorEvent) and error.request_id == refused.request_id
    assert error.source == "halver" and error.detail == "no negatives"


async def test_a_module_failure_is_published_on_the_error_topic_of_its_key():
    class Failing(Doubler):
        name = "failing"

        async def process(self, message):
            raise ValueError("boom")

    broker = InMemoryTransport()
    async with hosting(Failing, broker):
        with broker.subscribe(ErrorEvent, app="t", key="k") as errors:
            await broker.publish(measurement(1, at(1)), app="t", key="k")
            ((_, error),) = await take(errors, 1)
    assert error.source == "failing" and error.detail == "ValueError: boom"


async def test_a_hosted_module_reads_a_gateway_of_its_own_built_by_the_factory():
    broker = InMemoryTransport()
    built = []

    def gateway() -> DataGateway:
        built.append(1)
        return DataGateway([InMemoryClient("store", Measurement, [measurement(i, at(i)) for i in range(3)])])

    async with hosting(Counter, broker, gateway=gateway):
        with broker.subscribe(NumberResult, app="t", overflow=Overflow.GROW) as answers:
            await broker.publish(HalveCommand(value=1), app="t", key="a")
            await broker.publish(HalveCommand(value=1), app="t", key="b")
            got = await take(answers, 2)
    assert [a.result.value for _, a in got] == [3, 3]
    assert len(built) == 2  # one per key


async def winding_down() -> None:
    """Work that takes a moment to stop when cancelled, as real cleanup does."""
    try:
        await asyncio.sleep(10)
    except asyncio.CancelledError:
        await asyncio.sleep(0.1)
        raise


class SlowToStopHandler(Module):
    name = "slow-handler"
    input_model = None
    output_model = NumberResult
    commands = (HalveCommand,)

    async def handle(self, command: HalveCommand) -> None:
        await winding_down()


class SlowToStopProcess(Doubler):
    name = "slow-process"

    async def process(self, message: Measurement) -> None:
        await winding_down()


@pytest.mark.parametrize(
    ("module", "message"),
    [
        (SlowToStopHandler, lambda: HalveCommand(value=8)),  # waiting on its command inbox
        (SlowToStopProcess, lambda: measurement(1, at(1))),  # waiting on its run task
    ],
)
async def test_a_host_cancelled_while_it_drops_a_key_still_stops(module, message):
    """A pipeline closing just before shutdown -- what an app's lifespan does --
    has the host dropping a key when the cancel arrives. The cancel must stop
    it; swallowed, the host waits for ever."""
    broker = InMemoryTransport()
    host = ModuleHost(module, broker, app="t")
    task = asyncio.create_task(host.serve())
    await asyncio.sleep(0)
    await broker.publish(message(), app="t", key="k")
    await asyncio.sleep(0.02)  # the instance is busy with it
    await broker.publish(PipelineClosed(timestamp=utcnow()), app="t", key="k")
    await asyncio.sleep(0.02)  # the host is dropping "k", waiting for the work to stop
    task.cancel()
    done, _ = await asyncio.wait([task], timeout=2)
    assert done, "the host swallowed its cancellation and never stopped"
    assert host.keys() == []


def test_a_module_declaring_a_base_command_class_cannot_be_hosted():
    class Greedy(Module):
        name = "greedy"
        input_model = None
        output_model = NumberResult
        commands: tuple[type[Command], ...] = (PlayerCommand,)

    with pytest.raises(ValueError, match="has subclasses"):
        ModuleHost(Greedy, InMemoryTransport(), app="t")


def test_a_worker_needs_a_broker_and_a_family(monkeypatch, capsys):
    monkeypatch.delenv("PSWAMP_TRANSPORT", raising=False)
    assert main() == 2
    assert "PSWAMP_TRANSPORT is unset" in capsys.readouterr().err
    monkeypatch.setenv("PSWAMP_TRANSPORT", "kafka:pswamp_core.transport.kafka:KafkaTransport")
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:1")
    monkeypatch.delenv("PSWAMP_WORKER_FAMILIES", raising=False)
    assert main() == 2
    assert "names no pipeline family" in capsys.readouterr().err


def test_families_are_named_by_module_path(monkeypatch):
    import sys
    import types

    from pswamp_core.pipeline import PipelineFamily

    module = types.ModuleType("fake_app")
    module.FAMILY = PipelineFamily("fake", DataGateway, (Doubler,))
    module.NOT_ONE = 42
    monkeypatch.setitem(sys.modules, "fake_app", module)
    assert load_families("fake_app:FAMILY, ") == [module.FAMILY]
    with pytest.raises(ValueError, match="is not a PipelineFamily"):
        load_families("fake_app:NOT_ONE")
    with pytest.raises(ValueError, match="not module.path:NAME"):
        load_families("fake_app")
