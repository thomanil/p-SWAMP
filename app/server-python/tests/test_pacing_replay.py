# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""Pacer and Replay: wall-clock pacing, the controls, seek as a new consume."""

from __future__ import annotations

import asyncio
import time

from conftest import Measurement, at, measurement

from pswamp.data import Capability, DataGateway, InMemoryClient, Pacer, Replay

HISTORY = Capability.HISTORY_CONSUME


def _history(n: int, spacing: float = 0.01) -> InMemoryClient:
    """``n`` measurements ``spacing`` seconds apart, from the fixed anchor."""
    return InMemoryClient(
        "file",
        [Measurement],
        [measurement(index, at(index * spacing)) for index in range(n)],
        capabilities=HISTORY,
    )


async def _take(replay: Replay, n: int) -> list[str]:
    got = []
    async for payload in replay:
        got.append(payload.mRID)
        if len(got) == n:
            break
    return got


async def test_pacer_spaces_payloads_by_timestamp_over_speed():
    gateway = DataGateway([_history(6, spacing=0.02)])
    pacer = Pacer(gateway.consume(Measurement), speed=2.0)

    started = time.monotonic()
    got = [payload.mRID async for payload in pacer]
    elapsed = time.monotonic() - started

    assert got == [f"m{i}" for i in range(6)]
    # 5 gaps of 20 ms at ×2 is 50 ms of wall time; allow generous slack, but it
    # must not have been instantaneous.
    assert 0.04 <= elapsed < 0.5


async def test_paused_pacer_emits_only_on_step():
    gateway = DataGateway([_history(4)])
    pacer = Pacer(gateway.consume(Measurement), paused=True)

    async def first():
        return (await pacer.__anext__()).mRID

    task = asyncio.create_task(first())
    await asyncio.sleep(0.05)
    assert not task.done()

    pacer.step()
    assert await asyncio.wait_for(task, 1) == "m0"

    pacer.step(2)
    assert (await pacer.__anext__()).mRID == "m1"
    assert (await pacer.__anext__()).mRID == "m2"


async def test_replay_seek_lands_at_the_target_and_reanchors():
    gateway = DataGateway([_history(10, spacing=0.01)])
    replay = Replay(gateway, Measurement, speed=100.0)

    assert await _take(replay, 2) == ["m0", "m1"]

    replay.seek(at(0.07))
    assert await _take(replay, 2) == ["m7", "m8"]
    assert replay.position == at(0.08)

    await replay.aclose()


async def test_replay_seek_while_paused_shows_the_landing_sample():
    gateway = DataGateway([_history(10)])
    replay = Replay(gateway, Measurement, paused=True)

    replay.step()
    assert await _take(replay, 1) == ["m0"]

    replay.seek(at(0.05))
    # One payload arrives without a step: the sample at the new position.
    assert await asyncio.wait_for(_take(replay, 1), 1) == ["m5"]
    assert replay.paused

    # Stepping back is a seek to the previous instant.
    replay.seek(at(0.04))
    assert await asyncio.wait_for(_take(replay, 1), 1) == ["m4"]

    await replay.aclose()


async def test_replay_loops_to_the_beginning_when_asked():
    gateway = DataGateway([_history(3)])
    replay = Replay(gateway, Measurement, speed=1000.0, loop=True)

    assert await _take(replay, 5) == ["m0", "m1", "m2", "m0", "m1"]
    assert replay.passes == 1

    await replay.aclose()


async def test_replay_over_an_empty_source_ends_rather_than_spinning():
    empty = InMemoryClient("file", [Measurement], [], capabilities=HISTORY)
    replay = Replay(DataGateway([empty]), Measurement, loop=True)

    assert await _take(replay, 5) == []


async def test_replay_play_pause_and_speed_forward_to_the_pacer():
    gateway = DataGateway([_history(20, spacing=0.01)])
    replay = Replay(gateway, Measurement, speed=1000.0)

    assert await _take(replay, 1) == ["m0"]

    replay.pause()
    task = asyncio.create_task(_take(replay, 1))
    await asyncio.sleep(0.05)
    assert not task.done()

    replay.set_speed(500.0)
    replay.play()
    assert await asyncio.wait_for(task, 1) == ["m1"]
    assert replay.speed == 500.0
    assert replay.playing

    await replay.aclose()
