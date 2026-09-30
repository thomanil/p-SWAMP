# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""``cancel_and_wait`` and ``finish``: stop tasks and finish teardowns without
swallowing a cancellation of the caller."""

from __future__ import annotations

import asyncio

import pytest

from pswamp_core.util.tasks import cancel_and_wait, finish


async def slow_to_stop(stopped: list[str]) -> None:
    try:
        await asyncio.sleep(10)
    except asyncio.CancelledError:
        await asyncio.sleep(0.05)
        stopped.append("stopped")
        raise


async def fails_while_stopping() -> None:
    try:
        await asyncio.sleep(10)
    except asyncio.CancelledError:
        raise ValueError("while stopping") from None


async def test_it_stops_every_task_then_raises_a_failure_it_was_not_told_to_ignore():
    tasks = [asyncio.create_task(asyncio.sleep(10)), asyncio.create_task(fails_while_stopping())]
    await asyncio.sleep(0)
    with pytest.raises(ValueError, match="while stopping"):
        await cancel_and_wait(*tasks)
    assert all(task.done() for task in tasks)


async def test_a_failure_it_was_told_to_ignore_is_not_raised():
    task = asyncio.create_task(fails_while_stopping())
    await asyncio.sleep(0)
    await cancel_and_wait(task, ignore=(ValueError,))
    assert task.done()


async def test_a_cancellation_of_the_caller_is_passed_on_once_the_tasks_have_stopped():
    stopped: list[str] = []
    child = asyncio.create_task(slow_to_stop(stopped))
    await asyncio.sleep(0)
    caller = asyncio.create_task(cancel_and_wait(child))
    await asyncio.sleep(0.01)  # the caller is waiting for the child to stop
    caller.cancel()
    done, _ = await asyncio.wait([caller], timeout=1)
    assert done and caller.cancelled()  # passed on, not swallowed
    assert stopped == ["stopped"]  # and only after the child had stopped


async def test_finish_runs_a_teardown_to_its_end_then_passes_the_cancellation_on():
    steps: list[str] = []

    async def teardown() -> None:
        await asyncio.sleep(0.05)
        steps.append("first")
        await asyncio.sleep(0.05)
        steps.append("second")

    caller = asyncio.create_task(finish(teardown()))
    await asyncio.sleep(0.01)
    caller.cancel()
    done, _ = await asyncio.wait([caller], timeout=1)
    assert done and caller.cancelled()
    assert steps == ["first", "second"]  # nothing left half done


async def test_finish_returns_what_it_awaited():
    async def answer() -> int:
        return 42

    assert await finish(answer()) == 42
