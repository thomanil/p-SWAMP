# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""Stopping tasks without swallowing a cancellation of the caller.

The usual idiom -- ``task.cancel()`` then ``with suppress(CancelledError):
await task`` -- has a trap. A cancellation of the *caller* that arrives while
it waits raises the same ``CancelledError`` at the same ``await``, and the
``suppress`` swallows it: the caller carries on as if it had never been
cancelled. A host's feed loop did exactly that when shut down while dropping a
key, and waited for ever.

- ``cancel_and_wait(*tasks)`` stops tasks: it waits until they have finished,
  then passes the caller's cancellation on, if there was one.
- ``finish(awaitable)`` runs a teardown to its end even if the caller is
  cancelled meanwhile, then passes the cancellation on, so a cancellation
  arriving half way through never leaves a subscription or a gateway open.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable
from typing import TypeVar

__all__ = ["cancel_and_wait", "finish"]

T = TypeVar("T")


async def finish(awaitable: Awaitable[T]) -> T:
    """Run ``awaitable`` to its end, then return its result.

    A cancellation of the caller meanwhile is held until it has ended, then
    re-raised (and wins over whatever the awaitable ended with).
    """
    task = asyncio.ensure_future(awaitable)
    interrupted = False
    while not task.done():
        try:
            await asyncio.wait([task])
        except asyncio.CancelledError:
            interrupted = True
    if interrupted:
        if not task.cancelled():
            task.exception()  # retrieved: the cancellation wins
        raise asyncio.CancelledError
    return task.result()


async def cancel_and_wait(
    *tasks: asyncio.Task, ignore: tuple[type[BaseException], ...] = ()
) -> None:
    """Cancel ``tasks`` and wait until every one has finished.

    A cancellation of the caller meanwhile is held until they have, then
    re-raised, so nothing is left running and nothing is swallowed. A task
    that ends with an exception other than its cancellation re-raises it here
    once all have finished -- unless it is one of ``ignore``, the exceptions a
    task may expectedly end with while being stopped.
    """
    for task in tasks:
        task.cancel()
    try:
        if tasks:
            await finish(asyncio.wait(tasks))
    finally:
        errors = [task.exception() for task in tasks if task.done() and not task.cancelled()]
    for error in errors:
        if error is not None and not isinstance(error, ignore):
            raise error
