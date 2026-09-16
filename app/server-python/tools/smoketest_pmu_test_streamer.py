# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""Drive the PMU test streamer end to end. Driven by scripts/e2e-smoke-test.sh.

    python tools/smoketest_pmu_test_streamer.py <base-url>

The second wire-level smoke test, beside the Reference example's: open the
streamer's socket, press play, and wait for a state message whose ``stats`` is
filled in. That one field is the whole point -- it is the stats *module's*
result, and the module may be running in this process or in the stats-worker
container over the broker (``PMU_TEST_STREAMER_MODULE_TRANSPORT``). The socket
cannot tell which, and that is the property under test: under compose this
proves the worker path end to end; under a bare ``docker run`` (CI) it proves
the in-process path. Then stop, and check the ack.

Same shape and dependencies as smoketest_reference_subapp.py: websockets from
the server's own environment, urllib for the POSTs, every step reported.
"""

import asyncio
import json
import random
import sys
import urllib.error
import urllib.request
from datetime import datetime

from websockets.asyncio.client import connect

WS_PATH = "/api/pmu-test-streamer/ws"
API_PATH = "/api/pmu-test-streamer"

#: The replay runs at 20 Hz, so the first stats arrive within a frame or two of
#: play -- unless the module lives in a worker that has to notice a new client
#: key first. Generous, because a wedged path should fail here rather than be
#: masked by a retry.
STATS_TIMEOUT = 10.0
RECV_TIMEOUT = 5.0
HTTP_TIMEOUT = 5.0

failures: list[str] = []


def ok(label: str) -> None:
    print(f"    \033[32m✓\033[0m {label}")


def bad(label: str) -> None:
    print(f"    \033[31m✗\033[0m {label}")
    failures.append(label)


def check(label: str, condition: bool, detail: str = "") -> bool:
    if condition:
        ok(label)
    else:
        bad(f"{label}{f' -- {detail}' if detail else ''}")
    return condition


def _instant(text: str | None) -> datetime:
    return datetime.fromisoformat(text or "1970-01-01T00:00:00+00:00")


def post(base_url: str, path: str, client_id: str) -> tuple[int, dict]:
    request = urllib.request.Request(f"{base_url}{path}?client_id={client_id}", method="POST", data=b"")
    try:
        with urllib.request.urlopen(request, timeout=HTTP_TIMEOUT) as response:
            return response.status, json.loads(response.read() or b"{}")
    except urllib.error.HTTPError as error:
        return error.code, json.loads(error.read() or b"{}")
    except (urllib.error.URLError, OSError) as error:
        return 0, {"unreachable": str(error)}


def command(base_url: str, client_id: str, action: str) -> None:
    status, body = post(base_url, f"{API_PATH}/playback/{action}", client_id)
    check(
        f"POST {API_PATH}/playback/{action} -> 200 {{status, applied}}",
        status == 200 and body.get("applied") == action and "status" in body,
        f"got {status} {body}",
    )


async def stream_flow(base_url: str, ws_url: str) -> None:
    client_id = str(random.randrange(10**9, 10**10))
    print(f"\n  Streamer flow (client_id={client_id})")
    async with connect(f"{ws_url}{WS_PATH}?client_id={client_id}") as ws:
        first = json.loads(await asyncio.wait_for(ws.recv(), RECV_TIMEOUT))
        check(
            "on connect: a state with the header, paused replay, no stats yet",
            first.get("type") == "state"
            and first.get("header") is not None
            and first.get("player", {}).get("mode") == "replay"
            and first.get("player", {}).get("paused") is True
            and first.get("stats") is None,
            f"got {json.dumps(first)[:300]}",
        )

        command(base_url, client_id, "play")

        loop = asyncio.get_running_loop()
        deadline = loop.time() + STATS_TIMEOUT
        state: dict = {}
        frames = 0
        while loop.time() < deadline:
            remaining = deadline - loop.time()
            try:
                state = json.loads(await asyncio.wait_for(ws.recv(), min(RECV_TIMEOUT, remaining)))
            except asyncio.TimeoutError:
                break
            if state.get("frame") is not None:
                frames += 1
            if state.get("stats") is not None:
                break
        stats = state.get("stats") or {}
        frame = state.get("frame") or {}
        check(
            f"after play: the stats module's result arrives on the socket (after {frames} frame(s))",
            bool(stats),
            f"no stats within {STATS_TIMEOUT:.0f}s; last state {json.dumps(state)[:300]}",
        )
        if stats:
            # For the frame shown, or the one just before it: with the module in
            # the worker the result lands a few ms after its frame, and the
            # server keeps the previous frame's stats for exactly that gap.
            behind = (_instant(frame.get("timestamp")) - _instant(stats.get("timestamp"))).total_seconds()
            check(
                "the result is the module's, for the frame shown (or the frame before)",
                stats.get("app", {}).get("name") == "frame-stats"
                and stats.get("result", {}).get("n_stations") == 5
                and 0 <= behind <= 0.075,
                f"got {json.dumps(stats)[:300]} for frame at {frame.get('timestamp')}",
            )

        command(base_url, client_id, "stop")


async def main(argv: list[str]) -> int:
    if len(argv) != 2:
        print(f"usage: {argv[0]} <base-url>", file=sys.stderr)
        return 2
    base_url = argv[1].rstrip("/")
    ws_url = "ws" + base_url.removeprefix("http")
    try:
        await stream_flow(base_url, ws_url)
    except Exception as error:
        bad(f"streamer flow could not run: {type(error).__name__}: {error}")
    if failures:
        print(f"\n  \033[31m{len(failures)} step(s) failed\033[0m")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main(sys.argv)))
