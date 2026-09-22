# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""Drive the Time Series Explorer end to end. Driven by scripts/e2e-smoke-test.sh.

    python tools/smoketest_time_series_explorer.py <base-url>

The third wire-level smoke test: open the explorer's socket, read the coverage
the provider reports, count one second of it, and play half a second of it to
the end. Under compose the provider is the ``TimeSeriesDatabaseClient`` over
the stub service and the broker (``TIME_SERIES_EXPLORER_DATA_CLIENTS``), so
the count and the frames crossed REST and a Kafka topic; under a bare
``docker run`` (CI) it is the sample recording in-process. The socket cannot
tell which -- that is the property under test -- and the numbers are the same
because both are the same 20 Hz recording.

Same shape and dependencies as the other two: websockets from the server's
own environment, urllib for the POSTs, every step reported.
"""

import asyncio
import json
import random
import sys
import urllib.error
import urllib.request
from datetime import datetime, timedelta

from websockets.asyncio.client import connect

WS_PATH = "/api/time-series-explorer/ws"
API_PATH = "/api/time-series-explorer"

#: Over the stub the count crosses REST and the broker; generous, so a wedged
#: path fails here rather than being masked.
RESULT_TIMEOUT = 15.0
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


def post(base_url: str, path: str, client_id: str, body: dict | None = None) -> tuple[int, dict]:
    data = b"" if body is None else json.dumps(body).encode()
    request = urllib.request.Request(f"{base_url}{path}?client_id={client_id}", method="POST", data=data)
    if body is not None:
        request.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(request, timeout=HTTP_TIMEOUT) as response:
            return response.status, json.loads(response.read() or b"{}")
    except urllib.error.HTTPError as error:
        return error.code, json.loads(error.read() or b"{}")
    except (urllib.error.URLError, OSError) as error:
        return 0, {"unreachable": str(error)}


def command(base_url: str, client_id: str, path: str, verb: str, body: dict | None = None) -> None:
    status, reply = post(base_url, f"{API_PATH}{path}", client_id, body)
    check(
        f"POST {API_PATH}{path} -> 200 {{status, applied: {verb}}}",
        status == 200 and reply.get("applied") == verb and "status" in reply,
        f"got {status} {reply}",
    )


async def wait_for(ws, predicate, timeout: float) -> dict:
    """The first state satisfying ``predicate`` within ``timeout``, else the last one seen."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    state: dict = {}
    while loop.time() < deadline:
        remaining = deadline - loop.time()
        try:
            state = json.loads(await asyncio.wait_for(ws.recv(), min(RECV_TIMEOUT, remaining)))
        except asyncio.TimeoutError:
            break
        if predicate(state):
            return state
    return state


def iso(moment: datetime) -> str:
    return moment.isoformat()


async def explorer_flow(base_url: str, ws_url: str) -> None:
    client_id = str(random.randrange(10**9, 10**10))
    print(f"\n  Explorer flow (client_id={client_id})")
    async with connect(f"{ws_url}{WS_PATH}?client_id={client_id}") as ws:
        first = json.loads(await asyncio.wait_for(ws.recv(), RECV_TIMEOUT))
        player = first.get("player", {})
        check(
            "on connect: a state with a paused replay and the provider's coverage",
            first.get("type") == "state"
            and player.get("mode") == "replay"
            and player.get("paused") is True
            and player.get("coverage_start") is not None
            and player.get("coverage_end") is not None
            and first.get("count") is None,
            f"got {json.dumps(first)[:300]}",
        )
        if player.get("coverage_start") is None:
            return
        start = datetime.fromisoformat(player["coverage_start"])

        # (b) the batch case: count one second through the row-count module.
        command(base_url, client_id, "/count", "count",
                {"start": iso(start), "end": iso(start + timedelta(seconds=1))})
        state = await wait_for(ws, lambda s: s.get("count") is not None, RESULT_TIMEOUT)
        result = (state.get("count") or {}).get("result") or {}
        check(
            "after count: the row-count module's result arrives with 20 rows and no error",
            result.get("count") == 20 and result.get("error") is None,
            f"got {json.dumps(state.get('count'))[:300]}",
        )

        # (a) the stream case: play half a second, bounded, and see it end there.
        end = start + timedelta(seconds=0.5)
        command(base_url, client_id, "/playback/play-range", "replay",
                {"start": iso(start), "end": iso(end)})
        state = await wait_for(ws, lambda s: s.get("frame") is not None, RESULT_TIMEOUT)
        check(
            "after play-range: frames arrive on the socket",
            state.get("frame") is not None,
            f"no frame within {RESULT_TIMEOUT:.0f}s; last state {json.dumps(state)[:300]}",
        )
        state = await wait_for(ws, lambda s: s.get("player", {}).get("ended") is True, RESULT_TIMEOUT)
        player = state.get("player", {})
        check(
            "the bounded replay ends paused at the range end, with no error",
            player.get("ended") is True
            and player.get("paused") is True
            and player.get("range_end") is not None
            and datetime.fromisoformat(player["range_end"]) == end
            and player.get("error") is None,
            f"got {json.dumps(player)[:300]}",
        )

        # A range outside the coverage is refused before anything is published.
        status, reply = post(base_url, f"{API_PATH}/count", client_id,
                             {"start": iso(start - timedelta(seconds=5)), "end": iso(start)})
        check("a range outside the coverage is refused with 409", status == 409, f"got {status} {reply}")

        command(base_url, client_id, "/playback/stop", "stop")


async def main(argv: list[str]) -> int:
    if len(argv) != 2:
        print(f"usage: {argv[0]} <base-url>", file=sys.stderr)
        return 2
    base_url = argv[1].rstrip("/")
    ws_url = "ws" + base_url.removeprefix("http")
    try:
        await explorer_flow(base_url, ws_url)
    except Exception as error:
        bad(f"explorer flow could not run: {type(error).__name__}: {error}")
    if failures:
        print(f"\n  \033[31m{len(failures)} step(s) failed\033[0m")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main(sys.argv)))
