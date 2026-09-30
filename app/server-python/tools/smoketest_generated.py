# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""Drive two freshly generated apps over the wire. Driven by scripts/check-generators.sh.

    python tools/smoketest_generated.py <base-url> <counter-slug> <module-slug>

The counter (generate-new-subapp.sh): open its socket, see count 0, POST a bump,
see count 1. The module app (generate-new-module-with-frontend.sh): open its
socket and wait for a state whose ``result`` is filled in -- the generated
module's output, hosted by the server over the in-memory transport.

Same dependencies as the other smoke tools: websockets from the server's own
environment, urllib for the POST. Exits 0 if both flows passed, 1 otherwise.
"""

import asyncio
import json
import random
import sys
import urllib.request

from websockets.asyncio.client import connect

RECV_TIMEOUT = 5.0
RESULT_TIMEOUT = 15.0


def client_id() -> str:
    return str(random.randint(10**9, 10**10 - 1))


async def receive(ws, timeout: float = RECV_TIMEOUT) -> dict:
    return json.loads(await asyncio.wait_for(ws.recv(), timeout))


async def counter_flow(base_url: str, ws_url: str, slug: str) -> None:
    cid = client_id()
    async with connect(f"{ws_url}/api/{slug}/ws?client_id={cid}") as ws:
        first = await receive(ws)
        assert first.get("count") == 0, f"first state: {first}"
        request = urllib.request.Request(
            f"{base_url}/api/{slug}/count/bump?client_id={cid}", method="POST", data=b""
        )
        with urllib.request.urlopen(request, timeout=RECV_TIMEOUT) as response:
            assert response.status == 200, f"bump answered {response.status}"
        second = await receive(ws)
        assert second.get("count") == 1, f"after bump: {second}"


async def module_flow(ws_url: str, slug: str) -> None:
    async with connect(f"{ws_url}/api/{slug}/ws?client_id={client_id()}") as ws:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + RESULT_TIMEOUT
        while loop.time() < deadline:
            state = await receive(ws)
            if state.get("result") is not None:
                print(f"    first result: {state['result']['result']}")
                return
        raise AssertionError(f"no result within {RESULT_TIMEOUT:.0f} s")


async def main(argv: list[str]) -> int:
    if len(argv) != 4:
        print(__doc__.strip().splitlines()[2].strip(), file=sys.stderr)
        return 2
    base_url, counter, module = argv[1].rstrip("/"), argv[2], argv[3]
    ws_url = base_url.replace("http", "ws", 1)
    failed = False
    for label, flow in (
        (f"counter /{counter}: bump comes back on the socket", counter_flow(base_url, ws_url, counter)),
        (f"module /{module}: a result comes down the socket", module_flow(ws_url, module)),
    ):
        try:
            await flow
            print(f"    \033[32m✓\033[0m {label}")
        except Exception as error:
            failed = True
            print(f"    \033[31m✗\033[0m {label} -- {type(error).__name__}: {error}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main(sys.argv)))
