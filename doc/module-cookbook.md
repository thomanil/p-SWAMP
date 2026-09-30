# Module cookbook

Recipes for adding an analysis **module** to p-SWAMP: a small service that reads
the PMU stream and publishes a result. Optionally, add a page in the web client
that shows the result and sends commands to the module.

Every recipe builds the same example, **Peak frequency**: for each frame, the
station with the highest frequency. `generate-new-module-with-frontend.sh`
writes it for you under any name; the recipes explain what it wrote and how to
take it further. `doc/server-data-architecture.md` explains why each piece
exists.

## The pieces

```
source ─▶ gateway ─▶ player ─PmuFrame─▶ topic peak-frequency.pmu.frame ─▶ PeakFrequencyModule.process
                                                                                    │
browser ◀─ socket ◀─ api.py ◀─ pipeline.latest ◀─ topic peak-frequency.peak.frequency.result ◀┘
```

| Piece | Where | What it is |
|---|---|---|
| Module | `peak_frequency/peak_frequency_module.py` | A class: what it reads (`input_model`), what it publishes (`output_model`), `process` |
| Family | `peak_frequency/family.py` | The app's name (its topic namespace), its data sources, its modules |
| Edge | `peak_frequency/api.py` | One pipeline per browser client, and the socket that pushes state |
| Page | `app/client-web/src/pages/peak-frequency/` | Renders the state and sends commands |

Three facts explain most surprises:

- **A pipeline exists only while a client has the app's socket open.** It is
  built on the first connect, keyed by the browser's client id, and dropped
  300 s after its last socket closes. With no socket open there are no frames,
  so the module never runs.
- **A module does not talk to Kafka directly.** It is handed one message and returns one
  result; the host around it does the rest. (We may make Kafka swappable for NATS later)
- **Where a module runs is configuration.** With `PSWAMP_TRANSPORT` unset, the
  server hosts the module in its own process. With Kafka (compose, minikube), a
  worker container hosts it and the server hosts none.

## Q: How do I add a new module?

The minimal case: read every PMU frame and publish a small result for each.

```
./scripts/generate-new-module-with-frontend.sh peak-frequency "Peak frequency"
```

It writes a working app, wires it in everywhere (`APPS` in `server.py`, the
route, the nav, the socket path, and the module-worker's families in
`docker-compose.yml` and `k8s/p-swamp-local.yaml`), regenerates the api
contract and runs `error_check.sh`. Commit `doc/api/openapi.json` and
`app/client-web/src/api/schema.ts` with the rest.

| File | What it is |
|---|---|
| `app/server-python/src/peak_frequency/peak_frequency_module.py` | The module: replace its analysis with yours |
| `app/server-python/src/peak_frequency/family.py` | The family: topics, data sources, modules |
| `app/server-python/src/peak_frequency/api.py`, `__init__.py` | The edge: one pipeline per client, the socket |
| `app/client-web/src/pages/peak-frequency/` | The page and its socket hook |
| `app/server-python/tests/test_peak_frequency.py` | Unit tests and a pipeline test |

You need the app package even for a module you never look at in a browser,
because its socket is what starts a pipeline. For a page and api *without* a
module (to prototype frontend work), `./scripts/generate-new-subapp.sh` writes a
per-client counter instead.

What it wrote, file by file (docstrings trimmed):

**The module**, `peak_frequency/peak_frequency_module.py`:

```python
from typing import Literal

from pydantic import BaseModel, Field

from pswamp_core.messages import PmuFrame, ResultEnvelope
from pswamp_core.modules import Module


def highest_frequency(
    stations: list[str], frequencies: list[float | None]
) -> tuple[str, float] | None:
    readings = [(hz, station) for station, hz in zip(stations, frequencies) if hz is not None]
    if not readings:
        return None
    hz, station = max(readings)
    return station, hz


class PeakFrequencyBody(BaseModel):
    station: str = Field(description="The station with the highest frequency in the frame.")
    frequency_hz: float = Field(description="Its frequency, in Hz.")


class PeakFrequencyResult(ResultEnvelope[PeakFrequencyBody]):
    version: Literal["v1"] = "v1"


class PeakFrequencyModule(Module):
    name = "peak-frequency"
    input_model = PmuFrame
    output_model = PeakFrequencyResult

    async def process(self, frame: PmuFrame) -> PeakFrequencyBody | None:
        columns = frame.header.columns(measurement="f")
        peak = highest_frequency(
            [frame.header.station[i] for i in columns],
            [frame.values[i] for i in columns],
        )
        if peak is None:
            return None  # publish nothing for this frame
        station, hz = peak
        return PeakFrequencyBody(station=station, frequency_hz=hz)
```

- **The analysis is a plain function**, `highest_frequency`: plain values in,
  plain values out, no frames and no `async`. `process` only adapts a frame to
  it and its answer to a result. Keep that split when you replace the
  placeholder: it is what lets you test the analysis on its own (next recipe),
  and try it in a notebook. A larger analysis gets a file of its own beside the
  module, importing nothing from `pswamp_core`.
- `process` gets one frame and returns the result body, or `None` to publish
  nothing. The envelope (timestamp, module identity, `parameters`) is added for
  you.
- The frame carries its own channel layout in `frame.header`:
  `columns(measurement="f")` gives the frequency columns, and `station[i]` the
  station of column `i`. For a 700-column frame, derive the indexes once and
  keep them until `frame.header.header_id` changes.
- A result class's name is its topic: `PeakFrequencyResult` publishes to
  `peak.frequency.result`.
- Import `pswamp_core`, pydantic, and anything declared in
  `app/server-python/pyproject.toml`. To reuse an algorithm from the desktop
  package, follow the existing modules: they copy the function from
  `src/pswamp/monitoring/` (`islanding_module.py`, `n4sid_module.py`).

**The family**, `peak_frequency/family.py`:

```python
from pswamp_core.datagateway import DataGateway, gateway_from_env
from pswamp_core.pipeline import PipelineFamily

from .peak_frequency_module import PeakFrequencyModule

APP = "peak-frequency"

DEFAULT_DATA_CLIENTS = "sample:pmu_test_streamer.sample_client:SampleRecordingClient"
DATA_CLIENTS_VARIABLE = "PEAK_FREQUENCY_DATA_CLIENTS"


def gateway() -> DataGateway:
    return gateway_from_env(DEFAULT_DATA_CLIENTS, variable=DATA_CLIENTS_VARIABLE)


FAMILY = PipelineFamily(APP, gateway, (PeakFrequencyModule,))
```

`APP` is the url slug, and every topic of the app starts with it. The source
here is the committed 3-second sample; the last recipe lists the others.

**The edge**, `peak_frequency/api.py`:

```python
import contextlib
from typing import Literal

from fastapi import APIRouter, FastAPI, WebSocket
from pydantic import BaseModel, Field
from shared import connected_pipeline, get_logger, push_changes, serve_family, transport

from pswamp_core.messages import PlayerStatus
from pswamp_core.pipeline import Pipeline, PipelineRegistry

from .family import FAMILY
from .peak_frequency_module import PeakFrequencyResult

logger = get_logger("peak-frequency")


def build_pipeline(client_id: str) -> Pipeline:
    return Pipeline(client_id, FAMILY, transport(), autoplay=True, loop=True)


REGISTRY: PipelineRegistry[Pipeline] = PipelineRegistry(
    build_pipeline, max_pipelines=8, idle_seconds=300.0
)


@contextlib.asynccontextmanager
async def lifespan(app: FastAPI):
    async with serve_family(FAMILY, REGISTRY):
        yield


class PeakFrequencyState(BaseModel):
    type: Literal["state"] = "state"
    player: PlayerStatus = Field(description="Which stream is open, and where it is.")
    result: PeakFrequencyResult | None = Field(
        description="The module's latest result; null until the first."
    )


def state_message(pipeline: Pipeline) -> PeakFrequencyState:
    return PeakFrequencyState(
        player=pipeline.player.status(),
        result=pipeline.latest.get(PeakFrequencyResult),
    )


router = APIRouter()


@router.websocket("/ws")
async def ws_endpoint(ws: WebSocket) -> None:
    async with connected_pipeline(ws, REGISTRY) as pipeline:
        if pipeline is not None:
            await push_changes(ws, pipeline, lambda: state_message(pipeline))
```

`__init__.py` exports `router`, `WS_MESSAGE = PeakFrequencyState` (which puts
the socket message in the api contract) and `lifespan`. Keep `lifespan`:
`serve_family` is what hosts the module in the server when there is no broker.

**The worker entry.** Under compose and minikube the server hosts no modules,
so the script appended `peak_frequency.family:FAMILY` to the module-worker's
`PSWAMP_WORKER_FAMILIES` in `docker-compose.yml` and `k8s/p-swamp-local.yaml`.
Rename or move the family and that entry must follow; without it the page
connects and waits forever for a result.

### How do I know it is up and running in local dev?

**In the compose stack** (Kafka plus workers, the usual dev setup). A new
package needs an image rebuild, and so does an edit to `docker-compose.yml`, so
restart the server script (Ctrl-C, then run it again):

```
./scripts/start-local-hotloaded-pswamp-server.sh
```

At startup, its log shows:

```
server-1         | … INFO [shared] peak-frequency: modules peak-frequency are hosted by a worker
module-worker-1  | … INFO [pswamp_core.worker] worker hosting …, peak-frequency/peak-frequency
```

Next, start a pipeline. Either open the page (http://127.0.0.1:8000/peak-frequency,
or http://localhost:5173/peak-frequency with the web client script) or connect
from a terminal:

```bash
uv run --project app/server-python python - <<'EOF'
import asyncio, json
from websockets.asyncio.client import connect

URL = "ws://127.0.0.1:8000/api/peak-frequency/ws?client_id=4242424242"

async def main():
    async with connect(URL) as ws:
        for _ in range(5):
            print(json.loads(await ws.recv())["result"])

asyncio.run(main())
EOF
```

`result` is `null` in the first message or two, then results arrive at the frame
rate. The worker logs one module instance per client:

```
module-worker-1  | … INFO [pswamp_core.host] peak-frequency: module started for key 4242424242 (1 live)
```

When that client's pipeline goes, the worker logs `module dropped for key
4242424242`. Saving a change to the module restarts the worker (compose watch);
reconnect to get a fresh instance.

**In one process, with no broker.** This is the fastest loop, and it is how CI
and the tests run. Stop the compose stack first, since both use port 8000:

```
(cd app/server-python && uv run src/server.py)
```

Its log shows `[shared] transport: memory (modules hosted in this process)`,
followed by the module's own lines in the same terminal. It does not reload, so
rerun it after a change. It serves no page itself; run the web client script
beside it, which proxies `/api` to port 8000.

## Q: How do I unit test the logic inside my module, so I know how it behaves with example data?

Work bottom up, in three layers. The generator wrote all three into
`app/server-python/tests/test_peak_frequency.py`.

**Running them.** From the repo root, with nothing else running: no Docker and
no server, since the pipeline test hosts the module in the test process.

```
./scripts/run-python-server-tests.sh -k peak_frequency       # this app's tests
./scripts/run-python-server-tests.sh -k peak_frequency -v    # the same, one line per test
./scripts/run-python-server-tests.sh tests/test_peak_frequency.py::test_skips_stations_without_a_value
./scripts/run-python-server-tests.sh -x                      # every server and core test (~10 s), stop at the first failure
```

The script is `uv run pytest` run in `app/server-python/`, where the pytest
configuration lives, so every argument is a pytest argument and a test path is
relative to that directory. The first run creates the environment,
`app/server-python/.venv`: point an editor's test runner at that interpreter,
with `app/server-python` as its working directory. CI runs the whole suite on
every pull request (the `unit-tests` job), so these tests gate a merge like the
rest.

**1. The analysis: plain functions, plain tests.** Test the function directly,
with ordinary synchronous tests: no frames, no module, no `async`, no pytest
plugins. This is where the edge cases go, and it is the fastest loop there is.

```python
from peak_frequency.peak_frequency_module import highest_frequency


def test_picks_the_station_with_the_highest_frequency():
    assert highest_frequency(["a", "b", "c"], [49.9, 50.1, 50.0]) == ("b", 50.1)


def test_skips_stations_without_a_value():
    assert highest_frequency(["a", "b"], [None, 49.8]) == ("b", 49.8)


def test_none_when_no_station_has_a_value():
    assert highest_frequency(["a", "b"], [None, None]) is None
    assert highest_frequency([], []) is None
```

Compare computed floats with `pytest.approx(expected)`, or
`numpy.testing.assert_allclose` for arrays, rather than `==`. As the analysis
grows, split it into more small functions and test each the same way.

**2. The module: a frame in, a result body out.** `process` is an async method;
call it with a hand-made frame to check that it reads the right columns and
fills the result. A few cases are enough, since the analysis is already covered:

```python
from datetime import datetime, timezone

import pytest

from peak_frequency.peak_frequency_module import PeakFrequencyModule
from pmu_test_streamer.sample_client import SampleRecordingClient
from pswamp_core.messages import PmuFrame, PmuHeader


def frame(values: list[float | None]) -> PmuFrame:
    """A hand-made frame: one frequency channel per station."""
    n = len(values)
    header = PmuHeader(
        station=[f"s{i}" for i in range(n)],
        channel=["f"] * n,
        measurement=["f"] * n,
        units=["Hz"] * n,
        data_rate=50.0,
    )
    return PmuFrame(mRID="test", timestamp=datetime.now(timezone.utc), header=header, values=values)


@pytest.mark.asyncio
async def test_process_reads_the_frequency_columns_of_a_frame():
    result = await PeakFrequencyModule().process(frame([49.9, 50.1, None]))
    assert result is not None
    assert (result.station, result.frequency_hz) == ("s1", 50.1)


@pytest.mark.asyncio
async def test_publishes_nothing_for_a_frame_without_frequencies():
    assert await PeakFrequencyModule().process(frame([None, None])) is None


@pytest.mark.asyncio
async def test_over_the_sample_recording():
    module = PeakFrequencyModule()
    for f in SampleRecordingClient().frames:
        result = await module.process(f)
        assert result is not None and 49 < result.frequency_hz < 51
```

To run it over the Nordic 44 line trip as well:

```python
from islanding_stream.n44_client import N44RecordingClient


@pytest.mark.asyncio
async def test_over_the_n44_line_trip():
    module = PeakFrequencyModule()
    peaks = [await module.process(f) for f in N44RecordingClient(measurements=["f"]).frames()]
    assert len(peaks) == 3501
    assert max(p.frequency_hz for p in peaks) > 50.0
```

Example data in the repo:

| Frames | From | What they are |
|---|---|---|
| `SampleRecordingClient().frames` | `pmu_test_streamer.sample_client` | 5 stations, voltage phasor + frequency, 20 Hz, 60 frames (3 s) spanning a line trip |
| `N44RecordingClient(measurements=["f"]).frames()` | `islanding_stream.n44_client` | Nordic 44: 44 stations, 50 Hz, 3501 frames (70 s), a line trip and a reconnection; stations 6500, 6700, 6701 islanded from about 20 s. Omit `measurements` for all 700 channels |
| `frame([...])` | the helper above | The edge cases you pick |

The recordings feed the plain functions too: pick the columns you need from a
frame's `header` and pass its `values` for them, as `process` does.

**3. The pipeline: the module hosted, results on the transport.** The whole
pipeline in the test process, with the module hosted over the in-memory
transport. That transport makes the same JSON round trip as Kafka, so a result
that would not survive the wire fails here too:

```python
import asyncio

from app_test_support import Watch, fresh_transport
from peak_frequency import api, family
from peak_frequency.peak_frequency_module import PeakFrequencyResult


@pytest.fixture
def _own_transport(monkeypatch):
    with fresh_transport(monkeypatch) as transport:
        yield transport


@pytest.mark.asyncio
async def test_the_pipeline_delivers_results(_own_transport, monkeypatch):
    monkeypatch.delenv(family.DATA_CLIENTS_VARIABLE, raising=False)
    async with api.lifespan(None):  # hosts the module in this process
        pipeline = api.build_pipeline("42")
        seen = Watch(pipeline)
        await pipeline.start()
        try:
            with seen.subscribe(PeakFrequencyResult) as results:
                result = await asyncio.wait_for(results.get(), 2)
            assert result.app.name == "peak-frequency"
            assert api.state_message(pipeline).result is not None
        finally:
            await pipeline.stop()
```

## Q: How do I see what is going on with the module when it is running inside the p-SWAMP pipeline?

**What is logged without any code from you.** Every line reads
`HH:MM:SS LEVEL [logger] message`, on stdout:

| Line | Means |
|---|---|
| `worker hosting …, peak-frequency/peak-frequency` | The worker loaded your family |
| `hosting peak-frequency for peak-frequency: pmu.frame in, peak.frequency.result out, …` | The module's host subscribed to its topics |
| `pipeline started for 4242424242 (1/8 live)` | The server built a client's pipeline |
| `peak-frequency: module started for key 4242424242 (1 live)` | That client's first frame reached the host, which built an instance |
| `peak-frequency: module dropped for key 4242424242 (…)` | Its pipeline closed, or 300 s passed with no frames |
| `module peak-frequency failed on PmuFrame`, with a traceback | `process` raised; that frame is skipped |
| `peak-frequency is not keeping up with pmu.frame: …` | Frames were dropped, or were over 2 s old when read |

**Your own lines:**

```python
from pswamp_core.log import get_logger

logger = get_logger("peak-frequency")
logger.info("new peak station %s at %.3f Hz", station, hz)
```

Log on a change, not on every frame: frames arrive 20 to 50 times a second, for
each client.

**Compose.** The server script's terminal streams every container, each line
prefixed with its service name. To follow one container:

```
docker compose logs -f module-worker                     # or server, mode-estimation-worker
docker compose logs -f module-worker | grep peak-frequency
```

**Minikube.** `./scripts/logs-minikube.sh` follows the server only, and the
module runs in the worker:

```
kubectl logs -f deployment/p-swamp-module-worker
kubectl get pods                                          # RESTARTS: has it crashed?
kubectl logs deployment/p-swamp-module-worker --previous  # the crashed container's output
```

**One process.** Everything is in the one terminal.

**In the browser.** The error tray, on every page, shows what went wrong for
*your* client: `process` raising, a refused command, the module falling behind.

**On the wire (compose).** Use Kafka's CLI in the broker container. A topic is
named `<app>.<message topic>`, a record's key is the client id, and its value
is the message as JSON:

```
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list | grep peak-frequency
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic peak-frequency.peak.frequency.result --property print.key=true --max-messages 5
```

The consumer only reads records produced after it starts, so keep a page open
while it runs. Topics keep about a minute of data.

## Q: How do I add an interactive web UI page for it?

The generator wrote the page and registered its route and nav entry. Extend
it from here.

**1. Open it.** Start the server stack, then the web client, each in a
terminal of its own:

```
./scripts/start-local-hotloaded-pswamp-server.sh       # the server stack on http://127.0.0.1:8000
./scripts/start-local-hotloaded-pswamp-web-client.sh   # the web client, hot-reloading, on http://localhost:5173
```

| URL | Served by | Shows an edit to the page |
|---|---|---|
| http://localhost:5173/peak-frequency | Vite, which forwards `/api` to port 8000 | At once (hot reload) |
| http://127.0.0.1:8000/peak-frequency | The server, with the client built into its image at the last start | After restarting the server script |
| http://127.0.0.1:30080/peak-frequency | Minikube, after `./scripts/start-pswamp-in-local-minikube-cluster.sh`; on Linux also `http://$(minikube ip):30080/peak-frequency` | After rerunning that script |

The page is in the nav as "Peak frequency". It says "Waiting for the first
frame…", then shows the station with the highest frequency, updated 20 times a
second from the looping sample recording.

**2. The hook**, `app/client-web/src/pages/peak-frequency/usePeakFrequencySocket.ts`.
Its type comes from the generated contract (`Wire['PeakFrequencyState']`), never
a hand-written copy, so a field added on the server is a field in TypeScript
after `./scripts/generate-api-contract.sh`. The contract is not hot-reloaded:
rerun that script after changing a model the socket sends.

```ts
import type { Wire } from '@/api/wire'
import { PEAK_FREQUENCY_WS_PATH } from '@/lib/servers'
import { useServerSocket } from '@/hooks/useServerSocket'

export type PeakFrequencyState = Wire['PeakFrequencyState']

export function usePeakFrequencySocket() {
  const { message, status, connected } = useServerSocket<PeakFrequencyState>(PEAK_FREQUENCY_WS_PATH)
  return { state: message, status, connected }
}
```

**3. The page**, `PeakFrequencyPage.tsx`:

```tsx
import { Badge } from '@/components/ui/badge'
import { Card, CardAction, CardContent, CardHeader, CardTitle } from '@/components/ui/card'

import { usePeakFrequencySocket } from './usePeakFrequencySocket'

export function PeakFrequencyPage() {
  const { state, connected } = usePeakFrequencySocket()
  const result = state?.result?.result

  return (
    <Card className="w-full max-w-md">
      <CardHeader>
        <CardTitle>Peak frequency</CardTitle>
        <CardAction>
          <Badge variant={connected ? 'default' : 'outline'}>{connected ? 'Online' : 'Offline'}</Badge>
        </CardAction>
      </CardHeader>
      <CardContent>
        {result ? (
          <div className="text-2xl tabular-nums">
            {result.frequency_hz.toFixed(3)} Hz <span className="text-gray-500">at {result.station}</span>
          </div>
        ) : (
          <div className="text-gray-500">Waiting for the first frame…</div>
        )}
      </CardContent>
    </Card>
  )
}
```

State arrives once per frame. Plain React state is fine for a small message
like this one. For a chart, keep the samples in a `useRef` and redraw from
`useServerSocket`'s `onMessage` option, or cap the rate on the server with
`push_changes(..., min_interval=0.1)` (10 messages a second).

## Q: How do I send commands to my module from the web frontend?

A command goes up as a `POST`, becomes a typed message on the module's command
topic, and reaches that client's module instance. Its effect comes back on the
socket like any other change; the `POST` only acknowledges it.

The example adds a threshold per client, and each result says whether the peak
is above it.

**1. The module**: add a command class beside it, list it in `commands`, and
apply it in `handle`:

```python
from typing import ClassVar

from pswamp_core.messages import Command


class PeakFrequencyBody(BaseModel):
    station: str = Field(description="The station with the highest frequency in the frame.")
    frequency_hz: float = Field(description="Its frequency, in Hz.")
    above_threshold: bool = Field(description="Whether frequency_hz exceeds this client's threshold.")


class SetThresholdCommand(Command):        # topic: set.threshold.command
    hz: float = Field(description="The new threshold, in Hz.")


class PeakFrequencyModule(Module):
    name = "peak-frequency"
    input_model = PmuFrame
    output_model = PeakFrequencyResult
    commands: ClassVar[tuple[type[Command], ...]] = (SetThresholdCommand,)

    def __init__(self) -> None:
        super().__init__()
        self.threshold_hz = 50.1

    async def process(self, frame: PmuFrame) -> PeakFrequencyBody | None:
        ...                                  # as before, then:
        return PeakFrequencyBody(station=station, frequency_hz=hz, above_threshold=hz > self.threshold_hz)

    async def handle(self, command: SetThresholdCommand) -> None:
        self.threshold_hz = command.hz       # the next result shows it
```

- `handle` returns `None` (nothing to publish now) or a result body, which is
  published at once with the command's `request_id`.
- One instance per client means the threshold is per client.
- To refuse a command in the module's current state, implement
  `validate(command)` and raise `CommandRefused`. It runs where the module
  runs, so the refusal reaches the error tray, not the `POST`'s status. Put
  fixed range checks on the request body instead (step 2): pydantic rejects
  those at once with a 422.

**2. The route**, in `api.py`. Add `COMMAND_RESPONSES`, `ClientId`,
`CommandAck` and `dispatch_command` to the `shared` import:

```python
class ThresholdBody(BaseModel):
    hz: float = Field(ge=45, le=55, description="Flag peaks above this frequency, in Hz.")


@router.post("/threshold", operation_id="peak_frequency_threshold", responses=COMMAND_RESPONSES)
async def threshold(client_id: ClientId, body: ThresholdBody) -> CommandAck:
    """Set this client's threshold. The effect arrives on the socket."""
    return dispatch_command(REGISTRY, SetThresholdCommand(client_id=client_id, hz=body.hz), logger)
```

It answers 200 once the command is published, 404 when the client has no
pipeline (its page is not open), and 422 for a bad body.

**3. Regenerate the contract**, so the path and body are typed in the client:
`./scripts/generate-api-contract.sh`.

**4. The hook** gets one function per operation:

```ts
import { useCallback } from 'react'

import type { Wire } from '@/api/wire'
import { fireCommand, postCommand } from '@/lib/commands'
import { PEAK_FREQUENCY_API_PATH, PEAK_FREQUENCY_WS_PATH } from '@/lib/servers'
import { useServerSocket } from '@/hooks/useServerSocket'

export type PeakFrequencyState = Wire['PeakFrequencyState']

export function usePeakFrequencySocket() {
  const { message, status, connected } = useServerSocket<PeakFrequencyState>(PEAK_FREQUENCY_WS_PATH)

  const setThreshold = useCallback(
    (hz: number) =>
      fireCommand('peak-frequency', postCommand(`${PEAK_FREQUENCY_API_PATH}/threshold`, { body: { hz } })),
    [],
  )

  return { state: message, status, connected, setThreshold }
}
```

`postCommand` is typed against the contract, so a wrong path or body is a
`tsc` error. It rejects with a `CommandError` carrying the server's `detail` on
any non-2xx answer; `fireCommand` logs that to the console, since a command that
did not land changes nothing on screen.

**5. The page** gets a control, disabled while offline (a command sent then
would have no socket to show its effect on):

```tsx
import { Button } from '@/components/ui/button'
…
const { state, connected, setThreshold } = usePeakFrequencySocket()
…
<Button disabled={!connected} onClick={() => setThreshold(50.0)}>Flag peaks above 50.000 Hz</Button>
```

It renders `result.above_threshold`, for example as a red value.

**6. Unit test `handle`** beside `process`:

```python
@pytest.mark.asyncio
async def test_a_lower_threshold_flags_the_peak():
    module = PeakFrequencyModule()
    assert not (await module.process(frame([50.05]))).above_threshold
    await module.handle(SetThresholdCommand(hz=50.02))
    assert (await module.process(frame([50.05]))).above_threshold
```

The player's own commands (play, pause, step, seek, speed, switch source) work
differently. The server checks them before publishing, so a refusal comes back
as a 409 on the `POST`. `pmu_test_streamer/api.py` has one route for each, and
its page renders the transport controls from `PlayerStatus`. A page with
transport controls builds its pipeline without `autoplay=True`, so a replay
starts paused.

## Q: How do we configure a different pipeline (data source, etc.) for it in real deployments?

| Set in code (a PR) | Set in the deployment's environment |
|---|---|
| Which modules a family has | Which sources a gateway reads: `PEAK_FREQUENCY_DATA_CLIENTS`, plus each source's settings |
| The default sources (`DEFAULT_DATA_CLIENTS`) | The transport: `PSWAMP_TRANSPORT`, `KAFKA_BOOTSTRAP_SERVERS` |
| Player options in `build_pipeline` (`autoplay`, `loop`, `speed`) | Which worker hosts which family: `PSWAMP_WORKER_FAMILIES` |
| | A module's own settings, if it reads any (`MODE_ESTIMATION_EXECUTION`, for example) |

**Data sources.** `<APP>_DATA_CLIENTS` is a comma-separated list of
`name:module.path:Class` entries. A pipeline starts on the first one listed. A
history source is replayed (paced, seekable, looping); a live source is tailed.
Each source reads its settings from `<NAME>_<SETTING>`, where `NAME` is the
part before the first colon, upper-cased. **Give your source a name no other
app uses**: `N44_MEASUREMENTS` would also reach the islanding stream's `n44`
source. The sources available now:

| Class | Kind | Settings | Serves |
|---|---|---|---|
| `pmu_test_streamer.sample_client:SampleRecordingClient` | history | `<NAME>_PATH` (default: the committed sample) | A text recording: 5 stations, 20 Hz |
| `pmu_test_streamer.live_client:LiveSyntheticClient` | live | `<NAME>_PATH` | The same format's rows, re-stamped on the wall clock at 20 Hz |
| `islanding_stream.n44_client:N44RecordingClient` | history | `<NAME>_MEASUREMENTS` (e.g. `f`; all 700 channels if unset) | The Nordic 44 line trip, 50 Hz |
| `pswamp_core.datagateway.clients.remote_data:RemoteDataClient` | history | `<NAME>_URL` (required), `<NAME>_TIMEOUT` | Any range, from a deployment's remote data service over REST (`doc/remote-data-integration-contract.md`) |

Point Peak frequency at the N44 recording, in the `server` environment in
`docker-compose.yml`:

```yaml
PEAK_FREQUENCY_DATA_CLIENTS: "peak_n44:islanding_stream.n44_client:N44RecordingClient"
PEAK_N44_MEASUREMENTS: "f"
```

Or at a deployment's own history service, in a Kubernetes env block:

```yaml
- name: PEAK_FREQUENCY_DATA_CLIENTS
  value: store:pswamp_core.datagateway.clients.remote_data:RemoteDataClient
- name: STORE_URL
  value: http://pmu-store.my-namespace:8100
```

Check such a service against the contract before relying on it:
`./scripts/check-remote-data-service.sh http://…`.

A worker needs `<APP>_DATA_CLIENTS` only when a module reads the gateway
itself in `setup`, as the explorer's row count does. Peak frequency reads
frames off its topic, so only the server needs it.

**A file from outside the image.** Mount it and point `<NAME>_PATH` at it.
`k8s/p-swamp-local.yaml` does this for the live feed: `LIVE_PATH` points at a
file mounted from a ConfigMap.

**Your own source.** Implement `DataClient`, prove it with
`DataClientConformance`, and name it in `<APP>_DATA_CLIENTS`.
`pmu_test_streamer/sample_client.py` is the model, and "Providers" in
`doc/server-data-architecture.md` has the contract.

**A real deployment.** `k8s/p-swamp-local.yaml` is the local worked example:
the image is built straight into minikube (`imagePullPolicy: Never`). A
deployment builds its image from the one `Dockerfile`, pushes it to its own
registry, and sets the same variables in its own manifests; this repo publishes
no image and deploys nothing. After editing the local manifest,
`./scripts/start-pswamp-in-local-minikube-cluster.sh` re-applies it and
restarts every pod.

**Where the module runs.** Name the family in `PSWAMP_WORKER_FAMILIES` on
exactly one worker, with one replica. Two hosts would each answer every frame.

## Q: My module is CPU-heavy. How do I stop it stalling everything?

`process` runs on the event loop of the process hosting it. In the module
worker, that loop also serves every other client's modules and the Kafka
consumer, so anything longer than a few milliseconds per frame delays all of
them, and the error tray reports "not keeping up".

1. Run the work in a pool, off the loop:

   ```python
   import asyncio
   import concurrent.futures

   _POOL = concurrent.futures.ThreadPoolExecutor(max_workers=2)

   async def process(self, frame: PmuFrame) -> PeakFrequency | None:
       return await asyncio.get_running_loop().run_in_executor(_POOL, analyse, frame.values)
   ```

   numpy and LAPACK release the GIL while they compute; pure-Python work needs
   a `ProcessPoolExecutor`. `mode_estimation/n4sid_module.py` does both,
   chosen by `MODE_ESTIMATION_EXECUTION`, and measures each.
2. Set `OPENBLAS_NUM_THREADS=1` in the process that hosts the module. By
   default OpenBLAS starts a thread per core on every call, which multiplies
   the CPU cost inside a pool.
3. Give the module its own worker. Copy `mode-estimation-worker` in
   `docker-compose.yml`, and its Deployment in `k8s/p-swamp-local.yaml`. Set
   `PSWAMP_WORKER_FAMILIES: "peak_frequency.family:FAMILY"` and a CPU limit,
   and remove the family from `module-worker`'s list. Then add the new
   Deployment to the `kubectl rollout restart` line in
   `start-pswamp-in-local-minikube-cluster.sh`.
4. Tune the input queue with class attributes on the module: `maxsize`
   (default 64), `overflow` (default `DROP_OLDEST`), and `keep_up` (when
   falling behind is reported).

## When it does not work

| Symptom | Cause |
|---|---|
| The page connects, `result` stays `null`, and no worker logs `module started` | The family is missing from `PSWAMP_WORKER_FAMILIES` |
| Results under compose, but none from `uv run src/server.py` or in tests | `lifespan` is not exported from `__init__.py` |
| The server fails at startup with "serves a WebSocket … but exports no WS_MESSAGE" | `WS_MESSAGE` is not exported from `__init__.py` |
| The compose build fails in `tsc` | The page does not match the regenerated contract |
| The container runs old code, or cannot find the new package | The server script was not restarted after adding a package |
| Every result arrives twice | Two workers host the family |
| A command's `POST` answers 404 | The client has no pipeline: open the page first |
| Error tray: `module peak-frequency failed on PmuFrame` | `process` raised; the traceback is in the worker log |
| Error tray: `peak-frequency is not keeping up with pmu.frame` | `process` is too slow for the frame rate: see the CPU-heavy recipe |
| A worker exits with code 2 | `PSWAMP_TRANSPORT` is unset, or no families are named; its log says which |
| Another app's data changed when you set a source's settings | Two apps use the same source name: rename yours |
| `check-generators.sh` fails after a change to the core or `shared.py` | The module templates no longer match: update `scripts/templates/module/` and this cookbook |

## What it cannot do yet

- A module runs only while a client has its app's socket open. There is no
  headless, always-on pipeline.
- Every client gets its own pipeline and module instance, even on a live feed.
  A single pipeline shared by every viewer of a stream is designed, not built.
- A family runs on one worker replica; a second replica would answer every
  frame twice.
- A module reads one input class. Chaining modules (one reading another's
  results) has not been tried, and there is no example or test of it.
