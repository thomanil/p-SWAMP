# The server data architecture

How PMU data moves from a data source to a browser in the p-SWAMP server, and
what each piece on the way is for. This is the durable description of the
architecture that landed with the PMU test streamer as its first slice. The
last section says what is still open.

The code is the `pswamp_core` package under `core/`, with pydantic as its only
dependency (plus aiokafka behind the `kafka` extra, and httpx beside it behind
the `timeseries` extra). The PMU test streamer
(`app/server-python/src/pmu_test_streamer/`, route `/pmu-test-streamer`) is
the worked example of every piece, and is what the snippets below are taken
from. "Adding things" at the end is the recipe for a module of your own and
the page that shows it; "Running a module as a separate service" is the
optional last step of that recipe, explained.

The document reads in two directions on purpose. **The building blocks** are
described from the data outward -- provider, gateway, player, bus, module,
pipeline, edge -- because that is the order in which each depends on the one
before. **The worked example** ("What happens when you click Live", below) is
walked the other way, from the button in the browser up to the two data
clients, because that is the order in which a request actually travels.

## The layers

The core is a stack of eight layers, numbered from the bottom, and the `L`
numbers in the pictures and sections below refer to it. **Each layer imports
only the ones below it**, and nothing above -- which is what lets a provider
be written outside the repo against L1 and L2 alone, and a module be moved to
another process without touching the layers either side of it.

| | Layer | What it is | Where |
|---|---|---|---|
| L1 | Messages | The wire models: `DataModel` and every message derived from it -- `PmuFrame` (carrying its `PmuHeader`), `Command`, `PlayerStatus`, `ResultEnvelope`, `ErrorEvent`. pydantic only; what every arrow carries | `messages/` |
| L2 | Gateway | The provider contract (`DataClient`, its capabilities) and the `DataGateway` that stitches providers into one time-addressed stream | `datagateway/` |
| L3 | Player | Paces a gateway stream and owns the transport controls: play, pause, step, seek, speed, replay, live | `datagateway/player.py` |
| L4 | Bus | In-process publish/subscribe typed on message classes; one per pipeline | `bus/` |
| L5 | Modules | Analysis: consume one message class off the bus, publish another | `modules.py`, `remote.py` |
| L6 | Pipeline | One stream's gateway, player, bus and modules as a unit, and the registry that keeps one per key | `pipeline.py` |
| L7 | Hosting | Which process each piece runs in, from the environment: providers and transports by name, a module as its own service | `transport/`, `datagateway/config.py`, `remote.main` |
| L8 | Web edge | The app package: POSTs that become commands, the socket that pushes state | `app/server-python/src/<app>/` |

Not every layer is a hop that data passes through. L1 is the vocabulary of
all the others; L6 is the box drawn around L2–L5; L7 is deployment. So the
pictures below, which follow a frame and a command, show rows only for L2,
L3, L4, L5 and L8.

## The idea in one picture

```
                    the repo's own providers, in one gateway:
                    the committed sample recording      the same rows re-stamped
                    (history)                           on the wall clock (live)
                              │                              │
                              ▼                              ▼
   L2  DataClient   ┌────────────────────────┐  ┌──────────────────────┐   capabilities declared:
                    │ SampleRecordingClient  │  │ LiveSyntheticClient  │   HISTORY_CONSUME, LIVE_CONSUME,
                    └───────────┬────────────┘  └──────────┬───────────┘   PRODUCE -- and routing
                                └──────────┬───────────────┘               honours them
                                           ▼
   L2  DataGateway         consume(PmuFrame, start, end)  → one stitched, time-addressed stream
                                         │                  (seek = a new stream from `start`;
                                         ▼                   a chunk = a bounded [start, end))
   L3  Player              replay: paces a bounded stream over the history; pause · step · seek · loop
                           live:   an open stream from now, as it arrives; no transport
                                         │  publishes each PmuFrame, and PlayerStatus on every change
                                         ▼
   L4  InProcessBus        publish/subscribe typed on message classes; one per pipeline
                            │            │              ▲
                            │            ▼              │ publishes FrameStatsResult
                            │   L5  FrameStatsModule ───┘   (consumes PmuFrame)
                            ▼
   L8  WebSocket endpoint  subscribes PmuFrame · PlayerStatus · FrameStatsResult
                            → one PmuStreamState per change, down the socket
                                         │
                                         ▼
                                     browser

   POST /api/pmu-test-streamer/playback/seek ──► Command on the bus ──► Player.apply()
   POST /api/pmu-test-streamer/playback/live ──► Command on the bus ──► Player.go_live()
```

Two things to notice. **Every arrow carries a pydantic model** (`PmuFrame`,
`PlayerStatus`, `Command`, …) -- that is L1: the wire format is JSON with a
schema version, end to end, and the browser's TypeScript types are generated
from those same classes. And **nothing above the bus knows what is below it**: the endpoint
subscribes to message classes; the module subscribes to message classes; the
player writes to the bus. Swapping a provider (top row) changes nothing else. A
deployment's own provider -- a TSO's time-series store, a broker feed -- takes
the place of either of the two shown.

A third: L5 is the one box that can leave the process. In the compose and
minikube stacks it does -- the module runs in the `stats-worker` container,
reached over Kafka topics -- and nothing else in the picture changes. See
"Running a module as a separate service".

## The building blocks

### Messages — `pswamp_core.messages`

*What.* Every message that crosses a topic, a socket or a process boundary is a
`DataModel`: a pydantic model with a schema `version` (pinned per subclass), an
`mRID` identity, a UTC `timestamp`, and a **topic derived from the class name**.

```python
from typing import Literal
from pswamp_core.messages import DataModel

class LineTrip(DataModel):            # topic: "line.trip"
    version: Literal["v1"] = "v1"
    timestamp: datetime               # required: the event's own time
    line: str

LineTrip.topic                        # "line.trip"
LineTrip.model_validate_json(text)    # the whole codec; a "v2" payload fails here
```

*Why.* No pickle and no numpy on the wire means a message can be logged,
inspected, validated on receipt, and published into the browser contract
without an adapter. The set of `DataModel` subclasses *is* the topic catalogue,
with the schema attached to each entry.

*Where.* `core/src/pswamp_core/messages/`: `data_model.py` (the base),
`pmu.py` (measurements), `results.py` (what modules emit), `control.py`
(commands and player state).

The measurement shape is **one instant of every channel, carrying its layout**:

```python
PmuFrame    # per instant: timestamp, mRID, header, values: list[float | None]  (NaN is null on the wire)
PmuHeader   # nested in every frame: station / channel / measurement / units per column, data_rate;
            # header_id is a content hash, computed and cached, so a consumer can spot a change cheaply
```

`PmuHeader` is the config-frame analogue; `header.columns(measurement="f")`
is the query p-SWAMP's applications already make of a labelled window. It
rides inside every frame. That repeats ~1 KB per frame for the sample (about
3.4x a bare frame before compression, ~1.2x after, since a broker's batch
compression collapses the repeats), and it is what makes any single frame
enough to work from: a module reads the layout off the frame it is
processing, a worker that starts late is primed by its first input, a live
source describes itself, and a changed layout is simply the next frame's
header. The measurement behind the choice is in the docstring of
`messages/pmu.py`.

### Providers — `pswamp_core.datagateway.DataClient`

*What.* The contract a data source implements. Three abstract methods and a
declaration of what the source can do:

```python
from pswamp_core.datagateway import Capability, Coverage, DataClient, TimeRange

class SampleRecordingClient(DataClient):
    capabilities = Capability.HISTORY_CONSUME          # a file cannot tail live data

    async def coverage(self, model, mRID=None) -> Coverage | None:
        return Coverage(TimeRange(first_frame.timestamp, last_frame.timestamp + interval))

    async def consume(self, model, time_range, mRID=None):   # an async iterator
        for frame in self.frames:
            if time_range.contains(frame.timestamp):
                yield frame

    async def produce(self, data): raise TypeError("read-only")
```

*Why.* Capabilities replace the old nine-method duck type where half the
implementations of `seek` were `pass`: the core never asks a provider for what
it did not declare -- and the planner enforces it: a history segment goes only
to a `HISTORY_CONSUME` client, the live hand-off only to a `LIVE_CONSUME` one,
and a client that can only tail is offered only from the hand-off margin on.
`coverage` is re-asked on every call because most real stores have now-relative
windows. **History lives with the provider** — the repo persists nothing; a
TSO's time-series database is a `DataClient`, and that is how "no database in
the repo" and "navigate history" coexist.

*Where.* The contract: `core/src/pswamp_core/datagateway/data_client_model.py`.
The reference client (`InMemoryClient`): `datagateway/clients/in_memory.py`. Two
providers written *outside* the core, as a TSO's would be, both under
`app/server-python/src/pmu_test_streamer/`: `sample_client.py` (the recording:
history) and `live_client.py` (a synthetic live feed: `LIVE_CONSUME` only, the
recording's rows re-stamped on the wall clock at 20 Hz). Both serve `PmuFrame`
and nothing else; each frame carries its layout, so either one on its own is
enough for a page to render a table.

A provider proves itself with the **conformance suite** — inherit it, supply
three fixtures. The cases follow the declared capabilities: the history cases
skip for a tail-only client, whose `conformance_records` is `[]`, and the live
cases skip for a history-only one:

```python
from pswamp_core.datagateway.conformance import DataClientConformance

class TestMyClient(DataClientConformance):
    @pytest.fixture
    def client_under_test(self): return MyClient("mine")
    @pytest.fixture
    def conformance_model(self): return PmuFrame
    @pytest.fixture
    def conformance_records(self, client_under_test): return list(client_under_test.frames)
```

And it is **configured, and chosen, from the environment**, never from code in
the repo:

```
PSWAMP_DATA_CLIENTS="live:acme_tso.pmu:KafkaFeed,history:acme_tso.pmu:TimescaleClient"
HISTORY_DSN=postgres://…          # each client reads its own {NAME}_{SETTING} block
```

The local k8s manifest (`k8s/p-swamp-local.yaml`) is the worked example of
this with the repo's own providers: it names both in `PSWAMP_DATA_CLIENTS` and
sets `LIVE_PATH` to a data file mounted from a ConfigMap
(`k8s/deployment_pmu_data_file_example.txt`, every value counting up by one per
frame), so the live feed in that deployment visibly comes from outside the image.

```python
gateway = gateway_from_env(
    default="sample:pmu_test_streamer.sample_client:SampleRecordingClient,"
            "live:pmu_test_streamer.live_client:LiveSyntheticClient"
)
MyClient.show_config("history")   # prints the variables a deployment must set
```

### The gateway — `pswamp_core.datagateway.DataGateway`

*What.* One object over any number of providers, with two verbs:

```python
gateway.consume(PmuFrame, start=t0, end=None)    # a seek: from t0, onwards
gateway.consume(PmuFrame, start=t0, end=t1)      # a chunk: exactly [t0, t1)
await gateway.produce(message)                   # to every client that can store it
```

*Why.* "Jump to a time" and "query a chunk" are the same call, which is why they
are provider capabilities and not bus features. Behind `consume`, a
`SegmentPlanner` picks, one segment at a time, whichever client covers the
cursor (highest priority wins, coverage re-asked at every boundary), and a
`DataStream` stitches the segments into one iterator with a watermark that
drops duplicates across overlapping sources. A history store can carry a replay
right up to now and hand over to a live source only then. This layer is Louis
Pauchet's `test_pswamp` draft, lifted.

*Where.* `datagateway/data_gateway.py`, `planner.py`, `stream.py`,
`time_range.py`.

### The player — `pswamp_core.datagateway.Player`

*What.* Paces a gateway stream and owns the replay controls.

```python
player = Player(gateway, bus, model=PmuFrame, speed=1.0, loop=True)
await player.start()          # opens gateway.consume(PmuFrame, start, history_end); paused
player.resume(); player.pause(); player.set_speed(2.0)
await player.step(-1)         # one frame back
await player.seek(t)          # a NEW stream from t, announced as StreamChanged
await player.go_live()        # a NEW, open-ended stream from now; mode "live"
await player.replay()         # back to the recording's start, paused; mode "replay"
await player.replay(t0, t1)   # a BOUNDED replay of [t0, t1): ends paused at t1, never loops
player.status()               # PlayerStatus: mode, cursor, speed, paused, can_seek, can_go_live, coverage, range_end, error…
```

A bounded replay (the `replay` verb with `end`, and `play: true` to start it)
ends paused at `end` even on a looping player. A provider that raises mid-stream
-- or whose coverage call fails, at start or later -- ends the stream paused
with `PlayerStatus.error` set to the client's own error and an `ErrorEvent` on
the bus naming that client; the pipeline still starts, so the page connects and
shows why. The `refresh` verb asks the gateway again; a play or seek that finds
the source clears the error.

*Why.* The gateway yields as fast as the provider reads; a human watching a
disturbance needs real time, and needs to scrub. Three decisions live here so
they are made once: **seek is a new stream** (a stream's watermark rightly
refuses to go backwards); **mode is which stream is open** -- a *replay* is a
bounded stream over the history coverage, paced, looping at its end; *live* is
an open-ended stream from now, delivered as it arrives, with no transport
controls -- and nothing switches on its own, so an archive beside a live feed
replays paced and seekable (`can_seek`) and offers the switch (`can_go_live`),
and a page renders no dead buttons; and pacing **drops time rather than
bursting** when the loop falls behind. The player takes its commands **from
the bus** (below), so a POST, a test and a future Qt widget drive it the same
way. One more, learned the hard way: the read of the next frame is a task the
player awaits *outside* its lock, so a live feed that has gone quiet never
blocks the switch back to the replay.

*Where.* `datagateway/player.py`.

### The bus — `pswamp_core.bus.InProcessBus`

*What.* Publish/subscribe inside one process, typed on message classes. A
subscription to a base class receives every subclass.

```python
bus = InProcessBus()
with bus.subscribe(PmuFrame, PlayerStatus, overflow=Overflow.DROP_OLDEST, maxsize=64) as sub:
    async for message in sub: ...
bus.add_listener(ResultEnvelope, store.remember)   # synchronous, every message, in order
bus.publish(frame)                                  # from the loop
bus.publish_threadsafe(result)                      # from a thread: the ONE crossing point
```

*Why.* It is what decouples the player from the endpoint and the module from
both. The overflow policy is per subscription because a live stream and a
completeness-first replay want different answers to "the consumer is slow":
`DROP_OLDEST` for frames, `LATEST_ONLY` for state, `GROW` for commands.
`publish_threadsafe` is the seam through which the desktop package's blocking
application threads will publish when they are bridged in; it exists now so a
second mechanism never appears.

*Where.* `core/src/pswamp_core/bus/__init__.py`. `Latest` in the same module
keeps the newest message per class — what a freshly connected socket renders
from.

### Modules — `pswamp_core.modules.Module`

*What.* Consume one message class, publish another.

```python
class FrameStats(BaseModel): mean_frequency_hz: float | None; ...
class FrameStatsResult(ResultEnvelope[FrameStats]):     # topic: frame.stats.result
    version: Literal["v1"] = "v1"

class FrameStatsModule(Module):
    name = "frame-stats"
    input_model = PmuFrame
    output_model = FrameStatsResult
    async def process(self, frame: PmuFrame) -> FrameStats | None:
        ...
```

*Why.* A contributor's module is the analysis and two class attributes; `run`
subscribes, calls, wraps in the envelope (`timestamp`, `app` identity,
`parameters`, `request_id`) and publishes. The page that shows it subscribes to
`FrameStatsResult`, never to the module. This is the coroutine module; the
desktop package's thread-based `SnapshotApp`s are bridged to the same bus and
the same envelope when `GatewayIO` lands (deferred; see the last section).

*Where.* `core/src/pswamp_core/modules.py`;
`app/server-python/src/pmu_test_streamer/stats_module.py` (a module that
*reduces* a frame; it derives its column indexes from `frame.header` on the
first frame and again whenever a frame's `header_id` differs, so it has no
`setup`, and the same instance runs unchanged in another process).

### Pipeline and registry — `pswamp_core.pipeline`

*What.* One stream's gateway, bus, player and modules, and the registry that
builds one per key, caps them and evicts them.

```python
async def build_pipeline(client_id: str) -> Pipeline:
    gateway = gateway_from_env(DEFAULT_DATA_CLIENTS)
    bus = InProcessBus()
    return Pipeline(client_id, gateway, bus, Player(gateway, bus, loop=True), [FrameStatsModule()])

REGISTRY = PipelineRegistry(build_pipeline, max_pipelines=8, idle_seconds=300)
pipeline = await REGISTRY.acquire(client_id)   # builds on first connect; one per key
REGISTRY.peek(client_id)                       # a command's view: never builds
```

*Why.* The key is the unit of isolation, and **everything inside a pipeline is
built fresh for its key** -- the factory above runs once per client id and
constructs a new gateway (new client instances), a new bus, a new player and
new module instances. A **replay is keyed per client**: a visitor exploring
recorded data wants their own clock. A **live stream would be keyed per
stream**: every operator sees the same instant and the analysis runs once.
Same class, different key. The registry is the grid monitor's `HubRegistry`
moved down and generalised: per-key lock so five simultaneous sockets build one
pipeline, idle grace so a reload rejoins, LRU eviction at the cap, refusal when
nothing is reclaimable. "What is per client, what is shared", below, spells
out what that means in objects.

*Where.* `core/src/pswamp_core/pipeline.py`.

### The web edge — the app package

*What.* What is left in `pmu_test_streamer/api.py` once the core does the rest:
which messages to forward down the socket, and which POSTs become which
`Command`. The browser contract is unchanged: commands up as `POST`, state
down one socket, an acknowledgement that never carries state, everything
generated into `doc/api/openapi.json`.

*Where.* `app/server-python/src/pmu_test_streamer/api.py`; the page in
`app/client-web/src/pages/pmu-test-streamer/`. The push loop a page needs is
the grid monitor's, `event_queue` + `serve_updates` in `pswamp_web/pump.py`
(re-exported by `shared.py`), which serves a core pipeline unchanged. The
client-id handshake is not yet shared; see "Adding things".

## How data flows: source to browser

```
 1  sample_data.txt          parsed once, lazily, into 60 PmuFrame (20 Hz, 5 stations), each carrying the one PmuHeader
        │
 2  SampleRecordingClient    coverage() = [first frame, last frame + 50 ms)   capabilities = HISTORY_CONSUME
    LiveSyntheticClient      coverage() = [now - 50 ms, ∞) live                capabilities = LIVE_CONSUME
        │
 3  DataGateway.consume(PmuFrame, start, history_end)        ← replay: bounded to the history
        │                    planner: one segment, the recording, not live; the live client is
        │                             offered only from now - 5 s on, so it never enters a replay
        │                    stream:  frames in order, watermark, closed when the segment ends
    DataGateway.consume(PmuFrame, now, None)                 ← live: open-ended, after `go_live()`
        │                    planner: one live segment on the live client, tailed as frames tick
 4  Player                   replay: waits until each frame is due at `speed`, then bus.publish(frame)
        │                            on stream end: loop → a new consume() from the history start
        │                    live:   publishes each frame as it arrives; pause/step/seek/speed refused
        ├──────────────────────────────┐
 5  InProcessBus                       │
        │                              ▼
        │                    FrameStatsModule.process(frame) → bus.publish(FrameStatsResult)
        ▼
 6  ws endpoint              subscribe(PmuFrame, PlayerStatus, FrameStatsResult, StreamChanged)
                             wake → drain what else is pending → ONE PmuStreamState → send_state(ws)
        │
 7  browser                  useServerSocket → usePmuStreamSocket → FrameTable + controls
```

Step 6 coalesces: each message is the frame at the cursor (with its layout
inside), the player's status and the latest module result. A slow socket sees
the newest state, never a backlog. The page keeps the last layout it saw, so
the table stays laid out while no frame is at the cursor (a replay paused at
its start after a stream switch).

## What happens when you click Live: the chain, walked from the browser up

The concrete chain, on the PMU test streamer, for one command. "Live" is the
example because it is the one that changes the most; the other seven commands
(`replay`, `play`, `stop`, `forward`, `back`, `seek`, `speed`) travel exactly the
same path and differ only in what the player does at step 6. Each step names the
code that does it.

**1. The button.** `PmuTestStreamerPage.tsx` renders the Recorded | Live switch
from the last `PmuStreamState` it received: `Live` is enabled when
`player.can_go_live` is true and pressed when `player.mode === "live"`. Its
`onClick` calls `goLive()` from the page's own hook, and only when the page is
not already live -- the page never fires a command that would change nothing.

**2. The hook.** `usePmuStreamSocket.ts` has one `useCallback` per command.
`goLive` is `fireCommand('pmu-test-streamer', postCommand('/api/pmu-test-streamer/playback/live'))`.
`postCommand` (`src/lib/commands.ts`) is typed against the generated
`schema.ts`, so a wrong path is a `tsc` error; it appends `?client_id=` from
`src/lib/clientId.ts` (one id per browser profile, in `localStorage`) and
resolves the url against the serving origin (`src/lib/servers.ts`). Under Vite
that is the dev server, whose `/api` proxy forwards to the compose server; in
the image it is the server itself. `fireCommand` awaits the POST and logs a
failure. Nothing on the page waits for the reply: the effect arrives on the
socket like any other change.

**3. The route.** `server.py` mounts the streamer's `router` under
`/api/pmu-test-streamer`. The handler `live()` in `pmu_test_streamer/api.py` is
one line: `return await dispatch(client_id, "live")`.

**4. Dispatch: find, check, publish, acknowledge.** `dispatch()` in `api.py`:

- `live_pipeline(client_id)` asks `REGISTRY.peek()` for this client's pipeline.
  A command never *builds* one -- no page open, no pipeline, 404.
- `refusal(pipeline.player.status(), "live")` asks whether the player's current
  mode can apply the verb. `live` without a live source, or any transport verb
  while live, is a **409** with the reason, and nothing is published.
- Otherwise it builds a `Command(client_id=…, verb="live", args={})` -- which
  generates a `request_id` -- publishes it on **that pipeline's bus**, logs it
  with the roster, and returns `CommandAck(applied="live")`. The ack means
  *dispatched*: the player has not run yet.

**5. The bus.** `InProcessBus.publish` delivers the `Command` synchronously to
every subscription whose class matches. The only subscriber to `Command` is
the player's command task, subscribed with `Overflow.GROW` so no command is
ever dropped. The bus is per pipeline, so the command reaches this client's
player and no other.

**6. The player applies it.** `Player._commands` (`core/…/datagateway/player.py`)
reads the command off its subscription and calls `apply()`, which maps the
verb: `live` → `go_live()`, `replay` → `replay()`, `play` → `resume()`, and so
on. A `PlayerError` here (a refusal the edge's check could not see) is logged
and dropped.

**7. The switch.** `go_live()` takes the player's lock and calls
`_switch_stream(utcnow(), live=True)`; `replay()` calls
`_switch_stream(history_start, live=False)`. This is the one place a stream is
replaced, and it does five things in order: bump the generation (so a frame the
run loop had parked from the old stream is discarded); cancel the read task in
flight on the old stream; close the old `DataStream`; re-read what the gateway
offers (`coverage(model, capability=HISTORY_CONSUME)` for the seekable range,
`supports(model, LIVE_CONSUME)` for whether live exists); and open the new one
with `gateway.consume(model, start, end)` -- `end` is the history's end for a
replay, so it runs out and loops, and `None` for live, so it never ends. Then it
sets the `_live` flag, which is what `mode` reports, and publishes
`StreamChanged`. Back in `go_live()` the player unpauses (live has no pause);
in `replay()` it pauses (the same state the page opens in). Both publish a
`PlayerStatus`.

**8. The gateway plans a segment.** `gateway.consume()` returned a `DataStream`
without touching any client; the player's next read (a task, awaited outside
the lock) is what drives it. `DataStream._iterate` asks `SegmentPlanner.next_segment`
for the segment at the cursor. The planner calls `coverage()` on every
consuming client and keeps the offers that contain the cursor, honouring what
each client declared:

- `SampleRecordingClient.coverage()` is the fixed window
  `[2026-01-01 00:00:00.050, +3.0 s)`; a `HISTORY_CONSUME` client, so it may
  serve a history segment.
- `LiveSyntheticClient.coverage()` is `[now - 50 ms, ∞) live`; a `LIVE_CONSUME`
  -only client, so it is *offered* only from `now - 5 s` (the hand-off margin)
  on, and *eligible* only for the live hand-off.

For **live**, the cursor is `now`: only the live client covers it, the hand-off
applies, and the planner returns one open-ended live `Segment` on it. For
**replay**, the cursor is the recording's first instant: only the recording
covers it, and the planner returns one history `Segment` bounded to the
recording's end. The two clients never compete, and neither is ever asked for
what it did not declare.

**9. The data client serves it.** The stream calls the chosen client's
`consume(PmuFrame, segment.range)`:

- `LiveSyntheticClient.consume` registers an `asyncio.Queue` in its `_tails`
  and yields whatever the ticker puts there. The ticker has been running since
  `Pipeline.start` called `gateway.open()`; every 50 ms it takes the next row of
  the recording, stamps it `utcnow()`, and puts it on every tailing queue. The
  queue is removed in `finally`, so a switch away stops the delivery.
- `SampleRecordingClient.consume` iterates its sixty parsed frames and yields
  those inside the range. The frames were parsed once, lazily, from
  `sample_data.txt`, and each carries the recording's header.

That is the top of the chain. From here everything runs **back down**, and it
is the same path for both sources: the read task parks the frame; the run loop
paces it (replay) or passes it straight through (live) and `bus.publish`es it;
`FrameStatsModule` and the socket's subscription both receive it;
`FrameStatsModule` publishes a `FrameStatsResult`; the endpoint's push task
wakes, drains what else is pending, builds one `PmuStreamState` from the
player's status and the bus's latest frame and result, and `send_state`s it.
The page renders that message -- the badge turns red, the transport row
disables, the readout shows a wall-clock time -- because the *server* said the
mode is live, not because a button was pressed.

```
 browser   Live button → goLive() → POST /api/pmu-test-streamer/playback/live?client_id=<id>
 api.py    live() → dispatch(): REGISTRY.peek (404) · refusal() (409) · bus.publish(Command) · CommandAck
 bus       Command → the player's command subscription (this pipeline only)
 player    _commands → apply("live") → go_live() → _switch_stream(now, live=True)
                                                      generation++ · cancel read · close stream ·
                                                      re-read coverage · gateway.consume(PmuFrame, now, None)
                                                      _live = True · StreamChanged · PlayerStatus
 gateway   DataStream → SegmentPlanner: coverage() of each client → one live Segment on LiveSyntheticClient
 client    LiveSyntheticClient.consume: a queue in _tails, fed by the 20 Hz ticker
   ───────────────────────────── and back down ─────────────────────────────
 player    read task parks the frame → run loop publishes it (unpaced: live)
 bus       PmuFrame → FrameStatsModule (→ FrameStatsResult) and the socket's subscription
 endpoint  one PmuStreamState → send_state(ws)
 browser   renders mode "live": red badge, transport disabled, wall-clock readout
```

The command carries a `request_id`, generated on the server and logged with
the verb; a module answering a command copies it onto its `ResultEnvelope`, so a
result on a shared bus can be routed back to the client that asked. (Returning
it to the browser in the acknowledgement is the one edge change still pending;
see the last section.)

## What is per client, what is shared

| | in the streamer today (per client, replay or live) | for a shared live feed (designed, not yet built) |
|---|---|---|
| pipeline key | the browser's client id | the stream name |
| provider | the recording plus the synthetic live feed, opened per pipeline | the deployment's live and history clients |
| player | one per client: own cursor, own speed, own recorded/live switch; live has no transport | one per stream, always live; "pause" is view state |
| modules | run once per pipeline | run once per stream, results shared |
| bus | one per pipeline | one per stream |
| socket | one per page per client | one per page per client, subscribed to the shared bus |

### One gateway per client, concretely

"One pipeline per client" is easy to read as one *player* per client over some
shared plumbing. It is not: the gateway, and the data clients inside it, are
per client too. When the registry first sees a client id it calls
`build_pipeline(client_id)`, and that call runs `gateway_from_env(...)`, which
**instantiates the clients named in the spec** -- a new `SampleRecordingClient`
and a new `LiveSyntheticClient` -- and wraps them in a new `DataGateway`. Then
`Pipeline.start` calls `gateway.open()`, which is where this client's live
ticker starts. Eight browsers in live mode are eight tickers; evicting a
pipeline calls `gateway.close()` and stops that one.

What is actually shared across pipelines is exactly one thing: the **parsed
recording**. `load_sample()` is `lru_cache`d by path, so the sixty frames (and
the one header object they all point at) are read from `sample_data.txt` once per process and every
`SampleRecordingClient` holds the same frozen objects. That is safe because
nothing writes to them; the live client copies each row's `values` before
stamping it. The registry itself is shared, of course -- it is the one map from
client id to pipeline -- and so is the process's event loop.

Why not one gateway for all clients, with a player each? Because the gateway
is where the *provider's* state lives, and that state is per consumer as soon
as anything tails: a live client holds one queue per open `consume()`, a broker
client would hold one subscription per consumer, a database client one cursor.
A gateway shared by eight players would need to know about eight consumers, and
it does not -- the `DataClient` contract has no consumer identity in it, on
purpose, so that a provider stays a simple thing to write. Keeping the gateway
inside the pipeline keeps every provider single-consumer.

The cost is what the registry caps. A streamer pipeline is a handful of
`asyncio` tasks (the player's run, command and read tasks, one per module, the
live ticker) and the objects above; there are no threads and no copies of the
recording, so it is cheap next to the grid monitor's four-thread hubs.
`MAX_PIPELINES` and `IDLE_EVICT_SECONDS` in `api.py` are the bounds, and the
registry's tests pin them. Every socket a browser opens carries the same
`client_id`, so however many pages one browser has open, they land on that one
pipeline; a *different* browser is a different id and a different pipeline,
which is why two browsers in recorded mode can sit at different frames and two
in live mode -- with this synthetic feed -- see different ticks.

That last point is the honest limit. The live feed in the streamer is per
client because the gateway is, which is right for a synthetic source and wrong
for a real one, where every viewer must see the same instant and the analysis
must run once. That is the right-hand column of the table: one pipeline keyed
by the *stream*, its gateway and live client shared by every viewer, with only
the replay cursors and the view state per client. It is still a design.

## Adding things

**A provider** (a TSO's data source, or a new recording format): implement
`DataClient` in your own package, importing `pswamp_core.datagateway` and
`pswamp_core.messages` only; declare `env_settings`; inherit
`DataClientConformance` in a test with your three fixtures; name the class in
`PSWAMP_DATA_CLIENTS`. Nothing in this repo changes. `sample_client.py` is the
model.

**A module, and the page that shows it**: one app package with its own
pipeline. The streamer is that shape with more in it, so each step names the
piece of `pmu_test_streamer/` to copy.

1. `./scripts/generate-new-subapp.sh my-thing "My Thing"` for the folders and
   the four registry entries; replace the counter it writes, delete its
   `model.py` (and the `*_API_PATH` const if the page has no commands).
2. **The module**, in its own file: a pydantic result body, the envelope
   (`class MyThingResult(ResultEnvelope[MyThingBody])`, topic `my.thing.result`),
   and a `Module` subclass with `name`, `input_model`, `output_model` and
   `process`. Read the layout off `frame.header` when it matters, re-deriving
   on a changed `header_id` (`stats_module.py` does); `setup` only for
   something a module needs from the gateway itself (`row_count_module.py`
   keeps the gateway). Import only `pswamp_core`.
3. **The pipeline**, copied from the streamer's `build_pipeline`, `REGISTRY`
   and `lifespan`. The module list there is the only registry a module has.
   Name the provider in the `gateway_from_env` spec string and give the app its
   own `variable=`; don't import `pmu_test_streamer`. A page with no transport
   wants `Player(..., autoplay=True, loop=True)`, or it shows dashes for ever.
4. **The socket**: a state model exported as `WS_MESSAGE`, carrying the
   envelope as it is (and the frame at the cursor, if the page shows one --
   its layout comes inside it); `state_message` reading
   `pipeline.latest.get(MyThingResult)`; and the grid monitor's push loop
   from `shared.py`, which serves a core bus unchanged:

   ```python
   with event_queue(pipeline.bus, MyThingResult) as updates:
       await serve_updates(ws, updates, lambda event: state_message(pipeline))
   ```

   Open the queue before building the opening message. The handshake around
   it (`read_client_id`, `accept`, `REGISTRY.acquire`, 1013 on
   `CapacityError`, `release`) is not shared yet -- copy the streamer's
   `connected_pipeline` and point it at your registry.
5. **Commands**, if any: `POST`s that `REGISTRY.peek` (404 if none) and
   `bus.publish(Command(...))`; `dispatch` in the streamer is the model.
6. **The page**: `useServerSocket<Wire['MyThingState']>(MY_THING_WS_PATH)`;
   the component reads the module's fields as written.
7. `generate-api-contract.sh`, `error_check.sh`,
   `run-python-server-tests.sh`; restart the server script (a new package
   needs the rebuild). Tests: call `process` directly, and run the pipeline
   with `player.paced = False` reading the bus.
8. **Optional: run the module as its own service.** In-process is the default
   and costs nothing; if you need this, four edits, none to the module (its
   input carries everything it needs, as `stats_module.py` shows):
   - `build_pipeline`: `RemoteModule(MyThingModule, transport, key)` where
     `MyThingModule()` was, with the transport from
     `transport_from_env("MY_THING_MODULE_TRANSPORT")` built once per process
     and closed in `lifespan` (`stats_modules` / `module_transport` in the
     streamer's `api.py`); unset, the list holds the module itself;
   - `worker.py`: `raise SystemExit(main(MyThingModule, "MY_THING_MODULE_TRANSPORT"))`
     from `pswamp_core.remote`;
   - the variable on both sides in compose and `k8s/`, and a worker service
     that is the same image with that command (copy `stats-worker`);
   - a test running `ModuleHost(MyThingModule, broker)` beside the pipeline
     over `InMemoryTransport` (copy the streamer's).

**If the new module publishes a new data shape/type.** Nothing in `core/`
changes: the bus, `Latest`, the gateway and the player are all typed on
whatever `DataModel` subclass you hand them (the core's own tests run the
chain on a non-PMU `Measurement`). What has to be done:

- **Declare it once, as a `DataModel` subclass, where every consumer can
  import it.** The class *is* the type and the topic -- the bus routes by
  `isinstance`, so the module and whatever reads its output must import the
  same class, not two look-alikes. Consumed only inside the app package: beside
  the module, as `FrameStatsResult` is in `stats_module.py`. Consumed by
  another app package, a provider outside the repo, or the desktop package:
  in `core/src/pswamp_core/messages/`, so importing it needs nothing but the
  core. Never import another app package to get at its types.
- Pin `version: Literal["v1"]`; give it a `timestamp` if a player will pace it
  or an envelope will carry it.
- A module's output is always `ResultEnvelope[YourBody]`; the payload sits
  under `.result`, and a downstream module subscribes to the envelope class.
  A stage that must publish a *bare* message (a cleaned `PmuFrame` for the
  existing PMU modules to consume as if from a source) has no `Module` path
  today: it needs its own `run` calling `bus.publish`, or a decision to give
  `Module` one.
- If the type is an *input* rather than a result, something has to serve it:
  a provider answering `coverage`/`consume` for that class, and a
  `Player(model=YourType)` in the pipeline. One player streams one class.
- A `PmuHeader` is the PMU stream's layout, not the core's: a new type has no
  header unless you nest one in it, as `PmuFrame` does, and the page sends
  what it needs.
- Regenerate the contract; the browser type follows from the state model.

Don't add the module to the *streamer's* pipeline and put the page in
another package: two apps sharing a pipeline is the right-hand column of the
table above, a decision about the key, not a shortcut.

## Running a module as a separate service

*What.* The module's slot in the pipeline's module list is taken by a stand-in
that carries the module's input class out to a broker topic and its result
class back; a worker process runs the real module, one instance per pipeline
key. Nothing in the module changes, and nothing above the bus notices.

```
 web process (one pipeline per client)                     stats-worker process (one per deployment)
 gateway ─ Player ─▶ InProcessBus                          KafkaTransport: one consumer per topic,
              │ RemoteModule(FrameStatsModule, key=<client id>)           demultiplexed by record key
              │   outbox: PmuFrame  ──publish key=<id>──▶ pmu.frame ──────────────┬─▶ ModuleHost(FrameStatsModule)
              ◀── subscribe key=<id> ◀──── frame.stats.result ◀───────────────────┘     one module + bus per key
 the socket subscribes FrameStatsResult as before;  POST ─▶ Command ─▶ Player, unchanged
```

`RemoteModule(FrameStatsModule, transport, key)` has the module's `name`,
`input_model` and `output_model`. Its `run` drains the input class off the
local bus onto its topic (a `DROP_OLDEST` subscription, so a slow broker costs
frames, not memory) and publishes what arrives on the result topic back onto
the bus. `ModuleHost(FrameStatsModule,
transport)` subscribes to the input topic across every key; the first input for
a key builds that key's bus, module and forwarder, and a key idle for
`idle_seconds` is evicted. One variable, read by both sides, is the whole
switch:

```
PMU_TEST_STREAMER_MODULE_TRANSPORT=kafka:pswamp_core.transport.kafka:KafkaTransport
KAFKA_BOOTSTRAP_SERVERS=kafka:9092                # KAFKA_TOPIC_PREFIX optional
```

Unset, the module runs in-process; `mem:pswamp_core.transport:InMemoryTransport`
is the portless value every test uses. Compose runs `kafka` (the Apache image,
one KRaft node, no volume), the server and `stats-worker`; `k8s/` has the
matching three Deployments; the smoke test plays the streamer and waits for the
module's result either way.

*Why.* Three decisions:

- **The hop is a transport, not a provider.** An earlier sketch had the broker
  as a bus via `gateway.produce`/`consume`. The streamer's replay frames are stamped
  in January and go *backwards* at every loop; a time-addressed `DataStream`
  would drop them. A `Transport` carries what the bus said, in order, and
  never reads a timestamp. A broker as a *source* is still a `DataClient`.
- **The pipeline key is the record key.** One topic per class, never per
  client, so eight pipelines are three topics and the worker tells them apart
  by key. No `DataModel` gains a field (ADR-005), and the streamer stays per
  client with every command intact, since the player never leaves the server.
- **The input is all the worker needs.** A frame carries its layout, so a
  worker that starts late -- or a key that was evicted and rebuilt -- is primed
  by the first frame it sees, and a changed layout is just the next frame. The
  price is the layout repeated in every record, about 1.2x on the wire once
  the broker's batch compression has collapsed it.

This landed ahead of the measurements the design asked for before adding a
broker; the numbers to produce next are listed in the last section.

*Where.* `core/src/pswamp_core/transport/` (`Transport`, `InMemoryTransport`,
`transport_from_env`; `kafka.py`), `core/src/pswamp_core/remote.py`
(`RemoteModule`, `ModuleHost`, `main`); the streamer's `stats_modules`,
`module_transport` and `worker.py`; `kafka` and `stats-worker` in
`docker-compose.yml` and `k8s/p-swamp-local.yaml`; `core/tests/test_remote.py`
and the last cases of `app/server-python/tests/test_pmu_test_streamer.py`.

## A remote time-series store as a provider

*What.* `TimeSeriesDatabaseClient` (`datagateway/clients/time_series_database.py`,
the `pswamp-core[timeseries]` extra) is a `DataClient` over a deployment's own
time-series database, reached through a small REST api in front of it. A range
query goes up as `POST /v1/queries`; the records come back as `TimeSeriesResult`
envelopes on a Kafka topic, keyed by the query's id, closed by an `end` (or
`error`) envelope; coverage is `GET /v1/coverage`. **The contract is the class's
docstring.** Configuration is the `TSDB_*` block.

```
 gateway.consume(PmuFrame, t0, t1)
   └─ TimeSeriesDatabaseClient ── subscribe topic (key = query_id) ── POST /v1/queries ──▶ the store's api
                                ◀── time.series.result envelopes … {kind: "end"} ◀─────── its database
```

*Why.* Results are messages end to end, and the store's owner can put anything
behind the POST. The price is correlation -- a `query_id` on every envelope, an
explicit `end`, an `error` envelope, subscribe-before-POST -- paid once, in the
client. The dummy service `app/server-python/src/time_series_stub/` (the sample
recording tiled to a minute, `python -m time_series_stub`) stands in for a
deployment's api in compose and k8s, so the whole path runs from this repo.
`/time-series-explorer` drives it two ways: **play-range** (the player's bounded
replay, above) and **count** (`RowCountModule`, the first `Command` addressed to
a module, `target="row-count"`). Open points are in the last section.

*Where.* `messages/time_series.py`, `datagateway/clients/time_series_database.py`,
`transport/kafka.py:create_topic`; `time_series_stub/`, `time_series_explorer/`;
`time-series-stub` in `docker-compose.yml` and `k8s/p-swamp-local.yaml`. Tests:
`tests/test_time_series_database_client.py` runs the conformance suite over the
client wired to the stub in-process (`httpx.ASGITransport`, `InMemoryResultFeed`);
`KAFKA_TEST_BOOTSTRAP_SERVERS` gates the round trip through a real topic.

## The error topic

*What.* `ErrorEvent` (`messages/errors.py`) is what a pipeline publishes when
something *operational* fails: the player's provider raised, a module's
`process` raised. The bus is per pipeline, so the edge adds one hop: an
`ErrorForwarderModule` per pipeline (`app/server-python/src/errors/`, via
`shared.py`) copies them into a per-client hub tagged with the app's slug, and
`/api/errors/ws` pushes them to the layout's `<ErrorTray>` on every page. Not an
alarm: grid alarms are a correctly running application's result.

*Where.* `messages/errors.py`; `datagateway/player.py` (`_on_stream_error`),
`modules.py`; `app/server-python/src/errors/`; `hooks/useErrorFeed.ts`,
`components/ErrorTray.tsx`.

## What is deliberately not here yet

Absent from this slice, on purpose:

- the bridge from the desktop package's thread-based `SnapshotApp`s to the bus
  (`GatewayIO`, `AppIO`) and the thread-hosted player that goes with it;
- the grid monitor re-pointed at the core (it still runs the proof-of-concept
  `Hub`/`Bus`/`HubRegistry` in `pswamp_web/`), and so its own hub wired into
  the error topic;
- `request_id` in the browser-facing acknowledgement; batch jobs beyond the
  explorer's row count;
- the draft's CSV provider and a broker *as history* (a topic's retention as a
  time-addressed source; the transport is not one), and the measurements the
  out-of-process hosting was meant to wait for (frame-to-result latency and
  broker throughput at the target rates);
- paging and backpressure in the time-series provider, and models beyond the
  PMU pair in it; `Module.run` carrying a `Command` natively (the row-count
  module overrides it);
- a `Command` addressed to a module *in the worker* (in-process, the explorer
  has one);
- a `PmuFrameAssembler` for deployments that ingest per-PMU messages.
