# The server data architecture

How PMU data gets from a data source, through analysis modules, to the web
client, and what each piece on that path is for.

The architecture lives in `core/` as the Python package `pswamp_core`. The web
backend (`app/server-python/`) builds on it. One example runs through this
document and the code: the **PMU test streamer**, a recorded and a live PMU
feed going through one module to one page. Each section below names the code
that implements it.

## What it has to do

| Requirement | Answered by |
|---|---|
| PMU data from several possible sources, processed by swappable modules, delivered to the browser | providers → gateway → player → bus → modules → web edge |
| Adding a module is easy, and its contract (what it receives, what it sends) is explicit | `Module`: one input class, one output class, `process` |
| A heavy module can run as its own process/pod | `RemoteModule` + a worker process, over a transport |
| One place defines the shared pieces and data models | `core/` (`pswamp_core`) |
| Topics go through a transport layer, so Kafka can be swapped out (e.g. for NATS) | `Transport`, with Kafka as the default implementation |
| Pipelines of modules can be defined | `Pipeline`: one gateway, player, bus and module list per key |
| Commands are defined in the core, can be sent upstream, and are picked up by whichever piece declares them | typed `Command` classes, routed by class |
| Runs in compose and in minikube | a worker is the same image with a different command |
| A data source outside the project can be plugged in through a REST contract | `RemoteDataClient` + `doc/remote-data-integration-contract.md` |
| Frames get a reference to the CIM model early, so later modules can look up grid data | gateway enrichers stamp `PmuHeader.cimReferenceId` |
| Live pipelines are shared by every viewer; recorded ones are per client | the pipeline *key*: the stream name for live, the client id for recorded |

## Terms

- **Message**: a pydantic `DataModel`. Everything that crosses a topic, a
  socket or a process is one. The class *is* the topic.
- **PmuFrame**: one instant of every PMU channel. It carries its own layout
  (`PmuHeader`), so any single frame is enough to work from.
- **Command**: a message that asks for something (play, seek, reset). It is
  routed by its class to the one piece that declared it.
- **Provider** (`DataClient`): a data source. It declares what it can do:
  serve history, tail live data.
- **Gateway** (`DataGateway`): a pipeline's providers, at most one for history
  and one for live, behind two reads: `consume` (history) and `tail` (live).
  Enrichers run here.
- **Player**: pulls from the gateway and paces frames onto the bus. It takes
  the replay commands.
- **Bus**: in-process publish/subscribe, typed on message classes. There is one
  bus per pipeline.
- **Transport**: publish/subscribe *between* processes (Kafka, or in-memory for
  tests). A module only ever sees a bus; the transport matters only once a
  module moves out of the process.
- **Module**: analysis. It consumes one message class off the bus and publishes
  another.
- **Worker**: a process that hosts a module for every pipeline that uses it.
- **Pipeline**: one gateway, player, bus and module list, built per *key*.
- **Web edge**: an app package in the web backend. It turns `POST`s into
  commands and pushes state down a WebSocket.

## The picture

```mermaid
flowchart TB
    subgraph sources["providers"]
        direction LR
        rec["recording<br/>(history)"]
        live["live feed"]
        remote["remote data service<br/>(REST, outside the project)"]
    end
    gw["gateway<br/>+ enrichers (CIM reference)"]
    player["player"]
    bus["bus (one per pipeline)"]
    mod["module"]
    worker["worker process"]
    edge["web edge<br/>POST → command · state → WebSocket"]
    browser(["browser"])

    rec --> gw
    live --> gw
    remote --> gw
    gw --> player -- frames --> bus
    bus -- input --> mod -- result --> bus
    mod <-. "optional: over a transport (Kafka)" .-> worker
    bus --> edge --> browser
    browser -. "POST" .-> edge -. "command, routed by class" .-> bus
```

Data flows down, and commands flow up. Every arrow carries a message. Nothing
above the bus knows what is below it, which is why a provider can be swapped
and a module can be moved out of the process without the page noticing.

## Messages — `pswamp_core.messages`

*What.* Every message is a `DataModel`: a pydantic model with a pinned schema
`version`, an optional `mRID` identity, a UTC `timestamp`, and a **topic
derived from the class name**.

```python
class LineTrip(DataModel):            # topic: "line.trip"
    version: Literal["v1"] = "v1"
    timestamp: datetime
    line: str

LineTrip.model_validate_json(text)    # the whole codec; a "v2" payload fails here
```

| Kind | Classes | File |
|---|---|---|
| measurement | `PmuFrame`, carrying its `PmuHeader` | `pmu.py` |
| command (up) | `Command`, `PlayerCommand` and its subclasses (`PlayCommand`, `SeekCommand`, `GoLiveCommand`, …) | `commands.py` |
| result (down) | `ResultEnvelope[T]`: what every module emits | `results.py` |
| control (down) | `PlayerStatus`, `StreamChanged` | `control.py` |
| error (down) | `ErrorEvent`: something operational failed | `errors.py` |

*Why.* No pickle and no numpy on the wire: a message can be logged,
validated on receipt, and published into the browser contract as is. The set
of `DataModel` subclasses *is* the topic catalogue.

**A frame carries its layout.** `PmuFrame.header` is repeated in every frame
(~1.2x on the wire after a broker's compression). In return, any single frame
is enough to work from: a module in another process is primed by its first
frame, and a changed layout is just the next frame.

**A command's class is its address.** There are no verb strings: `SeekCommand`
*is* the seek, and its fields are its validated arguments. A module's own
commands and result classes live beside the module; the player's live here,
because the player is core.

## Topics: the bus — `pswamp_core.bus`

A topic is a message class. Two layers carry topics: the bus inside a process,
and the transport between processes (next section). **A module only ever sees
the bus.**

**The bus** (`InProcessBus`) is publish/subscribe inside one process. There is
one bus per pipeline, and a subscription to a base class receives every
subclass.

```python
bus = InProcessBus()
with bus.subscribe(PmuFrame, overflow=Overflow.DROP_OLDEST, maxsize=64) as frames:
    async for frame in frames: ...
bus.publish(result)                     # from the event loop
bus.publish_threadsafe(result)          # from a thread: the one crossing point
```

Overflow is chosen per subscription: `DROP_OLDEST` for a live stream, `GROW`
for commands. So a slow browser tab drops its
own frames and never stalls the analysis. `Latest` keeps the newest message
of each class, which is what a freshly connected socket renders from.

## Topics between processes: the transport — `pswamp_core.transport`

The transport (`Transport`) is publish/subscribe *between* processes. It
comes into play only when a module runs as its own service. There is one
topic per message class, and the pipeline key rides as the record key, never
as a message field:

```python
await transport.publish(frame, key="42")                  # topic pmu.frame, key "42"
with transport.subscribe(FrameStatsResult, key="42") as results: ...
with transport.subscribe(PmuFrame) as frames: ...         # every key: a worker's view
```

| Implementation | Where | Used by |
|---|---|---|
| `InMemoryTransport` | `transport/__init__.py` | tests: one instance shared by both sides *is* the broker |
| `KafkaTransport` | `transport/kafka.py`, the `pswamp-core[kafka]` extra | compose and k8s |

*Why a transport, not the broker as the bus.* The broker carries only what
crosses a process boundary. Everything else stays a method call on the event
loop. Swapping Kafka for NATS means one new `Transport` subclass that implements
`publish` and `_feed` (one broker consumer per class). Nothing above it
changes. `InMemoryTransport` is the second implementation that keeps the
contract honest.

A transport is chosen from the environment, like everything plugged in by name
(`pswamp_core.settings`):

```
PMU_TEST_STREAMER_MODULE_TRANSPORT=kafka:pswamp_core.transport.kafka:KafkaTransport
KAFKA_BOOTSTRAP_SERVERS=kafka:9092        # the transport's own {NAME}_{SETTING} block
```

Kafka topics are created with about a minute's retention (`LIVE_TOPIC_CONFIGS`).
Every topic is a live hop that nobody reads back, and the broker's defaults let
a fast replay fill a disk.

## Modules — `pswamp_core.modules.Module`

*What.* A module consumes one message class off the bus and publishes another.
That is the whole contract: its **input** is `input_model`, and its **output**
is `output_model`, a `ResultEnvelope[Body]` subclass whose name is its topic.

```python
class FrameStats(BaseModel):                            # the result body
    mean_frequency_hz: float | None
    ...

class FrameStatsResult(ResultEnvelope[FrameStats]):     # topic: frame.stats.result
    version: Literal["v1"] = "v1"

class FrameStatsModule(Module):
    name = "frame-stats"
    input_model = PmuFrame
    output_model = FrameStatsResult

    async def process(self, frame: PmuFrame) -> FrameStats | None:
        ...                                             # None publishes nothing
```

`Module.run` subscribes, calls `process`, wraps the body in the envelope
(timestamp, the module's identity, its `parameters`) and publishes it. A
`process` that raises publishes an `ErrorEvent` and the module carries on.
Whoever shows the result subscribes to `FrameStatsResult`, never to the
module.

*Why.* A contributor's module is the analysis plus two class attributes. It
imports only `pswamp_core`, and it never learns whether it runs in-process or
as its own service.

**Adding a module:**

1. Define the result body (pydantic) and its envelope beside the module. If
   another package must import it, put it in `core/.../messages/` instead.
2. Subclass `Module` with `name`, `input_model`, `output_model`, `process`.
   Read the layout off `frame.header`, and re-derive it when `header_id`
   changes. Don't cache anything the frame does not bring, so the module can
   later move to a worker unchanged.
3. Test it by calling `process` directly (see
   `app/server-python/tests/test_pmu_test_streamer.py`).
4. Put it in a pipeline's module list (see "Pipelines" below).

`process` runs on the event loop. A CPU-heavy module must hand its work to a
thread or process pool, or it stalls every pipeline in its process.

*Where.* `core/src/pswamp_core/modules.py`. The example is
`app/server-python/src/pmu_test_streamer/stats_module.py`.
