# STEP 3 — The target data architecture: one design for providers, topics, modules and clients

> **Status:** WIP proposal, third step of the data-integration track. Companion to
> `STEP1-WIP-data-integration-context.md` (requirements A1–A8, assessment of `main`,
> ordered next steps) and `STEP2-WIP-data-integration-evaluate-initial-louis-design-draft.md`
> (evaluation of the `test_pswamp` data-gateway draft at `draft:c6ce9a3`). This document
> is the synthesis: **one target architecture for the server side** that satisfies A1–A8
> under the settled constraints, built around the parts of the draft that STEP 2 said to
> lift, with everything that has to be added on top called out by name. It changes no
> code. It proposes ADRs; it writes none.
>
> **What is settled and not re-argued here** (STEP 1 §6, verbatim sources cited there):
> client-server is the direction and the analysis core stays Python (README, ADR-002);
> monorepo, new functionality is a module here (ADR-004); code-first OpenAPI with
> `x-websocket-channels` and a manual `API_VERSION` (ADR-003); commands up as `POST`,
> state down one socket, `CommandAck` never carries state (ADR-003, api doc, AGENTS.md);
> the analysis applications keep their thread execution model and the web layer bridges
> to them (port doc §2); the `io=` constructor argument is the source-swap point and
> replay survives as a mode (port doc §2, §10.2); no auth and no TSO config in the repo
> (rig doc); stateless, no database or volume without an explicit ask (AGENTS.md); every
> service exists in compose *and* k8s and is locally testable with stubs, and the rig is
> only complicated with numbers (contributor doc). STEP 1's three scope decisions also
> stand: the topic abstraction is transport-neutral with an in-process default; chunk
> queries and batch reports are consumed by server-side modules; core first, web second.
>
> **How to read the code sketches.** Signatures below are *proposals at the level of a
> contract*, precise enough to be argued with and to size the work, not a specification
> to transcribe. Names from the draft are kept wherever the concept is the draft's, so
> that a reader of `draft:src/p_swamp/core/datagateway/` recognises it here. File paths
> are relative to each repo root; `draft:` marks the sibling repo.

## 0. The one-paragraph version

The target is a layered core under `src/pswamp/` in which **messages are versioned
pydantic models** (the draft's `DataModel`, plus a per-instant PMU frame, a result
envelope, status, alarm and command models); **providers are capability-declared
`DataClient`s behind the draft's `DataGateway`**, which is where seek, range query and
history-to-live stitching already live; a new **`Player`** paces a gateway stream and
owns the replay controls; a new **in-process `Bus`** fans messages out to N subscribers
with an explicit overflow policy and is the one place application threads cross into the
event loop, with a `GatewayBus` adapter that makes a broker a bus; the existing
**`SnapshotApp`/`TimeWindowApp` become an enforced module contract** fed through one new
`GatewayIO` that retires the seven duck-typed io implementations; and a **`Pipeline`**
binds one stream's player, bus and modules together, shared for live data and per client
cursor for replay. The web edge keeps its contract unchanged and is re-pointed at the
core; a `source` app entry exposes the player's controls as POSTs, and `CommandAck`
gains the correlation id that batch jobs need. The messages base, the gateway, the
clients, the configuration scheme and the tests are the draft lifted with fixes; the
bus, the player, the module contract and registry, `GatewayIO`, the pipeline and
sessions, jobs, the `.npz` provider, the conformance suites, packaging and the web
re-point are new, and §9 lists them as the open threads.

## 1. Inputs

| input | what it fixes here |
|---|---|
| STEP 1 §1 (A1–A8 restated), §4 (needs per requirement), §5 (tensions), §8 (order) | the requirements, the candidate ADRs, the landing order this document revises |
| STEP 2 §5 (lift / adapt / leave), §6 (pull vs push) | which of the draft's parts are taken as-is, which reshaped, which deferred |
| `draft:src/p_swamp/core/datagateway/`, `core/models/`, `tests/` at `c6ce9a3` | the provider layer and the message base, by name |
| `src/pswamp/app_templates/`, `streaming/`, `utils/` | the module contract's starting point and the io seam to declare |
| `app/server-python/src/pswamp_web/` | the browser edge to keep; the PoC internals to replace (STEP 1 §2.8) |
| `doc/adr/001–004`, `doc/the-client-server-api.md`, `doc/client-server-rig.md`, `doc/how-the-core-contributors-work-together.md`, `doc/WIP-context-port-from-qt-to-web-frontend.md` | the constraints quoted in the status block |

Two things this document deliberately does **not** decide, because other documents own
them: how long the Qt path lives and therefore where the shared Python ends up (port doc
§7 — both moves stay cheap under this design, see §5), and the CIM / grid-model provider
side of the draft (`DataHub`, `cimgraph`, the converters), which STEP 2 §4 recommends as
a track of its own.

## 2. Principles

Eight rules the design is held to. Each is either already settled or was confirmed in
STEP 1; they are restated so that §4 can refer to them by number.

1. **Core first, web second.** Every layer in §4 is provable under `src/pswamp/` with a
   pytest run and no web server; the web edge is adapted last (§4.8) and its contract is
   kept verbatim.
2. **Transport-neutral, in-process by default.** A broker is an adapter of the same
   interfaces that a deployment plugs in, never a prerequisite for running the repo.
3. **Everything on a topic is a versioned pydantic model.** No pickle, no numpy on the
   wire, no `uuid.UUID` or `datetime` inside a result. The draft's `DataModel` is the
   base of every message.
4. **Providers declare capabilities; the core never calls what was not declared.** A
   `pass` method is a contract violation, not a permitted implementation.
5. **History lives with the provider.** The repo persists nothing; a time-series store
   in a TSO deployment is a `DataClient`, outside the "no database" rule rather than an
   exception to it.
6. **Applications keep their execution model.** Blocking loop, own thread, nine-method
   io. The core bridges to them; it does not rewrite them.
7. **Only complicate with numbers.** No module leaves the process, and no broker enters
   the default path, before a load generator and an end-to-end timestamp say why.
   Anything that does exists in compose *and* `k8s/`, with a local stub.
8. **Contracts are enforced, generated, scaffolded and conformance-tested.** The page
   contract sets the standard (`check_apps`, the generated OpenAPI document,
   `generate-new-subapp.sh`, the smoke test); the provider and module contracts are held
   to the same one.

## 3. The architecture in two pictures

### 3.1 The layers

```
                         browser(s)                      Qt (later; another host)
                              │  POST /api/<app>/…  ▲ ws
 ─────────────────────────────┼─────────────────────┼──────────────────  web edge (kept)
 L8  pswamp_web/   page packages · SessionRegistry · send_state · source app (player POSTs)
 ─────────────────────────────┼─────────────────────┼──────────────────  core: src/pswamp/
 L6  Pipeline       one stream = (gateway, cursor) → player + bus + modules;  PipelineRegistry
                    ┌─────────┴─────────────────────┴───────────┐
 L5  Modules        │ SnapshotApp / TimeWindowApp (contract)     │  MODULES registry
                    │   ▲ AppIO (Protocol) ──── GatewayIO ───────┼── the sync facade
 L4  Bus            │ InProcessBus ⇄ GatewayBus(DataGateway)     │  N subscribers, overflow policy,
                    │   publish_threadsafe (thread → loop)       │  the one crossing point
 L3  Player         │ paces gateway.consume(PmuFrame, …)         │  speed · pause · step · seek · mode
 L2  Gateway        │ DataGateway · SegmentPlanner · DataStream  │  (draft, lifted)
                    │   DataClient: Recording · InMemory · Csv · Kafka · <TSO's own>
 L1  Messages       │ DataModel → PmuHeader, PmuFrame, ResultEnvelope, AppStatus, Alarm,
                    │             Command, CommandAck, JobResult, PlayerStatus
                    └────────────────────────────────────────────┘
```

Arrows of dependency run **downward only**: a layer imports the ones below it and
nothing above. L1 and L2 are the draft's; L3–L6 are new; L5 is the existing template
formalised; L8 is the existing web edge re-pointed.

### 3.2 The same module, three hosts

```
 (a) in-process, web server (default)        (c) separate service (after numbers, §4.7)
 ┌───────────────────────────────────┐        ┌──────────────────────┐  ┌──────────────────────┐
 │ uvicorn loop                      │        │ web server           │  │ pswamp.runmodule     │
 │  Pipeline                         │        │  Pipeline            │  │  IslandingApp        │
 │   Player ─ InProcessBus ─ modules │        │   Player ─ Gateway-  │  │   io=GatewayIO(      │
 │            ▲ threads: 1 per module│        │            Bus ──────┼──┼──►  GatewayBus(kafka))│
 │  Gateway: RecordingClient         │        │  Gateway: TSO client │  │  Gateway: kafka      │
 └───────────────────────────────────┘        └──────────────────────┘  └──────────────────────┘
 (b) in-process, Qt: a Pipeline with its own loop thread; bus → Qt delivery not designed here
```

The module class is the same object in all three; what differs is the `io=` it is
handed (principle 6, port doc §2: *"a constructor argument … Don't let it become a
rewrite"*).

## 4. Layer by layer

Each layer: what it is for, the contract as a sketch, what comes from the draft, what is
new, which requirements it carries, and what stays open.

### 4.1 L1 — Messages (`pswamp/messages/`)

**Purpose.** One vocabulary for everything that crosses a topic, a socket or a process
boundary — measurements *and* results (STEP 1 A1: "two layers, not one").

**From the draft, lifted as-is.** `DataModel` (`draft:core/models/data_model.py:75`):
required `version` that subclasses pin to a `Literal`, `mRID`, a UTC-coerced
`timestamp` (`:115`), and the class-derived `topic` (`:124–146`) — minus the `branch`
field, see the decision at the end of this section. The JSON
codec is pydantic's own (`model_dump_json` / `model_validate_json`), which is what the
browser edge already uses, so one serialiser now spans core and edge. The package is
renamed from the draft's `core/models/` only because `src/pswamp/models/` is already the
grid model (`bus.py`, `line.py`, `load.py`, `reader.py`); the module names inside
(`data_model.py`, `measurements/`) are kept.

**Adapted: the measurement model.** STEP 2 A1 found the draft's per-PMU
`MeasurementPmu{Voltage,Current,Frequency}` (`draft:measurement_pmu.py:13,37,61`) have
no channel identity table, no `data_rate`, an optional timestamp, and arrive as three
unaligned streams — while every p-SWAMP application consumes one labelled row per instant
(`TimeWindowLabeled`, `src/pswamp/utils/time_window_labeled.py:93`; `PMUDecoder`,
`src/pswamp/utils/pypmu.py:35–90`). The canonical in-core form is therefore a **frame**:

```python
class PmuHeader(DataModel):                      # topic: pmu.header (namespace-prefixed per deployment)
    version: Literal["v1"] = "v1"
    timestamp: datetime                          # required: when this layout became valid
    mRID: str                                    # the stream (PDC / recording) identity
    header_id: str                               # content hash of the rows below
    station: list[str]                           # the three rows the Indexer already reads
    channel: list[str]
    measurement: list[str]                       # "f" | "df" | "<name>_Magnitude" | "<name>_Angle"
    units: list[str]                             # "Hz" | "Hz/s" | "V" | "A" | "rad"
    data_rate: float                             # frames per second
    freq_encoding: Literal["absolute_hz"] = "absolute_hz"   # STEP 1 A1's open decision, made explicit

class PmuFrame(DataModel):                       # topic: pmu.frame
    version: Literal["v1"] = "v1"
    timestamp: datetime                          # required (never stamped at produce time)
    mRID: str                                    # same stream identity as its header
    header_id: str
    values: list[float | None]                   # one per header column; None where NaN
    quality: list[int] | None = None             # a place for the C37.118 STAT word
```

`PmuHeader` is the config-frame analogue: sent once when a stream starts and again
whenever the layout changes; a consumer resolves `header_id` to the header it holds
(§4.5 says how). `values` is `float | None` for the reason the port doc §4.6 learned
the hard way: NaN is not JSON and is the normal case. The three-row vocabulary is
exactly what `Indexer.get_col_idx(measurement='f')` queries today, so the labelled
window and the decoder need no new concept, only a new source of the rows.

The draft's per-PMU models are **kept**, as the natural shape for a deployment that
ingests PMUs individually onto a broker or archives them per device (a CSV per model is
what `CsvClient` does). Which representation a *broker* carries canonically is the
one design choice in this layer with a real cost — STEP 2 A1 computed ~6,600 objects/s
for N44 per-PMU versus 50 rows/s per-frame — and STEP 2 §4 said to measure before
deciding. Default until then: frames on every topic the core itself reads; a
`PmuFrameAssembler` (§9) turns per-PMU objects into frames at the gateway edge for
deployments that need it.

**New: the result layer.**

```python
class AppStatus(StrEnum): OK = "OK"; ALERT = "Alert"; EMERGENCY = "Emergency"
                          INITIALIZING = "Initializing..."; UNDEFINED = "Undefined"

class AppIdentity(BaseModel): name: str; uuid: str

class ResultEnvelope(DataModel, Generic[T]):     # topic derived from the *subclass* name
    version: Literal["v1"] = "v1"
    timestamp: datetime
    app: AppIdentity
    parameters: dict[str, JsonValue] = {}
    request_id: str | None = None                # set when this result answers a Command
    result: T                                    # the module's declared output model

class AppStatusMessage(DataModel): app: AppIdentity; status: AppStatus; timestamp: datetime
class Alarm(DataModel): uuid: str; app: AppIdentity; type: str; message: str; timestamp: datetime
class Command(DataModel):
    request_id: str; client_id: str | None; target: str | None   # target = module uuid, or None for the player
    verb: str; args: dict[str, JsonValue] = {}
class CommandAck(BaseModel): status: Literal["ok"] = "ok"; applied: str; request_id: str
class JobResult(ResultEnvelope[T]): pass          # a ResultEnvelope whose request_id is never None
class PlayerStatus(DataModel): mode: Literal["live", "replay"]; cursor: datetime | None
    speed: float; paused: bool; can_seek: bool; coverage: TimeRange | None
```

`ResultEnvelope` is the `{time_stamp, info{app_name, uuid}, parameters, result}`
convention (STEP 1 §1 A1) made a model; `IslandingResult`, `LineOutageResult`,
`ModeEstimate` subclass it with a concrete `T`, and that subclass name is the topic. The
status enum is the `Literal` already in `pswamp_web/wire.py` pushed upstream. `Alarm` is
what the port doc §11 asks for (*"Make `AlarmHandler` emit JSON-native types"*).
`CommandAck` is the web edge's model (`wire.py:147`) plus the one field A7 needs.

**Two rules recorded here.** *Time base:* UTC `datetime` on every wire (the draft's
choice, enforced by validator); epoch float seconds inside `TimeWindow` and `Recording`,
as today; the conversion lives in `GatewayIO` and nowhere else. *Versions:* a model's
`version` literal is bumped when *that* schema breaks; `API_VERSION` in
`api_contract.py` is bumped when a wire model the browser sees breaks (ADR-003 rule);
additive changes bump neither.

**Carries:** A1 fully; the topic half of A2 (the catalogue is the set of `DataModel`
subclasses — name from the class, schema from the class, direction from which layer
publishes it).

**Decided: `branch` is dropped in the lift.** The draft splices a per-message `branch`
field into every topic name at index 1 (`draft:data_model.py:108,140–146`), default
`live`, documented only as "the data branch, by default live for the real time data".
Its one use is co-tenancy — a simulation or a development run sharing a broker with the
real feed without writing to its topics — which is the situation of a shared test Kafka
rig and not of a TSO deployment, where dev, test and prod are separate environments and
that boundary already isolates everything. The repo's own default path has no broker at
all. So the field, the splice and the convention go; a topic is the model's name. What
*does* survive is the other axis the multi-TSO example needs — *whose* stream
(`no.*` / `se.*`, `examples/nordic44_rtsim_multi_tso/config_no.toml`) — as a
`namespace` prefix supplied by gateway configuration, never carried on a message, so
nothing on an instance can disagree with the topic it was read from. `topic` stays an
overridable `ClassVar` for the odd case, and the CamelCase splitter
(`PMU` → `p.m.u`, digits dropped) is fixed in the same change.

### 4.2 L2 — Providers and the gateway (`pswamp/datagateway/`)

**Purpose.** The contract a data source implements (A8) and the operations the core
asks of it — stream, range, seek, produce (A5, A8).

**From the draft, lifted as-is — this is the draft's centre and it stays whole.**
The package moves under `src/pswamp/` with its own sub-structure and every name intact:

| name | draft | role here |
|---|---|---|
| `Capability` (`LIVE_CONSUME`, `HISTORY_CONSUME`, `PRODUCE`) | `data_client_model.py:81` | what a provider may be asked |
| `DataClient` ABC: `coverage()`, `consume()`, `produce()`, `open()`/`close()`, `supported_models`, `priority`, `show_config()` | `data_client_model.py:89–214` | **the provider contract** (A8), with the three `consume` rules quoted in STEP 2 |
| `TimeRange`, `Coverage` | `time_range.py:47,139` | half-open windows; what a client holds and whether it goes live |
| `SegmentPlanner` (`on_gap`, `live_handoff_margin`) | `planner.py:68` | which client serves which stretch; history → live hand-off |
| `DataStream` (watermark de-dup, `segments` trace, `aclose`) | `stream.py:37` | one stitched cursor over the clients |
| `DataGateway` (`consume(model, start, end, mRID)`, `produce(data)`) | `data_gateway.py:40` | the one object the rest of the core talks to |
| `EnvSetting`, `env_*`, `format_settings`, `from_env`, `show_config` | `config.py`, `csv_file.py:127`, `kafka_bus.py:146` | provider configuration without code (A8) |
| `InMemoryClient`, `CsvClient`, `KafkaClient` | `clients/` | reference / archive / broker examples |
| `test_data_gateway.py`, `test_csv_client.py`, `test_client_config.py` | `tests/` | the start of the conformance suite |

"Query a chunk" and "jump to a time" are the same call — `gateway.consume(PmuFrame, t0,
t1)` and `gateway.consume(PmuFrame, t0, None)` — which is why A5's seek and range query
are *provider* capabilities here and not bus features (STEP 2 A5).

**Fixes carried in with the lift** (each is a STEP 2 finding; none changes the design):
`timestamp` required on measurement models; `InMemoryClient` fans out to N consumers
instead of splitting one `asyncio.Queue` between them (`draft:in_memory.py:75,142`);
`KafkaClient.coverage` reads the broker's earliest offsets rather than assuming
retention (`draft:kafka_bus.py:185–199`); `produce` fan-out reports failures instead of
only logging them (`draft:data_gateway.py:157–164`); Python 3.11 spellings for `Self`
and `datetime.UTC`; `logging` for `loguru`; the `p_swamp` → `pswamp` import path.

**New.**

- **`RecordingClient`** — the committed `.npz` recording (`pswamp_web/data/n44_line_trip_50hz.npz`)
  as a `DataClient` with `HISTORY_CONSUME`, yielding `PmuHeader` then `PmuFrame`s from
  `Recording.time` / `Recording.data` (`pswamp_web/recorded_io.py:70–93`). This is the
  reference provider STEP 1 §8 step 3 asked for and the port doc §8.1 said belongs
  beside `kafka_io.py`; it replaces `RecordingPlayer` + `RecordedIO` (their pacing half
  moves to §4.3, their io half to §4.5). Coverage is exact (first and last timestamp),
  which is what makes it the right client to run the conformance suite against.
- **A client-parameterised conformance suite** — `pswamp/datagateway/conformance.py`
  exposing a pytest fixture contract (`client_under_test`, plus a `seed(records)`
  hook); the draft's tests become its first cases (history in order, window honoured,
  overlap emitted once, gap skip and raise, live hand-off, iterator released on close),
  extended to cover the **live tail** (an open range on a `LIVE_CONSUME` client keeps
  yielding) and **produce ordering**. A TSO runs it against their own client with a
  five-line `conftest.py`.
- **Out-of-repo packaging** — a `DataClient` is discovered by entry point
  (`[project.entry-points."pswamp.data_clients"]`) or dotted path, imports
  `pswamp.datagateway` and `pswamp.messages` and nothing else; `synchrophasor`,
  `aiokafka` and any database driver live with the client that needs them, not in the
  server image (AGENTS.md: *"none of that belongs in a headless server image"*).
- **Gateway composition from configuration** — the draft configures *a* client from the
  environment but composes the gateway in code
(`draft:examples/simulation_task/rtsim/rtsim.py:252–260`). Add one
  variable naming the clients, and a `from_env` on the ABC (today it is per client,
  `draft:csv_file.py:127`, `kafka_bus.py:146`):
  `PSWAMP_DATA_CLIENTS="recording:pswamp.datagateway.clients.recording:RecordingClient,archive:acme_tso.timescale:TimescaleClient"`,
  each then read with its own `{NAME}_{SETTING}` block. `show_config` prints the whole
  table. The repo's default is the single `recording` client, so nothing changes for a
  developer.
- **Kafka deployment rule**, written down with the client: single partition per topic,
  or partition by `mRID` — the watermark drops cross-partition out-of-order records
  (`draft:kafka_bus.py:89–93`).

**Carries:** A8 (contract, example, configuration, packaging, conformance); the seek and
range halves of A5.

**Open.** `priority` ties and the semantics of a client whose coverage *shrinks*
(retention passing over the cursor) — the planner re-asks coverage at every boundary
(`draft:planner.py:154`), so the answer is "the gap policy applies", but it should be
tested.

### 4.3 L3 — The player (`pswamp/datagateway/player.py`)

**Purpose.** The thing A5 actually commands: pace a gateway stream at a chosen speed,
pause it, step it, jump it — and know when none of that applies (port doc §10.2: *"a
mode, not a phase … No dead buttons on an operational screen"*).

**New.** The draft has no pacing (STEP 2 A5: a history segment is yielded as fast as the
client reads it) and no control on an open stream. The PoC's `RecordingPlayer`
(`recorded_io.py:273`) has pacing with `speed` and `loop` as constructor literals and no
controls. The player is the union, over the gateway instead of over one file:

```python
class Player:
    def __init__(self, gateway: DataGateway, bus: Bus, *, stream: str,
                 start: datetime | None = None, speed: float = 1.0, loop: bool = False): ...
    mode: Literal["live", "replay"]         # "replay" iff the gateway has HISTORY_CONSUME for PmuFrame
    def start(self) -> None                 # own daemon thread; see below
    def stop(self) -> None
    def pause(self) -> None;  def resume(self) -> None
    def step(self, frames: int = 1) -> None
    def set_speed(self, speed: float) -> None
    def seek(self, to: datetime) -> None    # replay only; raises Unsupported in live mode
    def status(self) -> PlayerStatus
    def subscribe(self) -> FrameSource      # what a GatewayIO reads from (§4.5)
```

**Three decisions, made here so they are made once.**

1. **Seek is a new stream.** `seek(t)` closes the current `DataStream`, opens
   `gateway.consume(PmuFrame, t, None)`, and publishes `StreamChanged(cursor=t)` on the
   bus. Modules re-prime themselves through their `GatewayIO` (§4.5); the player does
   not know their window lengths and must not. This is the desktop's "construct a new
   application at `t_start`" precedent (STEP 1 §2.5) reduced to its cheap part. A looping
   replay is the same mechanism once per pass — a *new* `DataStream` each time, because
   the watermark rightly drops anything older than what it has seen
   (`draft:stream.py:104–156`).
2. **The player runs on its own thread and pulls in batches.** AGENTS.md keeps the
   50 Hz sample path off the event loop for a reason (`serve_ticks` exists so that the
   window is *read* on the loop at 10 Hz, never *written* there). A player written as a
   coroutine would put eight pipelines × 50 wake-ups a second, each validating a
   `PmuFrame`, onto the loop that also serves every socket. So the player thread asks the
   loop for the next ~1 s of frames at a time (`asyncio.run_coroutine_threadsafe` over
   the stream), paces them on the thread, and hands each frame to its subscribers'
   queues. In live mode "pacing" is pass-through and the batch is whatever arrived.
   This is the default because it is what the PoC measured as working; it is a
   measurable choice, not a law.
3. **Pause in live mode is view state, not source state.** A live stream cannot be
   paused at the source; a "pause" there freezes what the viewer sees and lets the
   window keep filling. The player exposes `can_seek`, `can_pause_source` in
   `PlayerStatus` so the edge renders only real controls.

Overflow between the player and a slow module is a policy, stated per mode: in live
mode a subscriber queue drops oldest (a stale PMU frame late is worse than one never
delivered — port doc §13); in replay mode the player stalls, since the point of a replay
is completeness. This is the §4.4 landmine (`_offer` versus `_push_history`,
`recorded_io.py:466,489`) turned into two named policies.

**Carries:** A5's navigation half (seek, forward, back, speed), the live/replay mode.

### 4.4 L4 — The bus (`pswamp/bus/`)

**Purpose.** A2's publish/subscribe over named topics, in-process by default, with a
broker as an adapter (principle 2), and the single place where a module thread crosses
into the event loop (port doc §2).

**New — the draft has no bus** (STEP 2 A2: `consume` is a pull cursor; the only in-process
live path splits messages between readers). The PoC's `pswamp_web/bus.py` is the
starting material — `add_listener` (`bus.py:96`), `publish_threadsafe` (`bus.py:119`),
and the never-used `subscribe` with `latest_only` and `maxsize` (`bus.py:150`) — moved
into the core, typed on models instead of strings, and given the overflow policy the
port doc §13 asks for:

```python
class Overflow(StrEnum): DROP_OLDEST = "drop_oldest"; LATEST_ONLY = "latest_only"; GROW = "grow"

class Bus(Protocol):
    def publish(self, message: DataModel) -> None                        # loop thread
    def publish_threadsafe(self, message: DataModel) -> None             # any thread → loop
    def subscribe(self, *models: type[DataModel], overflow: Overflow = Overflow.DROP_OLDEST,
                  maxsize: int = 256) -> Subscription                    # async iterator + close()
    def add_listener(self, model: type[DataModel], callback) -> Callable[[], None]

class InProcessBus: ...          # the default; topic = Model.topic; fan-out to every subscription
class GatewayBus:                # a broker as a bus
    def __init__(self, gateway: DataGateway, local: InProcessBus): ...
```

`GatewayBus` publishes by `gateway.produce(message)` and subscribes by opening **one**
`gateway.consume(Model, now, None)` per model per process and fanning it out through the
local bus — never one broker consumer per subscriber, which is what N `consume` calls
on the draft's `KafkaClient` would cost (`draft:kafka_bus.py:222–228`). The draft's
produce fan-out to every `PRODUCE` client gives write-through archiving for free (STEP 2
A2): a Kafka client and a CSV client registered together means every live message is
also history, which is how a TSO's deployment turns the bus into its own provider.

**The seam rule, restated.** Every thread↔loop crossing in the core lives in **two
files**: `bus/` (`publish_threadsafe`, thread → loop) and `GatewayIO` (§4.5; the queue
a module thread blocks on, loop → thread), plus the existing locked window read
(`TimeWindow.get_safe`, `src/pswamp/utils/time_window.py:34`) that a ticker-driven page
already uses. Nothing else may touch the loop from a thread. This replaces the PoC
formulation ("exactly two crossings in `pswamp_web/`") with one that survives the move
into the core.

**Carries:** the pub/sub half of A2; A4's in-process option (with L2's broker client,
the out-of-process one).

**Open.** Whether `add_listener` (a synchronous callback on the loop, what the stores use
today) survives beside `subscribe`, or the stores become subscriptions. Keep both in the
first landing; delete one when the stores are ported.

### 4.5 L5 — Modules and the module contract (`pswamp/app_templates/`)

**Purpose.** A3 and A6: a module reads one topic, writes another, declares what it reads
and what it emits, and a contributor can add one by copying an example.

**Formalised, not replaced.** `SnapshotApp` and `TimeWindowApp` keep their names and
their loop (`snapshot_app.py:146–213`, `time_window_app.py:95–104`); every monitoring
application already subclasses them. What changes is that the conventions become
declarations:

```python
class SnapshotApp(ABC):
    input_model:  ClassVar[type[DataModel]] = PmuFrame
    output_model: ClassVar[type[ResultEnvelope]]           # required; the topic it publishes
    channel_selection: dict | None                          # as today; resolved against PmuHeader
    def __init__(self, io: AppIO, *, eval_freq=None, app_name=None, t_start=None, ...): ...
    @abstractmethod
    def run_analysis(self, time_stamp, measurements) -> BaseModel | None: ...   # returns output_model.result's type
    status: AppStatus                                       # not a free string
    def run_in_thread(self) -> None;  def stop(self) -> None   # the one way to run and to stop
    def reset(self) -> None                                 # new: re-prime after StreamChanged
    def handle_command(self, command: Command) -> None      # replaces the start() console loop
```

`run_analysis` becomes abstract (today `pass`, `snapshot_app.py:137`); `start()` with
its `open_console` verb (`snapshot_app.py:199–213`, arbitrary code execution for
anyone who can write to the topic — STEP 1 §7) is retired; `stop` arrives as a
`Command` with `target == self.uuid`; `ReportingApp` and `AlarmHandler`
(`status_reporting.py`) publish `AppStatusMessage` and `Alarm` instead of dicts.
`set_status`, today a per-app convention (`islanding.py:107`, `line_outage_detection.py:80`),
moves onto the base class.

**The io seam, declared.** The nine-method duck type (STEP 1 §2.3) becomes a `Protocol`
every implementation is annotated against, split so that a provider that cannot seek
does not have to pretend:

```python
class AppIO(Protocol):                      # what a SnapshotApp reads and writes through
    def get_config_frame(self) -> PmuHeader: ...
    def get_sample_data_frame(self) -> PmuFrame: ...
    def get_next_data_frame(self) -> PmuFrame: ...            # blocking; raises StopIteration at end
    def seek_relative_input_offset(self, n_samples: int) -> None: ...   # ≤ 0 only; no-op when the source has no history
    def get_next_command(self) -> Command | None: ...
    def handle_result(self, result: ResultEnvelope) -> None: ...
    def handle_output(self, message: DataModel) -> None: ...  # alarms, and anything else typed
    def handle_status(self, status: AppStatusMessage) -> None: ...
```

**New: `GatewayIO`, the eighth implementation and the one that retires the others.**

```python
class GatewayIO:
    def __init__(self, source: FrameSource, bus: Bus, gateway: DataGateway, *,
                 overflow: Overflow, module_uuid: str): ...
```

It reads frames from the player's per-subscriber queue (blocking, on the module's
thread), publishes results, alarms and status through `bus.publish_threadsafe`, and
delivers `Command`s addressed to `module_uuid` from a bus subscription. Three rules
inside it settle three questions STEP 1 and STEP 2 left open:

- **Header:** `get_config_frame()` returns the latest `PmuHeader` with
  `timestamp ≤ cursor` (a bounded history consume, last item, cached by `header_id`). A
  header change mid-stream is a new `PmuHeader` on the topic, and triggers `reset()`.
  No multi-model time alignment is needed because a frame is already one instant.
- **Pre-fill:** `seek_relative_input_offset(-n)` — which `TimeWindowApp.__init__` calls
  once to prime its window (`time_window_app.py:90–93`) — is a bounded
  `gateway.consume(PmuFrame, cursor - n / data_rate, cursor)` pushed onto the module's
  queue with `Overflow.GROW`, then the live queue resumes. This is the PoC's
  `_push_history` (`recorded_io.py:489`) as a rule rather than a fix. Windows differ per
  module (30 s store, 10 s islanding, 45 s N4SID), which is why pre-fill belongs here and
  not in the player. When the gateway has no history the call yields nothing and the
  window fills from NaN, exactly as a live Kafka source behaves today.
- **Reset:** on `StreamChanged`, `GatewayIO` drains its queue, re-runs the pre-fill at
  the new cursor and calls the module's `reset()`, whose default clears the window. So
  "seek versus a windowed application" (STEP 1 A5 item 2) is answered: the pipeline is
  *not* rebuilt; the module is re-primed, by the same code that primed it the first time.
- **Time base** conversion (wire `datetime` ↔ window float) happens here and only here.

With `GatewayIO` in place, `IslandingApp` runs unchanged over any `DataClient` — the
adoption path STEP 2 §4 called cheap. `KafkaIO`, `NQKafkaIO`, `MQTT_IO` and the two
offline adapters stay as **legacy `AppIO` implementations** (annotated, not rewritten)
until `examples/` are ported off `config['topics']` (`islanding.py:118–126`), so that
`main` stays runnable throughout (contributor doc). `time_series_io.py` and the
duplicate `PMUDecoder` in `pmu_time_window.py` are deleted in the same change.

**Registry and scaffold.** The analysis-side twin of `APPS`:

```python
class ModuleEntry(NamedTuple): slug: str; module: ModuleType; description: str = ""
MODULES = [ModuleEntry("islanding", pswamp.monitoring.islanding, "Islanding detection"), ...]
# each package exports: APP (the SnapshotApp subclass), DEFAULTS (constructor kwargs), and nothing else
```

`check_modules()` refuses to start a pipeline whose module declares no `output_model`,
as `check_apps` refuses an app with a socket and no `WS_MESSAGE`.

**Every pipeline runs every entry in `MODULES`, and that list is code.** Selecting a
subset per deployment (`PSWAMP_MODULES=…`, per-module `{SLUG}_{SETTING}` overrides in
the client-config style) is a small later addition if a deployment ever asks; it is
deliberately *not* part of the first landing, because nothing needs it yet and a
hard-coded list is the simplest thing that keeps `main` runnable. The one exception
worth building in from the start is a flag on the package, `ON_DEMAND = True`, for a
batch-report module (§4.6) that should be constructed only when a job asks for it.

**What "simple to add" means here, counted.** A page never assembles a pipeline: there is
one per client and it runs the whole list, so a new analysis reaching the screen is
*add a module* on one side and *add a panel that subscribes to its result model* on the
other, with the generated contract as the only thing between them. For a
voltage-stability panel that is: the module package plus its `MODULES` entry (both
written by the scaffold); a page package under `pswamp_web/` whose endpoint is three
lines — `connected_hub`, `event_queue(hub.bus, VoltageStabilityResult)`,
`serve_updates` — plus `WS_MESSAGE` and an `APPS` entry; and the client's existing
four panel edits from AGENTS.md ("Adding a p-SWAMP view"). Gone, relative to the PoC:
the `Hub._start` wiring, the topic constant, the store, the `adapt.py`, the
hand-written wire model, and any thinking about threads. Still by hand: the page
package and the panel folder, both copies of an existing one. The follow-up that makes
this one command is `generate-new-module.sh --with-panel`, rendering all three sides
from one slug (§9).

`scripts/generate-new-module.sh <slug> "<Name>"` renders a `TimeWindowApp` subclass
with a `ResultEnvelope` subclass, a `MODULES` entry and a conformance test — the
analysis half of what `generate-new-subapp.sh` already does for the page half (STEP 1
A3). The **module conformance test** feeds the recording through a `GatewayIO` over a
`RecordingClient` on an `InProcessBus`, asserts that every published message validates
against `output_model`, that status leaves `INITIALIZING`, and — for the islanding
example — the sanity values AGENTS.md lists (stations 6500, 6700, 6701; groups summing
to 44).

**Carries:** A3, A6; A4's "same class, any host".

**Open.** Whether `N4SIDApp`'s multiple inheritance (`N4SID, TimeWindowApp`,
`n4sid.py:99`) survives `ABC` on the base; and where `FFTOnline`'s hand-rolled Kafka
rewind (`fft.py:57`) goes — proposal: it becomes a plain `seek_relative_input_offset`
now that the call is honoured by every source that can.

### 4.6 L6 — Pipeline, streams, sessions and jobs (`pswamp/pipeline.py`)

**Purpose.** Decide the unit of isolation (STEP 1 §5.1) and give A7 what the PoC's
`HubRegistry` gave it, in the core, without a web server in the picture.

**New.** A `Pipeline` is one *stream* — a gateway and a cursor — with its player, its
bus and its module instances:

```python
class Pipeline:
    def __init__(self, key: str, gateway: DataGateway, modules: list[ModuleEntry],
                 *, loop: asyncio.AbstractEventLoop | None = None, start: datetime | None = None): ...
    player: Player; bus: Bus
    def start(self) -> None; def stop(self) -> None; def dead_threads(self) -> list[str]

class PipelineRegistry:                      # HubRegistry, generalised and moved down
    def __init__(self, *, max_pipelines: int, idle_seconds: float): ...
    async def acquire(self, key: str) -> Pipeline          # per-key lock; CapacityError over the cap
    def peek(self, key: str) -> Pipeline | None            # what a command uses; never builds
    async def release(self, key: str) -> None; async def stop_all(self) -> None
```

**The isolation decision.** *Live:* one pipeline per process, keyed by the stream
name; every client sees the same instant, which is what a control room means. *Replay:*
one pipeline per client cursor, keyed by client id, because a visitor exploring recorded
data wants their own clock (rig doc, "one timeline per viewer"). Both are the same class
with a different key. What is per-client in either mode is identity, cursor, view state
(channel selection, acknowledged alarms) and job results; what is shared in live mode
is the analysis. This is STEP 1 §5.1's "likely answer", now the answer.

**The cost, honestly.** The draft's `consume` iterator replaces the PoC's per-client
player thread and its copy of the reader, not the modules: a replay pipeline still runs
one thread and one window per module, so it costs what a `Hub` costs today minus one
thread and minus the pre-decompressed recording (already shared, `replay.py:42`). The
cap (`MAX_PIPELINES`, `hub.py:70–75`) therefore stays and is sized as *modules ×
pipelines*; the memory story in the rig doc still holds. The win from the draft is
elsewhere: the *live* case, where the PoC would have copied the analysis per client and
this design runs it once.

**Sessions, correlation and jobs.** The web edge keeps `client_id` as the routing key and
`SessionRegistry` (`pswamp_web/sessions.py:40`) for per-view state; nothing in the core
knows what a browser is. Two additions make A7's "individual results back" true for
work that is not a socket tick:

- Every `Command` carries `request_id` and, when issued from the edge, `client_id`;
  every `ResultEnvelope` or `JobResult` produced *in answer to* a command carries the
  same `request_id`. `SessionRegistry` maps `request_id → client_id` for the life of
  the request, so a result on a shared live bus reaches only its issuer.
- A **job** is a module whose `run_analysis` is invoked once over
  `gateway.consume(model, t0, t1)` rather than per tick: `POST /api/<app>/report`
  → `CommandAck{request_id}` → the module runs on its thread to completion → `JobResult`
  on the bus → the page's socket delivers it (or a `GET …/jobs/{request_id}` reads it
  back once). Raw rows never leave the backend; the browser sees only the derived result
  (rig doc's K3 rule; STEP 1's scope decision). This is the request/response slot ADR-003
  foresaw — *"if the socket protocol grows bidirectional channels"* — answered without
  making the socket bidirectional.

**Carries:** A7; the A4/A7 tension of STEP 1 §5.1; the batch half of A5.

**Open.** `replicas > 1` remains the known blocker (port doc §10.2, *"the single largest
piece of unscoped work"*). This design does not make it harder — no provider state is
held in the web process, and a pipeline key is a string a store could own — and does
not do it.

### 4.7 L7 — Hosting and configuration

**Purpose.** A4: the same module in the same process as the web (or Qt) app, or as a
separate service, as a *deployment* choice.

**Three hosts, one class** (§3.2). (a) In the web server: `pswamp_web`'s lifespan binds a
`PipelineRegistry` to uvicorn's loop, as `REGISTRY` is bound today. (b) In Qt: a
`Pipeline` given `loop=None` starts its own loop thread for the gateway and bus; how
messages reach Qt widgets (signals from a bus listener) is *not designed here*. (c) As a
service: `python -m pswamp.runmodule islanding` builds one module with
`io=GatewayIO(GatewayBus(kafka_gateway))` — a Dockerfile target and a compose profile
(`--profile kafka`) plus a `k8s/` manifest, per the contributor rule that a service
exists in both, with the compose Kafka as the local stub. **(c) exists on paper until
principle 7 is met**: a load generator (N simulated clients at realistic rates) and an
end-to-end timestamp are prerequisites, and they are in §9.

**Configuration** is the draft's scheme (§4.2): environment variables, one block per
named client, `show_config` printing the table, model classes never read from the
environment. The server's two variables today (`HOST`, `PORT`, `server.py:290`) gain
`PSWAMP_DATA_CLIENTS`, `PSWAMP_STREAM` (namespace), `PSWAMP_MAX_PIPELINES` and the
per-client blocks. The module list stays in code (§4.5). A TSO deployment is an image,
a `PSWAMP_DATA_CLIENTS` value naming their own client package, and that package's
variables — no fork, no config in the repo.

**What does not ship in the server image:** `synchrophasor`, `aiokafka`, database
drivers, Qt. They arrive with the client or host that needs them (AGENTS.md's existing
rule, now with a mechanism). The default compose stack registers no `PRODUCE` client
writing to disk, so the "no persistent volume under `app/`" rule holds; an archive
example, if wanted, is a profile with a tmpfs.

**Carries:** A4 (designed; proven when (c) runs against numbers), A8's "no deployment
infra in the repo".

### 4.8 L8 — The web edge, adapted (`app/server-python/src/pswamp_web/`)

**Purpose.** Keep the browser contract; replace the PoC internals with the core (STEP 1
§2.8's three columns).

**Kept verbatim:** `POST /api/<app>/…` up, one socket down, `CommandAck` never carries
state; the generated OpenAPI with `x-websocket-channels`; `AppEntry`/`router`/
`WS_MESSAGE`/`lifespan` and `check_apps`; `client_id` as routing key; `SessionRegistry`;
`send_state` as the one serialiser; `serve_ticks`/`serve_updates`/`wait_for_disconnect`;
the failure codes (422/404/1008/1013/1011). The package stays self-contained and
relative-importing, so the port doc §7 question stays open at no cost.

**Replaced or reshaped:**

| today | becomes |
|---|---|
| `Hub` with three apps hard-wired (`hub.py:107–173`) | `Pipeline` from the core, module list from `MODULES` |
| `pswamp_web/bus.py` | `pswamp.bus.InProcessBus`; `Bus.subscribe` (dead today) is what pages use |
| `HubRegistry` (`hub.py:305`) | `pswamp.pipeline.PipelineRegistry`; `connected_hub` / `live_hub` keep their names and their close-code semantics at the edge |
| `recorded_io.py`, `replay.py` | `RecordingClient` + `Player` in the core; `CountingTimeWindowLabeled` deleted once `n_appended`/`snapshot()` land upstream (port doc §11) |
| per-page `adapt.py` (`islanding/adapt.py`) | deleted — a page subscribes to a `ResultEnvelope` subclass and forwards it; the main-system reconstruction moves into `detect_islands` upstream (port doc §11) |
| `stores.py` | subscriptions on the core bus; `AlarmStore` stays as the acknowledge/silence state machine |
| `ReplayStatus` (`wire.py:380`), read-only | `PlayerStatus` on a `source` app's socket |
| — | **`source` app entry**: `POST play / pause / step / seek / speed`, each `live_hub(client_id).player.<verb>()`, each a 404 with no pipeline, each refused with 409 when `PlayerStatus.can_seek` is false — so the client renders controls from status, never guesses |
| `CommandAck{status, applied}` | `+ request_id` — a breaking change → `API_VERSION` 2.0.0 |

Every socket message that is today a page-local pydantic model becomes either a core
message (`AppStatusMessage`, `Alarm`, `IslandingResult`) or stays a page-local *view*
model (`TimeWindowSlice` with its `full`/`append` delta protocol, which is edge
mechanics and rightly so). The generated `schema.ts` therefore starts carrying core
models; nothing renames them on the way (AGENTS.md's rule).

**Carries:** the browser-facing half of A5 and A7.

## 5. Package layout after landing

```
src/pswamp/
├── messages/                 L1 — DataModel (draft), PmuHeader, PmuFrame, ResultEnvelope, AppStatus,
│   ├── data_model.py             Alarm, Command, CommandAck, JobResult, PlayerStatus, StreamChanged
│   ├── measurements/         draft's per-PMU models, kept; frame.py new
│   └── results.py
├── datagateway/              L2 + L3 — the draft's package, names intact
│   ├── data_client_model.py  DataClient, Capability
│   ├── time_range.py  planner.py  stream.py  data_gateway.py  config.py
│   ├── player.py             new
│   ├── conformance.py        new — the client-parameterised suite
│   └── clients/              in_memory.py  csv_file.py  kafka_bus.py  recording.py (new)
├── bus/                      L4 — Bus protocol, InProcessBus, GatewayBus
├── app_templates/            L5 — SnapshotApp, TimeWindowApp (formalised), AppIO, GatewayIO,
│   └── registry.py               ModuleEntry / MODULES / check_modules, status_reporting
├── pipeline.py               L6 — Pipeline, PipelineRegistry
├── runmodule.py              L7 — `python -m pswamp.runmodule <slug>`
├── monitoring/               unchanged classes, each package exporting APP + DEFAULTS
├── streaming/                legacy AppIO adapters (kafka_io, nqkafka_io, mqtt_io), annotated;
│                             time_series_io.py deleted; retired when examples/ are ported
├── utils/                    time_window (+ n_appended / snapshot), time_window_labeled, pypmu
├── test_utils/sample_datasets/n44/recordings/n44_line_trip_50hz.npz   (moved from pswamp_web/data/)
└── gui/, visualization/, models/, coordination/   untouched by this track

app/server-python/src/pswamp_web/     L8 — page packages, sessions, pump, wire (view models only),
                                      source/ (new app entry); hub.py / bus.py / recorded_io.py /
                                      replay.py / stores.py / */adapt.py removed or reduced
```

Both of the port doc §7 moves remain a `git mv`: everything new sits under `pswamp/`
already, and `pswamp_web/` imports it inward as it imports `pswamp.*` today.

## 6. Walkthroughs

**6.1 A browser opens the grid monitor (replay, default deployment).** Five sockets
arrive with one `client_id`; `connected_hub` → `REGISTRY.acquire(client_id)` builds a
`Pipeline` keyed by the client: `DataGateway([RecordingClient])`, a `Player` in
`replay` mode at `t=0`, an `InProcessBus`, and one instance per `MODULES` entry, each
with a `GatewayIO`. The player thread pulls the first second of frames, each module's
`GatewayIO` answers `get_config_frame()` from the recording's `PmuHeader`, primes its
window by a bounded consume, and the modules run as they do today. Results are
`ResultEnvelope`s on the bus; the islanding page's `serve_updates` forwards them; the
time-window page's `serve_ticks` reads the store's window at 10 Hz. Half a minute in,
the trip.

**6.2 The operator scrubs back to the trip.** `POST /api/source/seek {"to": …}` with
`?client_id=` → `live_hub(client_id)` (no pipeline: 404) → `PlayerStatus.can_seek`
(false in live mode: 409) → `player.seek(t)` closes the stream, opens a new one at `t`,
publishes `StreamChanged`. Each `GatewayIO` drains, re-primes at `t`, calls `reset()`.
The reply was `CommandAck{applied: "seek", request_id}`; the new `PlayerStatus` and the
re-filled window arrive on their sockets. Nothing was rebuilt.

**6.3 A TSO runs it live, on their data.** Their image adds one package,
`acme_tso.pmu`, with a `TimescaleClient(DataClient)` (`HISTORY_CONSUME`, `from_env`,
passes the conformance suite) and a `KafkaClient` configured for their PDC feed;
`PSWAMP_DATA_CLIENTS=history:acme_tso.pmu:TimescaleClient,live:pswamp…:KafkaClient`,
`PSWAMP_STREAM=no`. The pipeline key is the stream name, so every operator shares one
analysis; the player is in `live` mode, `can_seek` false, the client renders no scrub
bar. An operator opening an alarm's history asks a *batch module* for it (6.4), not the
raw range. No file in the repo changed.

**6.4 A batch report.** `POST /api/oscillation-report/run {"t0", "t1"}` → `CommandAck{request_id}`
→ the report module (a `SnapshotApp` with `run_once=True`) consumes the range from the
gateway on its thread, runs N4SID over it, publishes `JobResult[ModeEstimate]` carrying
`request_id` → `SessionRegistry` resolves the id to the issuing client → the page's
socket delivers it. The browser saw a derived result of a few kilobytes; the range of
raw PMU data stayed in the backend.

**6.5 A contributor adds a module.** `./scripts/generate-new-module.sh voltage-stability
"Voltage Stability"` writes `src/pswamp/monitoring/voltage_stability/` (a
`TimeWindowApp` subclass with `output_model = VoltageStabilityResult`), a `MODULES`
entry, and a conformance test that already passes on the recording; the contributor
replaces `run_analysis`. `./scripts/generate-new-subapp.sh` (or a panel, per AGENTS.md)
gives it a page; the generated contract carries `VoltageStabilityResult` to
`schema.ts`. Four edits on the analysis side, four on the page side, as STEP 1 A3 asked.

## 7. Coverage

| req. | component(s) | status after this design |
|---|---|---|
| **A1** domain model | `messages/` — `DataModel` (draft), `PmuHeader`/`PmuFrame`, `ResultEnvelope`, `AppStatus`, `Alarm` | covered; canonical broker form measured before final |
| **A2** pub/sub over topics | `bus/` (`InProcessBus`, `GatewayBus`); topic = model class | covered |
| **A3** module in → out, easy to add | module contract, `MODULES`, `generate-new-module.sh`, conformance | covered |
| **A4** in-process or service | same class, three hosts; `runmodule`; compose + k8s profile | **designed; proven after numbers** |
| **A5** upstream data commands | `DataGateway.consume` (seek, range — draft), `Player` (pace, step, speed, mode), jobs | covered |
| **A6** module contract | `SnapshotApp`/`TimeWindowApp` formalised, `AppIO`, `check_modules` | covered |
| **A7** multi-client | `PipelineRegistry`, `client_id` + `SessionRegistry` (edge), `request_id` routing | covered for one replica; the multi-replica store stays out of scope |
| **A8** provider contract, example, no infra | `DataClient` (draft), `RecordingClient`, conformance suite, entry points, env config (draft) | covered |

STEP 1 §5's tensions, resolved: (1) unit of isolation — per stream, shared live, per
client replay (§4.6); (2) replay controls vs live — a mode derived from capabilities
(§4.3); (3) stateless vs history — history with the provider (§4.2); (4) pickle —
`DataModel` everywhere, a deliberate wire break (§4.1); (5) request/response — jobs by
`request_id` (§4.6); (6) where shared Python lives — unchanged, both moves cheap (§5).
STEP 2 §6's addition, pull vs push: **pull at the provider seam** (a TSO implements
`consume`, the simpler thing to write and to test), **push at the bus** (a module
subscribes); `GatewayBus` is the adapter between them, and the choice is argued rather
than inherited.

## 8. Louis's contribution, made visible

The draft is the provider layer of this architecture, and the message base under it.
The table says what stays his by name, what is reshaped and why, and what is deferred —
so a reader can tell the lift from the additions. **Recommendation:** the lift (§11
step 1) is his pull request, with the fixes listed in §4.2 as review items rather than
rewrites.

| lifted verbatim (name, concept, mostly code) | adapted (concept his; shape changed for the row-per-instant core) | deferred / own track |
|---|---|---|
| `DataModel` — `version` literal, `mRID`, UTC `timestamp`, class-derived `topic` (`draft:data_model.py`) | measurement model: per-PMU objects kept, frame added as canonical in-core form; `timestamp` required | `DataHub`, CIM profile, GraphDB, converters (`draft:core/datahub/`, `converter/`) — the grid-model provider, a track of its own (STEP 2 §4) |
| `DataClient`, `Capability`, the three `consume` rules (`draft:data_client_model.py`) | `InMemoryClient` → N-subscriber fan-out; becomes the reference for the in-process default | `branch` — dropped; environments are deployed separately, and the stream axis is a configured namespace instead (§4.1) |
| `TimeRange`, `Coverage` (`draft:time_range.py`) | `KafkaClient` — coverage measured, partitioning rule written; the second shipped example, behind a profile, after numbers | the rtsim example as files — its *shape* (simulator publishes through the gateway) is what `examples/nordic44_rtsim` should become |
| `SegmentPlanner`, `DataStream`, `DataGateway` (`draft:planner.py`, `stream.py`, `data_gateway.py`) | `from_env` hoisted to the ABC; a client list from configuration | `events/` stub; `fastapi`/`uvicorn`/`proton-driver` deps; `loguru` |
| `EnvSetting`, `env_*`, `format_settings`, `show_config` (`draft:config.py`) | the tests → a client-parameterised conformance suite | Python 3.12-only syntax until the pin moves |
| `test_data_gateway.py`, `test_csv_client.py`, `test_client_config.py` as the first cases | | |
| the argument against pickle (`draft:examples/simulation_task/demo.ipynb`), now principle 3 | | |

Concepts of his that shaped layers he did not write: capability gating (principle 4)
is what makes the player's `mode` derivable; "history lives with the provider"
(`CsvClient` as an archive) is what reconciles A5 with the no-database rule; produce
fan-out is what makes a bus double as an archive; and "seek = a new stream" (§4.3) is
the draft's implicit model made explicit.

## 9. What we need in addition to the draft

The open threads, grouped by layer — everything this design relies on that the draft
does not supply. Not sized: these are the points to keep track of, not a schedule.
"Needs" names the item a thread cannot land before.

**L1 Messages**
- `PmuHeader` + `PmuFrame`, with `freq_encoding` decided and `quality` placed (grown from `Recording` + `LabeledRowDecoder`)
- `ResultEnvelope[T]`, `AppStatus`, `AppStatusMessage`, `Alarm`, `Command`, `CommandAck{request_id}`, `JobResult`, `PlayerStatus`, `StreamChanged`
- drop `branch`; stream `namespace` from configuration; overridable `topic`; splitter fix
- `PmuFrameAssembler` (per-PMU objects → frames at the gateway edge) — only when a deployment ingests per PMU; needs the §4.1 measurement

**L2 Providers**
- `RecordingClient` over the `.npz` recording; `tools/record_n44_dataset.py` writes it as `PmuHeader` + frames
- client-parameterised conformance suite (+ live tail, produce ordering) — needs the draft lifted
- `from_env` on the ABC; `PSWAMP_DATA_CLIENTS` composition; entry-point group
- the lift's fixes: N-subscriber `InMemoryClient`, measured Kafka coverage, produce failures surfaced, 3.11, `logging`

**L3 Player**
- `Player` (thread, batched pull, pace / pause / step / speed / seek, mode, `PlayerStatus`, loop as a new stream per pass)
- overflow policies per mode (drop-oldest live, stall replay) — part of the above

**L4 Bus**
- `Bus` protocol, `InProcessBus` (from `pswamp_web/bus.py`, typed on models, overflow policy)
- `GatewayBus` (one gateway subscription per model per process) — needs L2

**L5 Modules**
- `AppIO` protocol; legacy adapters annotated; `time_series_io.py`, duplicate `PMUDecoder`, `fft_v2.py` deleted
- `GatewayIO` (queue, header rule, pre-fill, reset, time-base conversion, command delivery) — needs L3, L4
- `SnapshotApp`/`TimeWindowApp` formalised (abstract `run_analysis`, `output_model`, `AppStatus`, `reset`, `handle_command`, `open_console` retired; `ReportingApp`/`AlarmHandler` emit models) — one existing app (islanding) moved first
- `ModuleEntry`/`MODULES`/`check_modules`; `generate-new-module.sh`; module conformance test
- `generate-new-module.sh --with-panel`: one slug renders the module, the `pswamp_web/` page package and the client panel folder — after the above and the web re-point; the step that makes "analysis to screen" one command
- upstream fixes the web works around today: `detect_islands` overlapping groups, main-system reconstruction, `n_appended`/`snapshot()` on `TimeWindow` (port doc §11)

**L6 Pipeline**
- `Pipeline`, `PipelineRegistry` (from `HubRegistry`, no fastapi import), keying rule live vs replay
- `request_id → client_id` in `SessionRegistry`; job pattern (`run_once` module, `JobResult` delivery, optional `GET …/jobs/{id}`) — needs L5

**L7 Hosting**
- `pswamp.runmodule` CLI, Dockerfile target, compose `kafka` profile **and** `k8s/` manifest — gated on numbers
- **load generator + end-to-end timestamp** (port doc §13, STEP 1 "nothing is measured") — prerequisite for L7(c) and for the §4.1 measurement
- env variables for the server (`PSWAMP_DATA_CLIENTS`, `PSWAMP_STREAM`, `PSWAMP_MAX_PIPELINES`)

**L8 Web edge**
- re-point `pswamp_web/` (Hub → Pipeline, bus, registry, stores → subscriptions, `adapt.py` deleted, recording moved)
- `source` app entry (five POSTs, `PlayerStatus` socket, 409 gating); `CommandAck.request_id`; `API_VERSION` 2.0.0; regenerate contract
- AGENTS.md rewrite of the web-layer invariants; `doc/the-client-server-api.md` gains the job pattern

**Cross-cutting**
- ADRs (§10)
- `examples/` ported off `config['topics']` onto `MODULES` + a gateway, then `streaming/` legacy adapters retired — last

Two of these dominate the rest in scope — the web re-point and the examples port — and
everything up to L6 is provable with pytest alone.

## 10. Decisions to record (ADR candidates)

Each with the default this document proposes; each is a `doc/adr/` entry when it lands
(template: context / decision / consequences / alternatives).

| ADR | decision | proposed default |
|---|---|---|
| 005 | **Wire format** | every message is a versioned pydantic `DataModel`; topic = model name under a configured stream namespace, no per-message `branch`; pickle retired; a deliberate break for existing Kafka consumers |
| 006 | **Provider contract** | the draft's `DataClient` with capabilities, pull-based `consume` over a `TimeRange`; history lives with the provider; conformance suite is the acceptance test |
| 007 | **Bus** | in-process default with an explicit overflow policy; brokers as `GatewayBus`; all thread↔loop crossings in `bus/` and `GatewayIO` |
| 008 | **Unit of isolation** | a pipeline per stream: shared for live, per client cursor for replay; cap sized by modules × pipelines |
| 009 | **Seek semantics** | seek = new stream + `StreamChanged`; modules re-primed by `GatewayIO`, never rebuilt; pause is view state in live mode |
| 010 | **Request/response** | jobs by `request_id` on `Command`/`CommandAck`/`JobResult`; the socket stays downstream-only; raw ranges never reach the browser |
| 011 | **Module contract** | `SnapshotApp`/`TimeWindowApp` with abstract `run_analysis` and declared `output_model`; `MODULES` registry checked at startup; `open_console` retired |
| — | measurement shape on a broker (frame vs per-PMU) | **not yet**: measured first (§4.1) |

## 11. Landing order

STEP 1 §8 revised with the draft's inputs. Steps 1–6 change no user-visible behaviour
and are provable without a web server; 7 is the first visible change; 8–9 extend it.

1. **Lift the draft** into `pswamp/messages/` and `pswamp/datagateway/` with the §4.2
   fixes, the draft's tests running under `run-core-python-tests.sh`'s hermetic subset,
   and ADR-005/006. *Louis's PR.*
2. **`PmuHeader`/`PmuFrame` and the result models**; `RecordingClient`; the
   conformance suite parameterised and passing on `InMemoryClient`, `CsvClient` and
   `RecordingClient`.
3. **`Player`** over the gateway, exercised by a core test and a tiny CLI
   (`python -m pswamp.datagateway.player --seek 20s`), no browser. ADR-009.
4. **`Bus`** (`InProcessBus`), **`AppIO`**, **`GatewayIO`**; `IslandingApp` runs unchanged
   over `RecordingClient` in a pytest, asserting the AGENTS.md sanity values. ADR-007.
5. **Module contract, `MODULES`, scaffold, module conformance test**; islanding and
   line-outage moved onto it; `detect_islands` and `TimeWindow` fixes upstream. ADR-011.
6. **`Pipeline` / `PipelineRegistry`** in the core with the registry tests from
   `app/server-python/tests/test_hub_registry.py` moved down and re-keyed. ADR-008.
7. **Re-point the web layer**; `source` app; `CommandAck.request_id`; `API_VERSION`
   2.0.0; AGENTS.md rewritten; `e2e-smoke-test.sh` extended with a seek. ADR-010 lands
   with the job pattern here or in 9.
8. **Numbers**: load generator and end-to-end timestamp; the §4.1 broker-shape
   measurement using the draft's rtsim.
9. **`GatewayBus` + `KafkaClient` behind a compose profile and a `k8s/` manifest**, the
   first out-of-process module via `runmodule` — if and as the numbers say; the first
   batch-report module over a range query.
10. **Port `examples/`** onto `MODULES` and a gateway; retire `streaming/`'s legacy
    adapters.

## 12. Risks, measurements first, and what stays undecided

**Risks.** (1) The frame-versus-per-PMU broker shape is decided by a number nobody has
yet; landing per-PMU topics before it would fix ~6,600 messages/s into TSO deployments.
(2) The player-on-a-thread choice is the PoC's pattern carried forward, not measured
against a coroutine at scale; the load generator settles it. (3) Retiring
`open_console` and pickle is a wire break for any desktop consumer still on Kafka
topics — bundle it with the ADR and say so in the release note. (4) The examples port
(step 10) is the largest unglamorous item and the one that keeps `main` runnable only if
the legacy adapters stay until it is done.

**Measure first** (STEP 1 assumption "nothing is measured yet"): frames/s per pipeline
on the loop versus on a thread; bytes/s and CPU per message shape on Kafka; end-to-end
latency tick → paint under N clients; RSS per pipeline with the module set. These are
step 8 and gate steps 9 and the broker-shape default.

**Left open, on purpose:** the Qt path's lifetime and therefore the port doc §7 move
(this design is indifferent to it); the CIM/grid-model provider (a sibling contract to
`DataClient`, STEP 2 §4); the external store for `replicas > 1`; auth and CORS narrowing
when anything real sits behind an endpoint (rig doc); the desktop bus → Qt delivery.
