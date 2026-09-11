# STEP 3 — Target architecture for data flow and module integration

> **Status:** WIP proposal, third step of the data-integration track. Synthesises
> `STEP1-WIP-data-integration-context.md` (requirements A1–A8, where we are, what is
> missing) and `STEP2-WIP-data-integration-evaluate-initial-louis-design-draft.md`
> (the `../test_pswamp` data-gateway draft: what to lift, what to add, what to park)
> into one target architecture. Nothing here is implemented; §12 is the order in which
> it would be. Where STEP1 says a decision is settled it is respected; where STEP1 or
> the port document says a question is open and this document takes a position, §15
> lists it as a decision to confirm, not as a fact.
>
> **Credit.** The provider and gateway half of this architecture — contracts 2 and 3
> and the base of contract 1 — is Louis Pauchet's design from `../test_pswamp`, lifted
> largely as written. The one idea everything else here is arranged around (a consumer
> names a model and a time range, never a source, and a backend declares what it can
> do and what it holds) is his. §13 lists every file of his that survives and how.
>
> **Two things this document does on purpose.** It names every piece of Louis's draft
> that survives, by file, so the work is carried forward rather than re-derived (§13). And
> it keeps the analysis modules' code and execution model as they are — the
> architecture wraps them, it does not rewrite them (STEP1 §6, port document §2).
>
> **Vocabulary.** *Provider* / *client*: a backend that holds or produces data
> (`DataClient`). *Gateway*: the router over clients. *Module*: an analysis application
> (today `SnapshotApp` / `TimeWindowApp` subclasses). *Message*: a typed payload
> (`DataModel`). *Pipeline*: one stream plus the modules running on it plus their
> stores. *Session*: one browser's identity and view state. *Namespace*: the
> deployment-level prefix in every topic (the draft's `branch`).

## 0. The one-page version

```
                         ┌──────────────────────────── one process (default) ────────────────────────────┐
  deployment-side        │                                                                                │
  providers              │   pswamp.data                   pswamp.modules                pswamp_web (edge)  │
                         │                                                                                │
  ┌────────────┐ consume │ ┌──────────┐ DataStream  ┌──────────┐ thread  ┌────────────┐                    │
  │ Recording  │────────▶│ │          │───────────▶ │ ModuleIO │───────▶ │ Islanding  │──┐                 │
  │ (.npz)     │         │ │          │  (paced)    │ (queue)  │         │ (unchanged)│  │ produce(Result) │
  ├────────────┤         │ │ Data-    │             └──────────┘         └────────────┘  │                 │
  │ CSV archive│────────▶│ │ Gateway  │                 …one ModuleIO + thread per module…│                 │
  ├────────────┤         │ │ planner/ │◀──────────────────────────────────────────────────┘                 │
  │ Kafka      │◀───────▶│ │ stitch   │                                                                     │
  ├────────────┤         │ │          │ consume(IslandingResult) ┌─────────┐  ws push   ┌─────────┐        │
  │ Timescale* │────────▶│ │          │─────────────────────────▶│ stores  │───────────▶│ pages   │──▶ browser
  ├────────────┤         │ │          │                          └─────────┘            └─────────┘        │
  │ live PDC*  │────────▶│ └──────────┘                                                      ▲             │
  └────────────┘         │       ▲  InMemoryClient = the in-process bus (fan-out, LIVE|PRODUCE)│ POST cmd    │
   * written by a        │       │                                                            │             │
     deployment, not     │  PipelineRegistry: shared pipeline for live, one per client for replay          │
     in this repo        └──────────────────────────────────────────────────────────────────────────────────┘
```

Six contracts, each owned by one package and each testable without the others:

| # | Contract | Package | Origin |
|---|---|---|---|
| 1 | **Messages** — every payload is a versioned, JSON-native pydantic `DataModel` that knows its topic; measurement header + sample; result envelope; status; command ack | `pswamp.data.models` | Louis's `DataModel` (lifted), measurement layer new |
| 2 | **Providers** — `DataClient` with `Capability` flags and `Coverage`; `DataGateway.consume(Model, start, end)` returns one stitched stream; `produce(msg)` fans out | `pswamp.data.gateway`, `pswamp.data.clients` | Louis (lifted) |
| 3 | **Bus** — the in-process fan-out is one `DataClient` (`InMemoryClient`) with `LIVE_CONSUME | PRODUCE`; a broker is another client of the same interface | `pswamp.data.clients.in_memory`, `.kafka` | Louis (adapted: fan-out) |
| 4 | **Modules** — `AnalysisModule`: declared input, output model, window, rate; `run_analysis` abstract; `reset()`; runs on a thread over a `ModuleIO` fed by a `DataStream`; discovered by name; scaffolded; conformance-tested | `pswamp.modules` | this repo (`SnapshotApp` formalised) |
| 5 | **Pipelines and sessions** — a pipeline is a stream + modules + stores; live pipelines are shared, replay pipelines are per client; source commands (seek, pace) are pipeline operations; jobs carry a request id | `pswamp.modules.pipeline` | this repo (`HubRegistry` generalised) |
| 6 | **Browser edge** — commands up as POST, state down on a socket, generated OpenAPI with socket channels | `pswamp_web` | ADR-003, unchanged |

Louis's gateway is the spine (contracts 2 and 3 and the base of 1); this repo's
application templates and web registry are the rest, formalised. Nothing needs a
broker, a database or a second process to run; each of those is a client swapped in
by configuration.

## 1. Principles the architecture is derived from

Each follows from a constraint in STEP1 or a document it cites. They are the test for
any later change: a proposal that breaks one needs an ADR.

1. **Core first; the web layer is an adapter.** Every contract lives under
   `src/pswamp/` and is provable with no web server (STEP1 preamble). `pswamp_web/`
   consumes it and keeps only the browser edge.
2. **Everything on a topic is a typed message.** JSON-native, versioned, validated on
   read; pickle is gone (STEP1 A1, the added assumption). In-process no serialisation
   happens, but the object *is* the schema.
3. **The consumer names a model and a time range, never a source.** Which backend
   answers is the gateway's business; a backend declares what it can do and what it
   holds (draft §1.2; STEP1 A5 "history lives with the provider", A8 capabilities).
4. **Transport-neutral, in-process by default.** The bus is a client; a broker is a
   client. Nothing in a module knows which (STEP1 A2, A4; rig doc "only complicate
   the rig when you have to").
5. **Modules keep their execution model.** A thread that blocks on the next sample.
   The adapter, not the module, does the async work, and there are exactly two
   thread↔loop seams in the process (STEP1 §6, port document §2).
6. **Isolation is per stream, not per copy of the source.** Clients are shared and
   read-only; a stream (and the modules on it) is shared for live data and per client
   for replay (STEP1 §5.1, STEP2 A7).
7. **Commands go up, state comes down.** A source command (seek, pace) is a pipeline
   operation exposed as a POST; the result arrives on the socket; a command never
   answers with state (ADR-003).
8. **The repo persists nothing; history lives with a provider.** A deployment's
   time-series database is a client, outside the "no database" rule (STEP1 §5.3).
9. **Every contract is discovered by name, checked at startup, generated into a
   document, scaffolded, and conformance-tested** — the pattern the page contract
   already follows (STEP1 A6).
10. **Numbers before splitting.** A module leaves the process only when a measured
    reason says so (contributor doc; STEP1 A4 item 4).

## 2. The layers, and what crosses between them

```
 L0  Providers        RecordingClient · CsvClient · InMemoryClient · KafkaClient · <deployment clients>
       │  DataClient: coverage() / consume(Model, TimeRange) / produce(msg)      [async]
 L1  Gateway          DataGateway → SegmentPlanner → DataStream (stitched, de-duplicated, optionally paced)
       │  AsyncIterator[DataModel]                                                [async]
 L2  Module I/O       ModuleIO: bounded queue, pre-fill, drop policy; the thread-facing face of a stream
       │  get_next_sample() blocks / publish(msg) hands back                      [thread ↔ loop seam #1]
 L3  Modules          AnalysisModule (SnapshotApp / TimeWindowApp) — run_analysis(t, window) → Result
       │  produce(Result | AppStatus | Alarm) → gateway → every PRODUCE client (bus, broker, archive)
 L4  Pipelines        Pipeline = stream spec + modules + stores; PipelineRegistry; ReplayControl; Jobs
       │  stores subscribe via gateway.consume(ResultModel) on the bus            [async]
 L5  Edge             pswamp_web: page packages, serve_ticks / serve_updates, POST commands, OpenAPI
       │  WebSocket (state down) / HTTP POST (commands up)
 L6  Browser          unchanged
```

The two thread↔loop crossings (principle 5): `ModuleIO.get_next_sample()` (loop →
thread, a queue) and `ModuleIO.publish()` (thread → loop, `call_soon_threadsafe`
into `gateway.produce`). The time window's own locked snapshot read stays as the
second seam it already is (STEP1 §"One event loop"); nothing else crosses.

## 3. Package layout

```
src/pswamp/
├── data/                      # contracts 1–3: the lifted gateway, under this repo's rules
│   ├── __init__.py            # re-exports the public surface (below)
│   ├── models/
│   │   ├── base.py            # DataModel, topic descriptor, namespace     ← draft data_model.py
│   │   ├── measurements.py    # StreamHeader, Channel, Sample, SampleBatch  (new, §4.2)
│   │   ├── results.py         # Result envelope, AppStatus, Alarm, Report   (new, §4.3)
│   │   └── commands.py        # CommandAck, JobAck                          ← pswamp_web/wire.py
│   ├── gateway/
│   │   ├── client.py          # DataClient, Capability, MRIDFilter, ModelSelector ← draft data_client_model.py
│   │   ├── time_range.py      # TimeRange, Coverage                          ← draft, verbatim
│   │   ├── planner.py         # SegmentPlanner, GapPolicy, Segment           ← draft, verbatim
│   │   ├── stream.py          # DataStream (de-dup key widened)              ← draft
│   │   ├── gateway.py         # DataGateway                                  ← draft
│   │   ├── pacing.py          # Pacer: speed, pause, step over any stream    (new, §5.4)
│   │   └── config.py          # EnvSetting, env_key, from-env helpers, build_gateway() ← draft + registry
│   ├── clients/
│   │   ├── in_memory.py       # InMemoryClient, now multi-subscriber = the bus ← draft, adapted
│   │   ├── recording.py       # RecordingClient over Recording (.npz)        ← pswamp_web/recorded_io.py, refitted
│   │   ├── csv_file.py        # CsvClient                                    ← draft, verbatim
│   │   └── kafka.py           # KafkaClient (optional extra)                 ← draft, verbatim
│   ├── conformance/           # importable test suite a deployment runs against its own client (§5.6)
│   │   └── __init__.py
│   └── data/
│       └── n44_line_trip_50hz.npz                                            ← pswamp_web/data/
├── modules/                   # contracts 4–5
│   ├── __init__.py
│   ├── base.py                # AnalysisModule ABC, ModuleStatus enum         (new, §7.1)
│   ├── templates.py           # SnapshotApp, TimeWindowApp on the ABC        ← app_templates/, kept names
│   ├── io.py                  # ModuleIO: the thread-facing adapter           ← recorded_io.RecordedIO, generalised
│   ├── runner.py              # ModuleRunner: thread, stop, join, dead detection ← hub.py pieces
│   ├── registry.py            # MODULES, discovery, check_modules             (new, §7.4)
│   ├── stores.py              # AlarmStore, StatusStore, IslandStore, OutageLog ← pswamp_web/stores.py
│   ├── pipeline.py            # Pipeline, PipelineRegistry, ReplayControl, Job ← hub.py HubRegistry, generalised
│   └── cli.py                 # `pswamp-modules run|replay|list` for core-only use (§10)
├── monitoring/                # unchanged subclasses: islanding.py, line_outage_detection.py, n4sid.py, fft.py
├── app_templates/             # thin re-export shims → modules.templates, kept for the Qt path; removed with it
├── streaming/                 # legacy transport (KafkaIO, NQKafkaIO, MQTT_IO): Qt path only; retired with it
└── …                          # gui/, visualization/, models/, utils/ untouched by this track

app/server-python/src/pswamp_web/        # contract 6 only
├── wire.py                    # VIEW models only (TimeWindowSlice, PhasorSnapshot, IslandingState, …) + send_state
├── pipelines.py               # connected_pipeline(ws), live_pipeline(client_id) — thin over pswamp.modules.pipeline
├── source/                    # NEW page package: replay/pace/seek POSTs + ReplayStatus socket (replaces pmu_test_streamer)
├── app_status/ grid/ time_window/ phasors/ islanding/ line_outage/   # page packages; adapt.py files deleted
├── pump.py sessions.py log.py # unchanged edge mechanics
└── (deleted) bus.py hub.py recorded_io.py replay.py stores.py data/  # moved to the core or replaced
```

Rules the layout carries:

- **`pswamp.data` and `pswamp.modules` obey the movability rule** `pswamp_web/` has
  today: relative imports inside, no import of anything outside `pswamp`, no
  FastAPI, no numpy-free pretence either (numpy is a root dependency and the sample
  path needs it). `pydantic` is the only new core requirement; `aiokafka` is an
  extra; `loguru`, `fastapi`, `uvicorn`, `proton-driver`, `cim-graph` from the draft
  are not carried (STEP2 §6.3).
- **Python 3.11**, matching both manifests (STEP2 §6.2).
- **The recording moves to the core** with its client. The web image already ships
  root `src/`, so nothing changes in the Dockerfile; `pswamp_web/data/` goes.
- **Where the shared Python finally lives (port document §7) is untouched.** Both
  packages are `git mv`-able in either direction; that question stays gated on the
  Qt decision as the port document says.
- **`app_templates/` and `streaming/` are not deleted by this track.** They serve the
  Qt path. They stop being the place new code goes.

## 4. Contract 1 — Messages (`pswamp.data.models`)

### 4.1 The envelope (lifted from Louis's draft)

```python
class DataModel(BaseModel):
    version: str                      # subclasses pin: version: Literal["v1"] = "v1"
    namespace: str = "live"           # the draft's `branch`; inserted into the topic (§4.5)
    mRID: str | None = None           # identifier: stream id for samples, app uuid for results
    timestamp: datetime | None = None # UTC-aware after validation; stamped by produce() if None
    topic: ClassVar[TopicDescriptor]  # "islanding.live.result" from the class name + namespace
```

Verbatim from `data_model.py` except the rename. Every message in the system is a
subclass. The desktop `{time_stamp, info{app_name, uuid}, parameters, result}` dict
and the four web wire "twins" (STEP1 §A1) collapse into this one base.

### 4.2 The measurement layer (new; replaces the draft's `mRID_*` models)

Decision, per STEP2 §6.6: **a header sent once, and array-valued samples ordered by
it** — the shape every existing module consumes (`TimeWindowLabeled` +
`generate_header`) and the shape `Recording` already stores.

```python
class Channel(BaseModel):
    station: str; channel: str; measurement: str      # the three header rows, as today
    unit: Literal["V", "A", "Hz", "Hz/s", "rad", ...]
    mRID: str | None = None                            # CIM equipment id when a deployment has one (§13: bridge to the CIM track)

class StreamHeader(DataModel):        # topic "stream.live.header"
    version: Literal["v1"] = "v1"
    stream_id: str                     # == mRID; what Sample.mRID refers to
    data_rate: float                   # Hz
    channels: list[Channel]            # column order of every Sample.values
    source: str = ""                   # provenance (recording file, PDC id)
    events: list[ScenarioEvent] = []   # from Recording.events; empty live

class Sample(DataModel):              # topic "sample.live"
    version: Literal["v1"] = "v1"
    mRID: str                          # stream_id
    values: FloatArray                 # (C,) numpy in memory; JSON: list[float | None] (NaN → null)
    quality: int | None = None         # C37.118 STAT word, when the source has it

class SampleBatch(DataModel):         # topic "sample.live.batch" — for range queries and archives
    version: Literal["v1"] = "v1"
    mRID: str
    timestamps: FloatArray             # (N,) epoch seconds
    values: FloatArray                 # (N, C)
```

Conventions written into the model docstrings and checked by the conformance suite:
time is epoch float seconds in the arrays and UTC `datetime` on the envelope; angles
are radians; magnitudes are SI; frequency is absolute Hz (STEP1 A1 item 2 decided:
the decoder converts C37.118 deviation encoding at the provider, so no module ever
sees it); NaN is legal in memory and becomes `null` on the wire (the one serialiser
rule STEP1 §2.8 keeps). `FloatArray` is a pydantic-annotated numpy type — the port
document's "one serialiser, `float | None`" rule, done once at the type.

What this drops from the draft, and why: `MeasurementPmuVoltage` /
`Current` / `Frequency` with dynamic `mRID_<channel>` extras and complex values.
Channel identity has to be a table, not field names, for modules to select by
station or measurement type; and three messages per sample per quantity is three
streams where the analysis wants one matrix. The idea that survives is the
per-channel `mRID` — it goes on `Channel`.

### 4.3 Results, status, alarms (new envelope over existing content)

```python
class ModuleRef(BaseModel):
    name: str; uuid: str

class Result(DataModel):              # base for every module output; topic from the subclass name
    module: ModuleRef
    parameters: dict[str, JsonValue] = {}
    request_id: str | None = None     # set when produced for a job (§8.4)

class IslandingResult(Result):        # "islanding.live.result"
    version: Literal["v1"] = "v1"
    islands: list[Island]; main_system: Island        # adapt.py's reconstruction moves *here*, done once

class LineOutageEvent(Result): ...    # "line.live.outage.event"
class ModeEstimate(Result): ...       # N4SID, when ported

class ModuleStatus(str, Enum): OK = "OK"; ALERT = "Alert"; EMERGENCY = "Emergency"; INIT = "Initializing"; UNDEFINED = "Undefined"

class AppStatus(DataModel):           # "app.live.status" — replaces ReportingApp's dict
    version: Literal["v1"] = "v1"
    module: ModuleRef; status: ModuleStatus

class Alarm(DataModel):               # "alarm.live" — replaces AlarmHandler's pickled dict
    version: Literal["v1"] = "v1"
    module: ModuleRef; type: AlarmEventType; message: str
    alarm_uuid: str                   # == mRID

class Report(Result): ...             # base for batch outputs (§8.4)
```

`islanding/adapt.py` and the other per-page adapters disappear: the module's
`run_analysis` returns the model. The two core gaps that adapter compensates for
(overlapping island groups, no station names) are fixed in `monitoring/islanding.py`
as the port document §11 asks, as part of the same change.

### 4.4 Commands

`CommandAck {status, applied}` moves from `wire.py` to `models/commands.py`
unchanged, plus `request_id: str` on it and a `JobAck(CommandAck)` with `job_id`
(§8.4). `ClientId` and `CLIENT_ID_PATTERN` stay in the edge; they are HTTP concerns.

### 4.5 Topics are derived, not configured

`IslandingResult.topic == "islanding.live.result"`; with `namespace="no"`,
`"islanding.no.result"`. The `[topics]` table in `config.toml` and the four string
constants in `hub.py` go. A deployment that must map a model onto an existing broker
topic uses `KafkaClient(topics={Model: "legacy.name"})`, which the draft already
provides. The multi-TSO example becomes two namespaces.

The catalogue is therefore *the set of `DataModel` subclasses the process imports*,
and `api_contract.py` can list it into the OpenAPI document the way it lists
`WS_MESSAGE` today (a new `x-topics` extension, one entry per model: topic,
direction implied by who produces it, `$ref` to the schema).

## 5. Contract 2 — Providers (`pswamp.data.gateway`, `pswamp.data.clients`)

### 5.1 The interface (Louis's, lifted verbatim)

```python
class Capability(Flag): LIVE_CONSUME = auto(); HISTORY_CONSUME = auto(); PRODUCE = auto()

class DataClient(ABC):
    name: str; capabilities: Capability; supported_models: set[type[DataModel]]; priority: int = 0
    env_settings: ClassVar[tuple[EnvSetting, ...]] = ()
    async def open(self) / close(self)
    @abstractmethod async def coverage(self, model, mRID=None) -> Coverage | None   # recomputed every call
    @abstractmethod def consume(self, model, time_range, mRID=None) -> AsyncIterator[DataModel]
    @abstractmethod async def produce(self, data) -> None
    @classmethod def from_env(cls, name, models, **overrides) -> Self
    @classmethod def show_config(cls, name) -> None
```

with the draft's three rules for `consume` (timestamped, non-decreasing, stops at
`end` unless live and open). `TimeRange`, `Coverage`, `SegmentPlanner`, `DataStream`,
`DataGateway` come across unchanged, with one edit: the stream's de-duplication key
becomes `(timestamp, model, mRID)` so two different models at the same instant never
collapse (STEP2 §6.6).

This is STEP1's "nine-method duck type implemented seven times" replaced by three
verbs and a declaration. The desktop `KafkaIO`/`NQKafkaIO`/`MQTT_IO` are *not*
ported onto it by this track; they serve the Qt path until that decision is made,
and `KafkaClient` supersedes the first.

### 5.2 Clients shipped in the repo

| Client | Capabilities | Role | Source |
|---|---|---|---|
| `RecordingClient` | `HISTORY_CONSUME` | **The reference provider.** `coverage()` = the file's span; `consume(range)` yields `Sample`s (or a `SampleBatch` for a range query); publishes its `StreamHeader` once. Loops when asked. Default when nothing is configured. | `Recording` + `LabeledRowDecoder` from `recorded_io.py`, refitted; the recorder tool moves with it |
| `InMemoryClient` | `LIVE_CONSUME \| PRODUCE` (+ `HISTORY_CONSUME` over its list, for tests) | **The bus** (§6). Always registered. | draft, adapted |
| `CsvClient` | `HISTORY_CONSUME \| PRODUCE` | Archive example; test double for a cold store. | draft, verbatim |
| `KafkaClient` | all three | Broker example, behind a compose profile; the A4 proof. | draft, verbatim |

Written by a deployment, never in the repo: a TimescaleDB/ClickHouse client
(`HISTORY_CONSUME`), a live PDC client wrapping `synchrophasor` (`LIVE_CONSUME`,
narrow now-relative coverage, decoding C37.118 into `Sample` at the source — which is
where `synchrophasor` and its FREQ/POLAR handling end up living, and why the server
image never needs it).

### 5.3 What "history with the provider" looks like in practice

A TSO deployment: `TimescaleClient(priority=10)`, `KafkaClient(priority=5)`,
`InMemoryClient`. A module opened with `consume(Sample, start=t_alarm - 60s)`
replays from the database, hands over to Kafka within `live_handoff_margin` of now,
then tails live — the planner's loop, unchanged from the draft
(`test_data_gateway.py:169-221` is the behavioural pin). The dev laptop:
`RecordingClient` only, and the same module code runs. Same for a batch report
over `(t0, t1)`.

### 5.4 Pacing and replay control (new)

`consume` is unpaced: an archive yields as fast as it reads. The monitor needs the
recording at wall-clock speed. So:

```python
class Pacer:
    """Async iterator wrapper: sleeps to each payload's timestamp delta scaled by speed."""
    speed: float; paused: bool
    def pause(self) / resume(self) / step(n=1) / set_speed(s)
def paced(stream: AsyncIterator[DataModel], speed=1.0, clock=time.monotonic) -> Pacer
```

Seek is not a `Pacer` method. **A seek is a new `consume`** starting at the target,
preceded by a bounded `consume(t - window, t)` that re-primes the module (§7.3). This
is the decision STEP1 A5 item 2 asked for ("either the pipeline is rebuilt on seek,
or applications get a `reset()` and the provider re-primes them — decide once"):
**reset and re-prime from the provider**, and the pre-fill burst the port document
§4.4 fought is gone because the history is delivered as one bounded stream before
the paced one starts, not as a burst into a live queue.

`live | replay` mode (port document §10.2) is `Coverage.live` of the segment the
stream is on; `ReplayStatus` reports it and the client shows controls only when the
stream is replayable.

### 5.5 Configuration (Louis's scheme, plus a registry)

```
PSWAMP_NAMESPACE=live
PSWAMP_CLIENTS=recording,bus                 # ordered client names; default when unset
RECORDING_TYPE=recording  RECORDING_PATH=<built-in n44>  RECORDING_PRIORITY=10  RECORDING_LOOP=true
BUS_TYPE=in_memory
ARCHIVE_TYPE=csv        ARCHIVE_DIRECTORY=/var/lib/pswamp/archive   ARCHIVE_PRIORITY=20
KAFKA_TYPE=kafka        KAFKA_BOOTSTRAP_SERVERS=kafka-1:9092  KAFKA_RETENTION_SECONDS=1200
TSDB_TYPE=my_tso.pswamp_clients:TimescaleClient   TSDB_DSN=...                 # out-of-repo class
```

`build_gateway_from_env()` in `config.py` resolves `<NAME>_TYPE` against built-in
names, an entry-point group `pswamp.data.clients`, or a `module:Class` path, then
calls that class's `from_env(name, models)`. `show_config()` prints the table for
any of them. With nothing set, the process is exactly today's dev experience: the
committed recording and the in-process bus.

### 5.6 The conformance suite

`pswamp.data.conformance` is an importable pytest module parameterised over a client
fixture: streams N samples in order; header matches; `coverage()` agrees with what
`consume()` yields; a bounded range stops at `end`; an open range on a
`LIVE_CONSUME` client keeps yielding; `produce` then `consume` round-trips (when
`PRODUCE`); `from_env` honours `show_config`. The draft's four test files are its
seed; the repo runs it against all four shipped clients, a deployment runs it against
its own.

## 6. Contract 3 — The bus is a client

The draft's `InMemoryClient` becomes multi-subscriber: `consume()` on an open range
registers a bounded `asyncio.Queue` per stream, `publish()` puts into every queue,
and the drop policy is per queue (drop-oldest for `Sample`, never-drop for results
and status — the two overflow policies the port document §4.4 found live and history
need). `produce()` from a module thread arrives through `ModuleIO` on the loop, so
`publish` itself needs no lock. That is the PoC `Bus` (`bus.py`) re-expressed as a
client, and `bus.py` is deleted.

Consequences:

- A module's output is `gateway.produce(result)`; the bus, an archive and a broker
  each receive it if registered with `PRODUCE` and the model. Nobody subscribes to a
  module; they `consume(IslandingResult)`.
- Stores (§7.5) are consumers of result models on the bus, so they work identically
  whether the result came from a thread in this process or from Kafka.
- In-process there is no serialisation; across Kafka it is `model_dump_json()`.
  Same object either side (principle 2).
- NQKafka (the `multiprocessing.Manager` broker) has no successor; a second process
  uses `KafkaClient`.

## 7. Contract 4 — Modules (`pswamp.modules`)

### 7.1 The abstract module

```python
class AnalysisModule(ABC):
    name: ClassVar[str]                              # registry key and ModuleRef.name
    input_model: ClassVar[type[DataModel]] = Sample  # what it consumes
    output_model: ClassVar[type[Result]]             # what run_analysis returns; joins the topic catalogue
    channel_selection: ClassVar[dict | None] = None  # station/measurement filter, as today
    window_length: ClassVar[float | None] = None     # seconds; None = snapshot
    eval_hz: ClassVar[float]                         # evaluation rate

    status: ModuleStatus
    @abstractmethod def run_analysis(self, t: np.ndarray, window: np.ndarray) -> Result | None
    def set_status(self, result: Result) -> ModuleStatus     # default: leave as is; islanding overrides
    def reset(self) -> None                                  # clear window/state before re-prime (§5.4)
    def stop(self) -> None
```

`SnapshotApp` and `TimeWindowApp` keep their names and become the two concrete
templates implementing the loop (`update` / `update_storage` / `get_result`) on top
of `ModuleIO` instead of the io duck type; the `assert`-based rate checks, the
`t_start` seek arithmetic and `open_console` go. `IslandingApp`,
`LineOutageDetectionApp`, `N4SIDApp` change only in what `run_analysis` returns (a
`Result` subclass instead of a dict) and in declaring the four class attributes.

### 7.2 The thread-facing adapter: `ModuleIO`

The one piece that lets a blocking module read an async stream:

```python
class ModuleIO:
    """A module's whole view of the world. Built by the Pipeline, one per module."""
    header: StreamHeader
    def get_next_sample(self, timeout=None) -> Sample          # blocks on a bounded queue; StopIteration when closed
    def publish(self, msg: DataModel) -> None                  # thread → loop → gateway.produce()
    # loop side:
    async def feed(self, stream: AsyncIterator[Sample], policy: OverflowPolicy)
    async def prime(self, history: AsyncIterator[Sample])      # bounded pre-fill; never dropped
    def close(self)
```

This is `RecordedIO` (`recorded_io.py:438-580`) generalised: same queue, same
drop-oldest for live, `_push_history` replaced by `prime()` over a gateway range.
`get_next_command()` and `seek_relative_input_offset()` do not exist on it; commands
reach a module through the pipeline (§8.3) and seeking is a new stream.

For the Qt path during the transition, a `LegacyIO` shim exposing the old
nine-method surface over a `ModuleIO` costs ~40 lines and lets the desktop launcher
run a migrated module; it is optional and lives beside `app_templates/`.

### 7.3 Priming, seeking and windows, decided once

A `TimeWindowApp` with `window_length=W` started at `t` is primed with
`consume(Sample, t - W, t)` unpaced (from whichever client covers it), then fed
`paced(consume(Sample, t, None))`. A seek to `t'`: pipeline pauses the pacer,
cancels the stream, calls `module.reset()`, primes from `t' - W`, feeds from `t'`.
The window is never "wrong after a jump" because it is rebuilt from the provider;
the desktop precedent ("construct a new app") is kept in spirit without a new thread.

### 7.4 Registry, discovery, scaffold, conformance

```python
MODULES: list[ModuleEntry] = [ModuleEntry("islanding", IslandingApp, "Island detection …"), …]
```

The analysis-side twin of `APPS`: discovered the same way (a package exposing a
module class), extended by the entry-point group `pswamp.modules`, checked at startup
by `check_modules()` (declares an `output_model` that is a `Result`; `run_analysis`
overridden; `name` unique), listed into the OpenAPI document with its topic.
`scripts/generate-new-module.sh islanding-v2 "Islanding v2"` renders a package with
a passing conformance test: run the module over `RecordingClient` for the whole
recording, assert every output validates as `output_model`, assert `status` leaves
`INIT`. The islanding test additionally pins the sanity values STEP1 lists (islands
6500/6700/6701 around 20 s).

### 7.5 Stores

`AlarmStore`, `AppStatusStore`, `IslandStore`, `LineOutageStore` move from
`pswamp_web/stores.py` to `pswamp.modules.stores` as pure state machines fed by
`consume(Alarm)`, `consume(AppStatus)`, … on the bus. This is the
`AlarmMonitor` split the port document §11 lists ("pure state machine + a feeder");
the feeder is now the gateway. Acknowledge/silence/annotate stay store operations
exposed by the page's POSTs.

## 8. Contract 5 — Pipelines, sessions, commands, jobs

### 8.1 Definitions

- **Source**: the process's `DataGateway` and its clients. One per process. Shared,
  read-only from the modules' point of view.
- **Pipeline**: `{stream spec (model, start, pacing), modules[], ModuleIO[], stores}`.
  Built by `PipelineRegistry`, the generalisation of `HubRegistry`: cap, idle
  eviction, per-key lock so five sockets build one pipeline, close code 1013 when
  full, dead-thread reporting — all kept from `hub.py`.
- **Session**: `client_id` + per-connection view state in `SessionRegistry`
  (channel selection, last sequence). Unchanged.

### 8.2 Unit of isolation (the §5.1 decision)

**A pipeline is keyed by what it streams, and a client owns a pipeline only when it
owns the clock.**

| Stream | Pipeline key | Who shares it |
|---|---|---|
| Live (`Coverage.live`, no pacing) | `("live", namespace)` | every client; one set of module threads per process |
| Replay of a recording or an archive range, paced | `("replay", client_id)` | that client only; its own cursor, pacer, module threads, stores |
| Batch job | `("job", job_id)` | nobody streams it; runs to completion and produces a `Report` |

This is the PoC's per-client pipeline kept exactly where its reasoning holds ("a
visitor wants the event from the start") and dropped where it does not (a shared
live feed). On the dev laptop nothing changes for the viewer: the monitor is a
replay, so each browser still gets the trip at ~20 s on its own clock. The cap
(`PSWAMP_MAX_PIPELINES`) now counts replay and job pipelines; the live one is free.

Memory follows: a replay pipeline is still one thread per module and their windows,
but the recording is shared (as today) and the bus queues are small.

### 8.3 Source commands

A `source` page package (replacing `pmu_test_streamer`) exposes, per client:

```
POST /api/source/pause · /resume · /step · /speed {factor} · /seek {t}   → CommandAck{request_id}
WS   /api/source/ws → ReplayStatus{mode: live|replay, t, speed, paused, coverage}
```

Each is `live_pipeline(client_id).replay.<op>()` — `ReplayControl` wraps the pacer
and the re-prime sequence of §7.3. On a live pipeline the POSTs return `409` with
`applied=false` and the client hides the controls (no dead buttons). Module commands
shrink to `stop`, which is a pipeline operation; `open_console` is retired.

### 8.4 Jobs (request/response inside commands-up, state-down)

```
POST /api/reports/<module>/run {start, end, parameters} → JobAck{job_id, request_id}
WS   /api/reports/ws → Report(request_id=…)  when the job's module produces it
```

A job pipeline runs the module's `run_analysis` once over
`consume(SampleBatch, start, end)` (or a windowed pass, per module declaration) and
`produce`s a `Report` carrying the `request_id`; the report page's store keeps the
last N per client. Raw samples never reach the browser (rig doc, K3 data); only the
derived model does. This is the ADR STEP1 §5.5 asks for, and it needs no second
socket direction.

## 9. Contract 6 — The browser edge (unchanged, and what changes behind it)

ADR-003 stands: POST up, socket down, `CommandAck` never carries state, generated
OpenAPI with `x-websocket-channels`, `schema.ts`, `error_check.sh` gate,
`AppEntry`/`router`/`WS_MESSAGE`/`lifespan`, `check_apps`, the subapp scaffold,
`serve_ticks`/`serve_updates`, `SessionRegistry`, `wait_for_disconnect`.

Behind it:

| Today | Target |
|---|---|
| `Hub._start` hard-wires three apps | `Pipeline` builds from `MODULES` and the pipeline key |
| `HubRegistry` per client | `PipelineRegistry` per key (§8.2); `connected_pipeline(ws)` / `live_pipeline(id)` keep their shape |
| `Bus` + `publish_threadsafe` | `InMemoryClient` on the gateway; `event_queue(bus, TOPIC)` becomes `event_queue(gateway, ResultModel)` |
| `stores.py` in the web layer | `pswamp.modules.stores`, fed by the bus |
| `islanding/adapt.py` etc. | deleted; the module returns the model |
| `wire.py` holds messages and views | `wire.py` holds **views** (`TimeWindowSlice`, `PhasorSnapshot`, `IslandingState`, `AppStatusTable`, `ReplayStatus`, `GridModel`) built from stores; messages live in `pswamp.data.models` and are re-exported for the contract |
| `pmu_test_streamer` | retired; `source/` is its successor with real controls |
| `recorded_io.py`, `replay.py`, `data/` | in the core |

`api_contract.py` gains `x-topics` (§4.5) and lists `MODULES` beside `APPS`. The
smoke test gains a `source` step (seek, assert the socket's `ReplayStatus.t`).
AGENTS.md's web-layer invariants are rewritten at this point, as STEP1 §6 says.

## 10. Running the core without the web layer

`pswamp-modules` (a `[project.scripts]` entry in the root manifest):

```
pswamp-modules list                                   # MODULES with topics and output models
pswamp-modules replay --module islanding --speed 4    # RecordingClient → module → results to stdout as JSON lines
pswamp-modules run --module islanding                 # gateway from env (Kafka in, Kafka out): a module as a service
pswamp-modules report --module <batch> --start … --end …
```

`run` is the A4 proof and the shape of a separate service: the same `Pipeline`
class with a `KafkaClient` in and out, one container, no FastAPI. Its compose profile
and k8s manifest are added together (contributor rule), after the load generator
exists (principle 10). `replay` is what STEP1 step 3 calls "a core-level test or CLI
with no web server involved".

## 11. Deployment topologies the same code supports

| Topology | Clients (env) | Pipelines | Processes |
|---|---|---|---|
| **Dev / CI / open-source demo** (default) | `recording`, `bus` | replay per browser | 1 (server) |
| **TSO, single container** | `tsdb`, `kafka` (live PMU in), `bus` | live shared; replay per browser from `tsdb`; jobs | 1 |
| **TSO, split modules** | as above, plus `pswamp-modules run` containers each with `kafka` in/out | server hosts stores + edge; modules elsewhere | 1 + N |
| **Multi-TSO** | two namespaces (`no`, `se`), clients per namespace | live per namespace | 1 or 1 + N |

Nothing in a module, a store or a page package differs between rows. Replicas > 1
of the *server* remains out of scope (the external store STEP1 §A7 names), and the
provider contract makes it no harder: no provider state lives in the web process.

## 12. Migration plan

Phases map onto STEP1 §8 and STEP2 §7; each is independently mergeable and leaves
the monitor working. "Done when" is the gate.

| Phase | Work | Done when |
|---|---|---|
| **0. Lift** | Copy Louis's `datagateway/`, `data_model.py`, `config.py`, three clients and four test files into `src/pswamp/data/` under the layout in §3; Python 3.11; drop `loguru` for `logging`; fix the three defects (STEP2 §1.3); widen the de-dup key; `Copyright Contributors` headers with Louis credited as author in the module docstrings. Park `datahub/`, `protocole/`, `converter/`, `cim/` in a branch or a `doc/` note for the grid-model track. | 33 tests green under `run-core-python-tests.sh`; `error_check.sh` syntax gate passes; nothing imports it yet |
| **1. Models + ADRs** | `models/` per §4; `FloatArray`; `Result` envelope; `AppStatus`, `Alarm`; `CommandAck` moved. ADR-005 (messages and topics), ADR-006 (provider contract). | Unit tests for JSON round-trip incl. NaN; `wire.py` imports the moved models; monitor unchanged |
| **2. Reference provider** | `RecordingClient` over `Recording`; `Pacer`; conformance suite run against all shipped clients; `pswamp-modules replay` printing samples; recorder tool moved. | Conformance green ×4; `replay` streams the recording at ×1 and ×10 |
| **3. Module contract** | `AnalysisModule`, `ModuleIO`, `ModuleRunner`, `templates.py`, `registry.py`, `check_modules`; `IslandingApp` migrated (returns `IslandingResult`, island gaps fixed upstream); scaffold script; module conformance test. `LineOutageDetectionApp`, `MeasurementStoreApp` follow. | `pswamp-modules replay --module islanding` prints `IslandingResult`s with 6500/6700/6701 at ~20 s; scaffolded module passes its own test |
| **4. Bus, stores, pipelines** | Fan-out `InMemoryClient`; `stores.py` to the core as bus consumers; `Pipeline`, `PipelineRegistry`, `ReplayControl` (pause/resume/step/speed/seek with re-prime); ADR-007 (unit of isolation). | Registry tests (today's `test_hub_registry.py`, re-pointed) green; seek test: prime + feed lands the window at `t'` |
| **5. Web adaptation** | `pswamp_web` per §9: `pipelines.py`, `source/` package, delete `hub.py`/`bus.py`/`recorded_io.py`/`replay.py`/`stores.py`/`adapt.py`; regenerate contract with `x-topics`; smoke test gains seek; retire `pmu_test_streamer`; rewrite AGENTS.md web invariants. **First user-visible change.** | `e2e-smoke-test.sh` green; monitor shows the same sanity values; scrub/pace controls work; 5 sockets → 1 replay pipeline |
| **6. Broker + service** | Load generator + end-to-end timestamp (port document §13) first; then `KafkaClient` behind a compose profile with k8s manifest; `pswamp-modules run`; ADR-008 (transport-neutral bus, in-process default). | Islanding as a separate container produces the same results over Kafka; numbers recorded in `doc/` |
| **7. Jobs** | `Report`, `JobAck`, job pipelines, `reports/` page package; ADR-009 (request/response via request id). | A batch report over a recording range arrives on the socket with its `request_id` |

Phases 0–4 are core-only and change nothing a user sees. Phase 5 is where the PoC's
internals are replaced. The Qt path is untouched throughout; `app_templates/` and
`streaming/` retire with it, not with this track.

## 13. What is carried over from Louis Pauchet's draft, and how

Louis's `../test_pswamp` work (25 commits, 2026-08-31 → 2026-09-07) supplies the
provider contract, the gateway, three of the four shipped clients, the message
envelope, the configuration scheme and the test style of this architecture. The
table is here so that contribution stays visible rather than being absorbed into a
rewrite. "Verbatim" means the file moves with its tests and only the package path,
licence header and logging change; his authorship is kept in each module's docstring
and in the ADRs that adopt the design (005, 006).

| Draft file | Disposition | Where it lands |
|---|---|---|
| `datagateway/data_client_model.py` | **verbatim** | `data/gateway/client.py` — contract 2 |
| `datagateway/time_range.py` | **verbatim** | `data/gateway/time_range.py` |
| `datagateway/planner.py` | **verbatim** | `data/gateway/planner.py` |
| `datagateway/stream.py` | **adapted**: de-dup key includes the model | `data/gateway/stream.py` |
| `datagateway/data_gateway.py` | **verbatim** | `data/gateway/gateway.py` |
| `datagateway/config.py` | **extended**: `<NAME>_TYPE`, entry points, `build_gateway_from_env` | `data/gateway/config.py` |
| `clients/in_memory.py` | **adapted**: per-stream queues (fan-out) and per-model overflow policy | `data/clients/in_memory.py` — contract 3 |
| `clients/csv_file.py` | **verbatim** | `data/clients/csv_file.py` |
| `clients/kafka_bus.py` | **verbatim**, optional extra | `data/clients/kafka.py` |
| `models/data_model.py` | **adapted**: `branch` → `namespace` | `data/models/base.py` — contract 1 |
| `models/measurements/*` | **replaced** by header + array samples (§4.2); per-channel `mRID` kept on `Channel` | — |
| `models/events/` | **dropped** (empty); the `Result` hierarchy takes the role | — |
| `tests/*` | **verbatim**, plus parameterised into the conformance suite | `tests/data/`, `data/conformance/` |
| `examples/data_gateway_demo2.ipynb` cells 3–31 | **adapted** into a runnable example over `InMemoryClient` + `RecordingClient` | `examples/data_gateway/` |
| `datahub/`, `protocole/cim_profile.py`, `converter/cim/*`, `cim/`, `utils/sparql2df.py` | **parked** for a grid-model track (STEP2 §6.9); the `Channel.mRID` field is the hook it will attach to | — |
| `pyproject.toml` deps `fastapi`, `uvicorn`, `proton-driver`, `loguru`, `cim-graph`; Python 3.13 | **not carried** (STEP2 §6.2, §6.3) | — |

The design ideas that shape this document and come from Louis: caller names a
model and a range, never a source; capability flags plus coverage as the provider
declaration; re-plan at every segment boundary; topic derived from the model; env
configuration namespaced by client name with self-description; hermetic tests with a
fixed past anchor.

## 14. ADRs to write

| ADR | Decision (imperative) | Phase |
|---|---|---|
| 005 | Represent every topic payload as a versioned pydantic `DataModel` with a derived topic; JSON on any wire; retire pickle | 1 |
| 006 | Adopt the capability-declared `DataClient` / `DataGateway` contract as the provider seam; history lives with providers; the repo ships recording, CSV, in-memory and Kafka clients | 1 |
| 007 | Key pipelines by stream: shared for live, per client for paced replay, per job for batch; seek = reset + re-prime from the provider | 4 |
| 008 | Keep the bus transport-neutral with the in-process client as default and brokers as optional clients; split a module out only on measured need | 6 |
| 009 | Carry request/response as a `request_id` on `CommandAck`/`JobAck` and on the produced `Report`, over the existing downstream socket | 7 |

Each uses `doc/adr/template.md`; 005 and 006 can be drafted from §4 and §5 directly.

## 15. Decisions taken here that need confirming

1. **Header + array samples instead of per-quantity models with named fields** (§4.2).
   The draft's author proposed the latter; STEP2 §8 Q1–2 asks. Recommendation stands
   because every module consumes a matrix.
2. **Complex vs. magnitude/angle**: magnitude/angle floats, matching the analysis code.
3. **Frequency semantics**: absolute Hz on `Sample`; deviation decoding is the
   provider's job.
4. **Rename `branch` → `namespace`** (§4.5).
5. **Live pipelines are shared** (§8.2). If a deployment ever wants per-operator
   live analysis, that is a per-client key again, not a new mechanism.
6. **Pacing as a core wrapper, seek as a new stream** (§5.4, §7.3).
7. **`KafkaClient.coverage()` stays retention-assumed** in v1 (STEP2 §6.6).
8. **NQKafka gets no successor**; the Qt examples keep using `streaming/` until
   the Qt decision.
9. **`app_templates/` becomes a shim** over `modules.templates` rather than being
   deleted, for the Qt path.

## 16. Risks and how the plan contains them

- **The `ModuleIO` bridge is the load-bearing piece.** If it is wrong, every module
  is. It is built in phase 3 against the reference provider and one real module,
  with the pre-fill and drop behaviours the PoC already proved, before anything
  web-facing depends on it.
- **The measurement model change touches every module's input.** Contained by
  keeping `TimeWindowLabeled` and the three-row header exactly as they are; `Sample`
  is that row with an envelope.
- **Wire-format break for existing Kafka consumers** (port document §11). Deliberate,
  in phase 1's ADR; the Qt examples are not moved onto it by this track.
- **Async gateway performance at 50 Hz × modules.** The measured pydantic cost is
  <1 % of a core per stream (STEP2 §9); the queue hop is what the PoC already pays.
  The phase 6 load generator is where this is checked before any split.
- **Scope creep into the CIM track.** Parked explicitly; the only hook is
  `Channel.mRID`.
- **Doing the Python-consolidation move twice.** Avoided by keeping both new packages
  movable and not deciding §7 of the port document here.
