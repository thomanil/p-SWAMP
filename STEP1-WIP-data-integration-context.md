# STEP 1 — Data integration: target requirements, where we are, what is missing

> **Status:** WIP context document, first step of the data-integration track. Written
> against branch `test-and-iterate-data-flow-and-integration-patterns`, which is at the
> tip of `main` (`949ee29`, clean tree) — so everything here describes `main`. It is an
> assessment only: no code was changed. It is a companion to
> `doc/WIP-context-port-from-qt-to-web-frontend.md` (the Qt → web port) and cites it
> rather than repeating it. Where a document or ADR has already decided something, this
> report quotes it and does not re-decide it; where the docs say a question is open, it
> stays open here.
>
> **Scope decisions taken while writing** (confirmed with the author of the
> requirements): the report recommends a *transport-neutral* topic abstraction with an
> in-process default and brokers as optional adapters; chunk queries and batch reports
> are consumed by *server-side modules only*; and the report stops at assessment, gaps
> and an ordered list of next steps — no target architecture is designed here.
>
> **Order of work, and how to read the web-path findings.** The web frontend path is the
> newer addition, and its `/` grid-monitor chain over the existing Python modules is a
> **proof of concept**. The architecture for moving data between the core, pre-existing
> modules is landed *first*, in the core; the web layer is then *adapted* to consume it
> under the stated constraints. So wherever this report describes the web path's
> internals — `Hub`, `Bus`, `HubRegistry`, per-client pipelines, the stores, the
> per-page adapters, where `recorded_io` lives — read them as **evidence of what the PoC
> needed, not as fixed points the architecture must fit around.** The two parts of the
> web path that *are* to be kept are the browser-edge contract (ADR-003: commands up as
> POST, state down on a socket, generated OpenAPI) and the hard-won operational knowledge
> in the port document's §4 (NaN on the wire, deltas not windows, the pre-fill burst,
> the thread→loop seam). §2.8 below sorts the web path into keep / replace / learn-from.

## 0. The one-paragraph version

The repo already contains, twice over, most of the *concepts* the next phase needs:
topics with logical names, a source seam that applications read through, an application
template, a per-client session model, and a command channel. It contains almost none of
the *contracts*. The desktop package moves PMU data between processes over a broker but
the payload is pickled Python objects, the io seam is a nine-method duck type implemented
seven times and declared nowhere, the app template's abstract method is an empty concrete
method, and the only upstream commands are `stop` and `open_console`. The web proof of
concept has a real, generated, enforced contract — but only for the browser-facing edge;
behind it the pipeline is three applications hard-wired in one constructor, replaying one
committed file with no control surface at all, through an in-process bus and a per-client
registry that were built to get a dashboard on screen, not as the core's data
architecture. Nothing in either half can seek a source on demand, query a time range, or
accept a provider from outside the repo. The work is therefore mostly *naming and
enforcing seams that exist in the core*, plus two genuinely new things: a
capability-declared provider contract with history navigation, and a request/response
path for batch work that the current commands-up/state-down contract has no slot for.
Once that lands in the core, the web layer is re-pointed at it.

## 1. Target requirements, restated, with the assumptions checked

Each requirement A1–A8 as given, followed by discussion: what is unclear, what the repo's
own documents already constrain, and what should be added or pushed back on.

### A1 — A shared domain model for the core PMU data

**As stated:** one shared model of PMU measurements that every module and every transport
uses.

**Discussion.** Agreed, and it is the foundation the other seven stand on. Two things the
statement leaves implicit that must become explicit:

- **Units and time base are currently conventions, and one of them is non-conformant.**
  `PMUDecoder.data_frame_to_row` (`src/pswamp/utils/pypmu.py:78-90`) reads the raw
  C37.118 `FREQ` field and treats it as absolute Hz; the `synchrophasor` library itself
  interprets the field as a deviation (`fnom + freq/1000`). It works because every
  generator in this repo writes absolute Hz, and it would silently break against a real
  PDC using deviation encoding. Likewise phasors are stored with `convert2polar=False`
  and *labelled* magnitude/angle, which is only correct because every generator sets
  the POLAR data format (`src/pswamp/test_utils/csv_playback/data_frame_generator.py:37`).
  Time is float Unix seconds throughout; angles are radians; the C37.118 `STAT`/quality
  word is dropped entirely. A shared model has to state each of these, and the FREQ
  semantics need an explicit decision.
- **There are two layers, not one.** Measurements (sample, batch, header/channel
  identity) and *results* (what a module emits: islands, events, alarms, modes, status).
  Today the result envelope `{time_stamp, info{app_name, uuid}, parameters, result}` is a
  convention followed inconsistently (`IslandingApp` includes the outer `time_stamp`,
  `N4SIDApp` does not) and carries numpy arrays, `uuid.UUID` and `datetime`. A3 says
  module outputs differ by type — fine — but the *envelope* has to be shared and
  JSON-native, or nothing downstream of a module can be generic.

**Add to the assumptions:** the wire representation must be JSON-native (or another
language-neutral encoding). Pickle is the current default for anything that is not a
plain dict (`src/pswamp/streaming/utils.py:8-21`), which rules out any non-Python
consumer, any version skew between producer and consumer, and any inspection of a topic
with standard tooling.

### A2 — Abstractions for streaming data over topics: publishers, subscribers

**As stated:** good publish/subscribe abstractions over named topics ("subjects").

**Discussion.**

- **Vocabulary:** the repo says "topic" everywhere (692 occurrences) and "subject" only in
  the Apache licence text. Keep "topic".
- **Two buses already exist, with the same vocabulary grown independently.** The desktop
  package has broker topics (`pmudata`, `alarms`, `islanding`, `application.status`,
  `application.commands`, …) configured as a logical→physical table in `config.toml`.
  The web stack has an in-process, per-pipeline `Bus` (`pswamp_web/bus.py`) with topics
  `"status"`, `"alarms"`, `"islanding.result"`, `"line_outage.result"`. They share no
  code and no declared interface. The web one is what runs the monitor today, but it is
  a PoC-local device for getting application-thread results onto an event loop; the
  abstraction A2 asks for belongs in the core, and the web bus is then either replaced
  by it or becomes one adapter of it.
- **Pushback on transport:** the natural reading of A2 + A4 is "adopt Kafka as the bus",
  which is what the root `README.md` says ("Kafka is chosen as the framework for
  facilitating communication between different monitoring applications"). The
  client-server documents point the other way, and they are newer:
  `doc/client-server-rig.md` — *"Defer jumping down rabbithole of microservices,
  separate packages, multi repo etc to begin with. Each such decoupling makes the project
  harder for partners to iterate in quickly!"*; and
  `doc/how-the-core-contributors-work-together.md` §"Only complicate the rig when you
  have to" — *"we try to start as simple as we can, and do more elaborate rigging only
  when/where actually needed, supported by performance numbers and experiments."*
  **Recommendation (confirmed):** the abstraction is transport-neutral. The in-process
  bus is the default and what the repo runs; a broker is an adapter of the same
  interface that a deployment (or an example) plugs in. Kafka stays *an* option, not
  *the* bus.

### A3 — Modules consume on one topic and produce on another, and are simple to plug in

**As stated:** a module reads a topic, analyses, writes a different-typed result to
another topic; adding one is easy.

**Discussion.** This is exactly the shape of the desktop `SnapshotApp`/`TimeWindowApp`
(`input_topic` → `run_analysis` → `output_topic`, plus `status_topic`), so the concept is
proven. Two qualifications:

- "Output can be different types" is right, but each type must be a *declared schema*.
  The web contract is generated from pydantic models (ADR-003); a module whose result is
  an ad-hoc dict of numpy arrays cannot enter it, which is why every web page today
  carries a hand-written adapter (`pswamp_web/islanding/adapt.py:53`) and why
  `doc/WIP-context-port-from-qt-to-web-frontend.md` §6 says to *"expect one adapter per
  analytic app, and budget for it."* Declared output models turn N adapters into one.
- "Simple to plug in" is true on the desktop (a process and a config entry) and false in
  the web stack, where the application list is a literal in `Hub._start`
  (`pswamp_web/hub.py:107-173`) and each new application also needs a store, a bus
  topic constant, a page package and a wire model. The page half is scaffolded
  (`scripts/generate-new-subapp.sh`); the analysis half is not.

### A4 — A module can run in the same process as the Qt/web app, or as a separate service

**As stated:** because modules are decoupled by topic, hosting is a deployment choice.

**Discussion.** The desktop already does the separate-process version (six or more OS
processes for one demo, see §2.1). The web stack deliberately does the opposite: one
container, one event loop, application threads inside it, no second service. The
contributor doc adds a rule any new service must satisfy: it must be *"defined both in
the kubernetes deployments as well as the local docker compose file"* and *"remain
testable locally for example via synthetic data, stubs/mocks etc."*

**Push back on one thing:** A4 and A7 pull against each other *if* the web PoC's unit of
isolation is carried forward. The PoC gives *one pipeline per client* — its own replay,
its own application threads, its own alarms (`pswamp_web/hub.py`, `HubRegistry`). A
module hosted as a separate service is by nature *shared*: one process serving every
client. The core architecture has to decide the unit of isolation on its own terms — the
desktop model is "one shared stream, many consumers", the PoC model is "one stream per
viewer" — and the likely answer is that what is per-client is the *cursor and view*
(where in the recording, which channels, which alarms acknowledged), while analysis of a
given stream is shared. The PoC's per-client pipeline is an input to that decision, not a
constraint on it; the web layer adapts to whatever the core decides. See §5.

### A5 — Upstream commands to adjust what comes down: jump forward, back, query a chunk

**As stated:** subscribers can command the source, because historical data has to be
navigated and batch reports run on sections of it.

**Discussion.** Three separate things are bundled here and they have different answers:

- **The command transport is settled and should be reused.** ADR-003 and
  `doc/the-client-server-api.md`: a user action is a `POST /api/<app>/…`, it answers
  with a `CommandAck` and never with state, the result arrives on the socket. AGENTS.md:
  *"A command never answers with state … that is the whole design being undone."* Seek /
  forward / back fit this exactly, and a working precedent exists in
  `pmu_test_streamer` (four POSTs) — though it drives a text file, not the pipeline.
- **Seek only makes sense on a replayable source.** The port document already decided
  how to handle that, §10.2: *"Replay controls stop making sense — play/stop/seek are
  meaningless live. Make the source a mode (live | replay) with conditional controls
  … No dead buttons on an operational screen."* and *"Keep the replay path when live
  lands … A mode, not a phase."* So A5's navigation commands are a *capability* of some
  providers, not a property of the bus.
- **"Query a chunk" is request/response, which the contract has no slot for.** The api
  document's taxonomy is: a GET for what never changes (topology, channel catalogue), a
  socket for what is pushed, a POST for a command. A bounded query returning data is
  none of those. And the rig document constrains where the data may go: *"Apis that
  return sensitive prod data when deployed in TSO infra must filter/shape/transform the
  data to only show what the UI needs … production PMU data is K3 graded eg. should not
  be directly transmitted to a frontend in its raw form."* **Recommendation (confirmed):**
  chunk queries and batch reports are consumed by server-side modules. A batch report is
  a module that asks the provider for a range, analyses it, and publishes a *derived*
  result; the browser sees the derived result over the existing socket. That keeps the
  browser-facing contract downstream-only and the raw data inside the backend.

**Add to the assumptions:** historical data implies *storage*, and the repo has decided
not to have any: AGENTS.md — *"The client-server stack is stateless on purpose. There
is no database and no persistent volume anywhere under `app/` or `k8s/`. Don't
reintroduce one without an explicit ask."* The reconciliation is that **history lives
with the provider.** p-SWAMP asks a provider for a range; the provider (a file, a
time-series database, a broker with retention) is where the range comes from. The
committed recording is the example of that, not an exception to it.

### A6 — A standard "app template" contract so contributors can slot in modules

**As stated:** keep something like the existing template as the contract.

**Discussion.** There are two templates, and both should stay:

- The **analysis-module** contract: `SnapshotApp` / `TimeWindowApp`
  (`src/pswamp/app_templates/`). Override `run_analysis`, return a dict; the template
  pulls frames, keeps the window, publishes the result and the status. Real, used by
  every monitoring application, and **unenforced**: `run_analysis` is a concrete `pass`
  (`snapshot_app.py:137`), the result envelope is convention, status is a free string,
  and there are three different ways to start the thread.
- The **page** contract in the web stack: an `AppEntry` in `APPS` plus a package
  exporting `router`, optionally `WS_MESSAGE` and `lifespan` (`api_contract.py:72`,
  `server.py:75`). Enforced at startup by `check_apps` (`api_contract.py:195`),
  generated into the OpenAPI document, scaffolded by a script, and smoke-tested in CI.

A6 is about the first one. The second is the model for what "a contract" should look like
here: discovered by name, checked at startup, generated into a document, with a reference
example and a generator.

### A7 — Multiple frontend clients at once, each with a client id, individual commands and results

**As stated:** assume a unique client id per frontend session.

**Discussion.** This is the requirement the web PoC covers most completely, for its
current shape: one random
integer per browser profile (`app/client-web/src/lib/clientId.ts`), one pipeline per id
(`HubRegistry`, cap 8, idle eviction, close code 1013 when full), every command carries
`?client_id=`, a `SessionRegistry` fans a command out to every open view of that client.
The docs are candid about the limits: one replica, restarts reset everyone, the id
*"authenticates nothing"*.

Two things A5 and A4 add to it:

- A batch query is long-running and its result is a *message*, so "individual results
  back" needs a **request/correlation id**. `CommandAck` (`pswamp_web/wire.py:147`) has
  none; every result today is identified only by which socket it arrives on.
- Per-client *pipelines* are a PoC choice that suits exploring a recording (the rig
  document explains why) and does not suit a live stream or a shared module. The
  requirement should be read as "per-client identity, cursor, view state and job
  results", not "per-client copy of every analysis". The core architecture decides the
  unit; the web layer's registry is then reshaped to match. See §5.

### A8 — Open source repo, no deployment infra; a provider contract, an example implementation

**As stated:** p-SWAMP defines the contract and api surface for data providers, including
the data commands; deployments implement it; the repo ships an example.

**Discussion.** Already a stated goal — `doc/client-server-rig.md`: *"Consumed PMU/grid
data need to be stubbed out, so that deployments in TSO infra fetches from full dataset,
while the public repo consumes local non-sensitive test data during local
development/testing."* and (after the most recent doc commit) *"The public open source
repo does not contain any TSO config/details, nor any explicit authentication. Any
deployments of the repo in the wild must bolt on their own auth, config etc."*

The seam for it exists and is used seven times (§2.3); the contract is written down
nowhere. Two refinements to the assumption:

- **Providers differ in what they can do, so the contract must declare capabilities.** A
  live PDC stream cannot seek. A Kafka topic can seek by offset (and, with effort, by
  timestamp) but only within retention. A file or a time-series database has random
  access and can answer a range query. The NQKafka default retains 120 seconds of
  `pmudata` (`src/pswamp/streaming/base.py:38`). A single "provider" interface with
  seek and query as unconditional methods would be implemented as `pass` by half the
  providers — which is precisely what `mqtt_io.py:165` and `time_series_io.py:42` do
  today.
- **"Including taking the data commands"** should mean the provider answers *source*
  commands (seek, range) — not application commands (`stop`, and today `open_console`),
  which belong to the module contract.

### Assumptions missing from the list

- **Wire format is language-neutral and versioned.** Follows from A1/A2/A4; see above.
- **Contracts are testable.** The reference subapp is the end-to-end proof of the page
  contract; the provider contract and the module contract each need the same thing: a
  conformance suite a deployment can run against its own provider, and a scaffold that
  produces a passing module.
- **Nothing is measured yet.** The port document §13: *"Nothing in the repo currently
  measures throughput or latency."* Any decision to split a module out of process is
  supposed to be *"supported by performance numbers and experiments"* — so a load
  generator and an end-to-end timestamp are prerequisites to A4, not an afterthought.

## 2. Where we are now

### 2.1 The desktop path: processes over a broker

```
PDC / rtsim ──PMUToKafka──▶ topic "pmudata" ──▶ app process 1 (SnapshotApp) ──▶ topic "islanding" ─┐
   (C37.118 DataFrame,          (pickled           app process 2 (N4SIDApp)   ──▶ "modeestimation"  ├─▶ GUI consumers
    config frame attached)       Python object)    app process N               ──▶ "application.status", "alarms"
                                                        ▲
                                          topic "application.commands" ◀── GUI (stop / open_console)
```

- **Source → topic.** `PMUToKafka` (`src/pswamp/coordination/pmu_to_kafka.py:9`) wraps
  a `synchrophasor.pdc.Pdc` and forwards whole `DataFrame` objects to `pmudata`. The
  examples use `PMUToKafkaPublisher`, which pulls frames off a tops-rt simulator's
  in-memory queue and does the same. The config frame travels attached to every data
  frame (`KafkaIO.get_config_frame()` is `get_sample_data_frame().cfg`).
- **Transport.** `src/pswamp/streaming/base.py` is a string switch over
  `kafka` / `nqkafka` / `mqtt` with `__getattr__` delegation — not a base class
  (`base.py:55`, `:96`). Default everywhere is `nqkafka`, an in-process
  `multiprocessing.Manager` server whose topics are fixed-length Python lists.
  Encoding on Kafka/MQTT is "JSON if it's a plain dict, else pickle inside a JSON string"
  (`streaming/utils.py:8`); on NQKafka the manager pickles. No consumer groups; one
  partition asserted (`kafka_io.py:16`).
- **Topics** are a logical→physical table in `[topics]` (`test_utils/default_config.toml:8`),
  which is also the multi-TSO mechanism: `examples/nordic44_rtsim_multi_tso/` maps the
  same logical names to `no.*` and `se.*` and adds `[[other_tso]]`.
- **Applications** are one OS process each, launched by the Qt `AppLauncher` via
  `multiprocessing.Process`; the main window itself runs several consumers on daemon
  threads, each opening its own `pmudata` consumer. A full demo is six or more processes
  (NQKafka server, queue manager, simulator, PMU publisher, main window, one per app).
- **Retention** under the default transport is 6000 messages for `pmudata`
  (`base.py:38`) — two minutes at 50 Hz. That is the whole history horizon.

### 2.2 The web path: threads in one process over a recording

```
n44_line_trip_50hz.npz ──▶ RecordingPlayer (1 thread) ──▶ RecordedIO ──▶ MeasurementStoreApp ──┐
  (3501 × 700, 50 Hz,        one per client, speed=1,     one per app    IslandingApp          ├─▶ publish(topic, payload)
   line trip @ 20 s)         loop=True, no controls                      LineOutageDetectionApp┘         │
                                                                                                  Bus (per pipeline)
                                                                                                  ├─▶ stores (alarms, islands, status, outages)
                                                                                                  └─▶ page packages ──▶ one WebSocket per page ──▶ browser
                                                                          browser ──POST /api/<app>/… (client_id)──▶ store / view state (never the player)
```

- **Source.** `Recording` (frozen dataclass: 3-row header, `time`, `data`, `data_rate`,
  `events`, `source`) is loaded once and shared; `RecordingPlayer` walks it at real time
  on one thread per client and fans rows to `RecordedIO` subscribers
  (`pswamp_web/recorded_io.py`). `speed` and `loop` are constructor arguments only, set
  as literals in `Hub._start` (`hub.py:124`). There is **no** `pause`, `resume`, `seek`,
  `set_speed` or `goto` on the player.
- **Applications.** Three, hard-wired in `Hub._start` (`hub.py:107-173`), each with its
  own `RecordedIO` and a `publish(topic, payload)` callback that rewrites the generic
  `"result"` topic to a per-app one and drops results nobody subscribes to
  (`hub.py:215-227`). They are real p-SWAMP applications, unchanged, on daemon threads.
- **Bus.** `Bus.publish_threadsafe` (`bus.py:119`) is the one thread→loop crossing;
  consumers attach with `add_listener`. `Bus.subscribe` / `Subscription` (`bus.py:149`)
  are never called anywhere — dead code.
- **Per client.** `HubRegistry` builds one `Hub` per client id on first connect, holds
  it `IDLE_EVICT_SECONDS = 300` after the last socket closes, caps at
  `MAX_PIPELINES = 8` (`hub.py:70-75`) and refuses with close code 1013. A pipeline is
  four threads and ~30 MB. `connected_hub(ws)` (`hub.py:566`) is how every socket gets
  its hub; `live_hub(client_id)` (`hub.py:541`) is how every command does, and it never
  builds one.
- **Edge contract.** Every socket message is a pydantic model sent through
  `send_state` (`wire.py:161`); the OpenAPI document carries the socket half as
  `x-websocket-channels`, every entry hard-coded `"direction": "server-to-client"`
  (`api_contract.py:282`). `API_VERSION = "1.0.0"` (`api_contract.py:109`).
- **Configuration.** Exactly two environment variables are read, `HOST` and `PORT`
  (`server.py:290-291`). Nothing selects a source, a recording, a speed or a cap.

### 2.3 The io seam: the de-facto provider contract

Every application reads its input through an object satisfying nine duck-typed methods.
No `Protocol`, no ABC, no docstring names the set. It is implemented seven times:

| method | `KafkaIO` | `NQKafkaIO` | `MQTT_IO` | `TimeSeriesIO` | `RecordedIO` (web) | `OfflineTestingAdapter` | `PMUPublisherMod` |
|---|---|---|---|---|---|---|---|
| `get_sample_data_frame()` | last msg on topic | same | opens a new consumer | first row | `frame_at(cursor)` | yes | yes |
| `get_config_frame()` | `.cfg` of a frame | same | same | metadata dict | player meta | yes | yes |
| `get_next_data_frame()` | blocking `next()` | same | queue, depth 3 | iterator | queue, drop-oldest | CSV row | **steps the simulator** |
| `get_next_command()` | `application.commands` | same | same | `pass` | returns `None` | — | `pass` |
| `handle_result(result)` | → `output_topic` (if set) | same | same | list | `publish("result", …)` | list | list |
| `handle_output(topic, out)` | → topic (only if `output_topic` set) | same | same | dict | `publish(topic, …)` | — | — |
| `handle_status(msg)` | → `status_topic`, no flush | same | same | list | `publish("status", …)` | — | — |
| `seek_relative_input_offset(n)` | real, relative, ≥ 0 clamp | real, relative | **`pass`** | **`pass`** | negative only, pushes history | **`pass`** (wrong arity) | **`pass`** |
| `get_sample_pmu_data_frame()` | alias | alias | absent | absent | absent | — | — |

Files: `src/pswamp/streaming/kafka_io.py:85`, `nqkafka_io.py`, `mqtt_io.py`,
`time_series_io.py:8` (imported by nothing — dead), `app/server-python/src/pswamp_web/recorded_io.py:438`,
`src/pswamp/test_utils/csv_playback/offline_testing_adapter.py`,
`src/pswamp/test_utils/offline/rtsim_adapter.py`.

Beside it sits a second, equally implicit **decoder** duck type — `PMUDecoder`
(`utils/pypmu.py`, and a verbatim copy in `utils/pmu_time_window.py:13-88` marked
"should be removed") and the web layer's `LabeledRowDecoder` — with `generate_header`,
`get_data_rate`, `get_time_stamp`, `data_frame_to_row`, `data_dtype`. The web docstring
states the design intent that is otherwise unwritten: *"an application is portable
between a live PMU stream and a recording without changing anything but its `io` and
`input_decoder` arguments."* That sentence is the provider contract; it just is not code.

`RecordedIO` + `Recording` + `LabeledRowDecoder` is the closest thing to a reference
provider: broker-free, random-access, labelled, with a recorder tool that refuses to
write a file whose scenario does not reproduce. The port document §8.1 already says it
belongs at `pswamp/streaming/recorded_io.py`, *"beside `kafka_io.py`"*, and can *"land
first and alone."*

### 2.4 Topic catalogue as it stands

Desktop (`[topics]` in every config; payload = what is actually put on the topic):

| logical name | producer | consumer(s) | payload | encoding |
|---|---|---|---|---|
| `pmudata` | `PMUToKafka` / rtsim publisher | every application, every GUI layer | `synchrophasor` `DataFrame` | pickle |
| `application.status` | `ReportingApp` in each app | status widget, `AlarmSender` (unused) | `{uuid, app_name, status, time_stamp}` | JSON |
| `alarms` | `AlarmHandler` in each app | `AlarmMonitor` → GUI | `{uuid, time_stamp(datetime), app, app_name, type, message}` | pickle |
| `islanding` | `IslandingApp` | alarm views | envelope with numpy index arrays | pickle |
| `modeestimation` | `N4SIDApp` | mode viz | envelope with complex arrays | pickle |
| `grid.events` | `LineOutageDetectionApp` | — | envelope with event list | JSON/pickle |
| `application.commands` | GUI status widget | every app's `start()` loop | `{target_uuid, command}` | JSON |
| `model.data` | `runners.publish_model_data` | `models.reader.get_model_data` | grid tables | pickle |
| `pmu.coords`, `voltage.stability.index` | — | — | commented out / app absent | — |

Web (`pswamp_web/bus.py`, in-process, per pipeline): `"status"` → `AppStatusStore`;
`"alarms"` → `AlarmStore` + islanding page; `"islanding.result"` → `IslandStore` +
islanding page; `"line_outage.result"` → `LineOutageStore` + line-outage page; raw
`"result"` dropped. Payloads are the same Python dicts the desktop pickles, coerced to
pydantic per page.

Neither catalogue is documented as such; the desktop one is inferred from config keys
and the web one from string constants in `hub.py`.

### 2.5 Command inventory

**Desktop — what exists upstream:**

- `application.commands` topic, consumed by `SnapshotApp.start()`
  (`src/pswamp/app_templates/snapshot_app.py:199-213`): broadcast, filtered by
  `target_uuid`, verbs **`stop`** and **`open_console`** (which opens an interactive
  console with `self` in scope inside the running application — arbitrary code execution
  for anyone who can write to the topic). No acknowledgement, no reply topic.
- `t_start` at construction (`snapshot_app.py:82-89`): converts a wall-clock target into
  a message count assuming constant rate, adds five seconds, rewinds the consumer once.
  Construction-time only; bounded by retention; MQTT no-op.
- `TimeWindowApp` primes its window by rewinding one window length
  (`time_window_app.py:93`), and `FFTOnline` rewinds further by hand (`fft.py:57`) using
  Kafka-only calls — which is why the port document found that N4SID "slots straight
  in" to the web stack and FFT "does not".
- `consumers_seek_to_beginning=True` in `io_kwargs`: global mutable config used as an
  implicit parameter at some 25 call sites.
- **Historical navigation in the GUI** (`gui/alarms/views/default.py:77`,
  `interactive.py:66-106`): opening an alarm constructs a *new* application instance
  rewound to the alarm time into an unbounded growing window; the slider then indexes
  that in-memory array. It never touches the consumer. Navigating history = constructing
  a new app.
- **Playback control** (`test_utils/csv_playback/playback_gui.py`): a speed slider and
  a pause toggle on the CSV *source* process — the publisher, not a consumer.

**Web — what exists upstream:** eleven `POST`s, all under the settled commands-up
pattern, and **none touches the pipeline's source**:

| path | changes |
|---|---|
| `time-window/selection`, `time-window/resync` (`time_window/api.py:164`, `:185`) | per-view channel selection; `resync` has no caller in the client |
| `islanding/alarms/{uuid}/acknowledge` / `silence` / `annotate` (`islanding/api.py:150`) | `AlarmStore` state + a wake-up nudge |
| `pmu-test-streamer/playback/play` / `stop` / `forward` / `back` (`pmu_test_streamer/api.py:212-236`) | an index into a 300-line text file (`model.py:32`); shares nothing with `pswamp_web` |
| `reference-subapp/count/bump` / `reset` | the scaffold counter |

`ReplayStatus` (`wire.py:380`) is the only replay-state surface and it is read-only; the
client shows it as a footer string. There is no scrub bar, no range query endpoint, and
`Recording` — though random-access in memory — exposes no `(t0, t1)` method.

### 2.6 What the multi-client model gives and does not give

Gives: one id per browser profile; five sockets → one pipeline (per-client lock in
`HubRegistry.acquire`); commands fan out to every open view; 422 / 404 / 1008 / 1013 /
1011 failure semantics documented in `doc/the-client-server-api.md`. Does not give (rig
doc, verbatim headings): *"One replica only"*, *"Restarts reset everyone"*, *"`client_id`
is unauthenticated"*, *"Clearing site data is a new identity"*. The port document calls
the external live store that would lift the first *"the single largest piece of unscoped
work here."*

### 2.7 Tests that touch any of this

Server: `tests/test_hub_registry.py` pins the registry bounds with a stubbed Hub;
nothing tests `recorded_io`, the bus, the stores, the adapters or an endpoint beyond the
reference-subapp smoke test. Desktop: `tests/monitoring/test_islanding.py` is the one
test asserting on a module's *output* (alarm timing over a real N44 simulation);
`tests/monitoring/test_time_window_labeled.py` is the one solid unit test of the window
model; the streaming tests need brokers and end in `sys.exit()`; nothing tests the
codec, `t_start`, seek, retention or any message schema.

### 2.8 The web path sorted: keep, replace, learn from

Because the web path is a proof of concept that will be adapted to the core
architecture rather than the other way round, it helps to say now which of its parts
are which.

| Keep (ADR-backed browser edge) | Replace or reshape once the core lands | Learn from (knowledge, not code) |
|---|---|---|
| Commands up as `POST /api/<app>/…`, state down on a socket, `CommandAck` never carries state (ADR-003) | `Hub` — the hard-wired application list, the per-app `publish` rewrite, the stores it attaches | NaN is not JSON and is the normal case: one serialiser, `float \| None` (§4.6 of the port doc) |
| Generated OpenAPI with `x-websocket-channels`, `schema.ts`, the `error_check.sh` staleness gate | `Bus` — the PoC's in-process topic fan-out; becomes an adapter of the core bus or is deleted | Send deltas, not windows: ~240× less traffic (§4.7) |
| `AppEntry` / `router` / `WS_MESSAGE` / `lifespan` page contract, `check_apps`, the subapp scaffold | `HubRegistry` and one-pipeline-per-client — the unit of isolation is the core's decision | The pre-fill burst overflows a streaming queue; live and history need different overflow policies (§4.4) |
| `client_id` as the routing key, `SessionRegistry` for per-view state | `recorded_io` / `replay.py` placement and shape — the reference provider moves to the core and is generalised there | Exactly two thread→loop crossings, no third; application threads stay as they are (§2 of the port doc) |
| The per-page delivery patterns (`serve_ticks` / `serve_updates`) as edge mechanics | Per-page `adapt.py` — disappears once module outputs are declared models | `detect_islands` returns overlapping groups; the counting window; both are core gaps, not web features (§11) |

## 3. Coverage matrix

| | Desktop (Qt, broker) | Web (FastAPI, in-process) | Verdict |
|---|---|---|---|
| **A1** domain model | `DataFrame` on the wire (pickled); `TimeWindowLabeled` + 3-row header in analysis; units by convention | `Recording` + `(meta, t, row)` frames + `LabeledRowDecoder`; pydantic only at the browser edge | **Partial.** Analysis-side model exists twice by convention; no owned wire schema; no result envelope schema |
| **A2** pub/sub over topics | broker topics, logical→physical config; string-dispatch shim; 9-method duck type | per-pipeline `Bus`; 4 topic strings; same duck type | **Partial.** Concepts present, interface undeclared, two unrelated implementations |
| **A3** module in → module out | yes: `input_topic` → `run_analysis` → `output_topic`, one process each | 3 apps hard-wired in `Hub._start`; results to stores, not re-consumable; adapter per page | **Desktop covered, web partial** |
| **A4** in-process or separate service | separate processes over broker | in-process only, by design | **Desktop covered, web missing**; and A4 vs A7 tension unresolved |
| **A5** upstream data commands | `stop`/`open_console`; one-shot `t_start`; retention ≈ 120 s | POST pattern settled; no command reaches the player; no range query | **Mostly missing.** Transport pattern covered; seek/query capabilities absent everywhere |
| **A6** module contract | `SnapshotApp`/`TimeWindowApp`, unenforced | page contract enforced + scaffolded; analysis-module contract not formalised | **Partial** |
| **A7** multi-client with client id | n/a (single operator) | client id, per-client pipeline, sessions, caps | **Web covered** for replay; no correlation id; per-client-pipeline model does not extend to live/shared |
| **A8** provider contract + example, no infra in repo | io seam × 4 impls, config.toml selects transport | io seam × 3 impls; recorded provider = de-facto example; zero source config | **Partial.** Seam exists, undeclared; no capability model; no out-of-repo packaging; no conformance tests |

## 4. What is needed, per requirement

Each item names the existing thing to grow from. Order within a requirement is rough
dependency order.

### A1 — Domain model

1. **Measurement schema**, transport-neutral: header/channel identity (the existing
   `station` / `channel` / `measurement` table is right, keep it), sample and batch
   shapes, `data_rate`, time base (epoch float seconds, state it), units (V, A, Hz,
   radians — state them), and a place for quality. Grow it from `Recording` and
   `LabeledRowDecoder`; they are already the labelled, broker-free form.
2. **Decide FREQ semantics** (absolute Hz vs C37.118 deviation) and make the decoder
   honour the config frame's format bits instead of assuming POLAR.
3. **Result envelope schema**: `{time_stamp, app: {name, uuid}, parameters, result}` as
   a pydantic model with JSON-native fields; the port document §11 already lists
   *"Make `AlarmHandler` emit JSON-native types"* as a change worth making alone, and
   warns it is *"a wire-format change for existing Kafka consumers; do it deliberately."*
4. **Status vocabulary** as an enum (`OK / Alert / Emergency / Initializing… /
   Undefined`), already a `Literal` in `wire.py`; push it upstream.
5. Delete the duplicate `PMUDecoder` in `utils/pmu_time_window.py`.

### A2 — Topics, publishers, subscribers

1. **Declare the seam**: a `typing.Protocol` (or ABC) for the source side of the io duck
   type, split by capability — stream (`get_next_data_frame`, `get_config_frame`,
   `get_sample_data_frame`), seekable (`seek_relative_input_offset`, and a
   timestamp-based seek), queryable (range read), commandable — and a `Protocol` for the
   sink side (`handle_result`, `handle_output`, `handle_status`). Every existing
   implementation is annotated against it; the `pass` methods become "capability not
   declared".
2. **One bus interface** implemented by the in-process `Bus` (delete the dead
   `subscribe` half or make it the interface) and by broker adapters (`KafkaIO`'s
   producer/consumer pair, later). The topic catalogue becomes data: name, direction,
   payload schema — the `x-websocket-channels` idea applied one layer down.
3. **Replace the JSON-or-pickle codec** with the schemas from A1. Until then the
   desktop broker path cannot be shared with anything non-Python and cannot be inspected.

### A3 / A6 — Modules and the module contract

1. **Formalise `SnapshotApp`/`TimeWindowApp`**: one abstract `run_analysis`, a declared
   `output_model`, declared input channel selection, the status enum, one way to run
   (thread) and one way to stop. Keep the class names; the monitoring applications
   subclass them already.
2. **A module registry** replacing the literal list in `Hub._start` — the analysis-side
   twin of `APPS`, discovered the same way (`getattr` on a package), so adding a module
   is a package plus an entry.
3. **Generic result adapter** once outputs are models: the page package then only says
   which topic it renders. `islanding/adapt.py` still has to reconstruct the main system
   and station names — those are the two *core* gaps §11 lists, fix them upstream.
4. **Scaffold for an analysis module** mirroring `generate-new-subapp.sh`, and a
   **conformance test** (feed the recording, assert the envelope shape and status).

### A4 — In-process or out-of-process

1. Depends on A2's interface: the same module class runs in-process (bus adapter) or
   as its own process (broker adapter) by what `io=` it is given — the seam the port
   document says is *"a constructor argument … Don't let it become a rewrite."*
2. The message envelope needs a **stream/client identity** if a shared service is to
   serve per-client work at all — or the per-client/shared split in §5 is decided
   first, which is the recommendation.
3. Any broker in an example must appear in `docker-compose.yml` *and* `k8s/`, with a
   local stub (contributor doc rule). The repo has one service today.
4. **Numbers before splitting**: a load generator and an end-to-end timestamp
   (§13 of the port document), so the decision to host a module separately is
   *"supported by performance numbers"* rather than architecture taste.

### A5 — Upstream data commands

1. **Source control in the core provider contract**: pause / resume / seek-to-time /
   step / speed as capability-gated operations on a *provider*, implemented first by the
   reference (recorded) provider in the core and exercised by a core-level test or CLI
   with no web server involved. Only then do four or five POSTs on a `source` app entry
   expose it in the web layer, and `ReplayStatus` grows `speed` and `paused`. The
   PoC's `RecordingPlayer` is the starting material, not the home.
2. **Seek semantics for windowed applications** must be designed, not assumed: a
   `TimeWindowApp` holds a ring buffer that becomes wrong the moment the source jumps.
   The desktop precedent is "construct a new application at `t_start`"; the web
   precedent is the §4.4 landmine (a burst of history overflowing the reader queue,
   fixed by `_push_history`, `recorded_io.py:489`). Either the pipeline is rebuilt on
   seek, or applications get a `reset()` and the provider re-primes them. Decide once.
3. **Source mode**: `live | replay`, exposed to the client so controls appear only when
   the provider declares the capability (§10.2 rule: no dead buttons).
4. **Range query as a provider capability plus a server-side batch module**: the module
   asks the provider for `(t0, t1, channels)`, runs its analysis over the chunk, and
   publishes a derived result. Needs a **job** notion — POST returns a job id in the
   `CommandAck`, the result arrives on a socket (or a GET by job id) carrying the same
   id. This is the request/response slot the contract lacks; ADR-003 names
   *"if the socket protocol grows bidirectional channels"* as its revisit trigger, so it
   is an ADR.

### A7 — Multiple clients

1. **Correlation id** on commands and on the results they cause (`CommandAck` gains an
   id; result models gain an optional `request_id`).
2. **Per-client cursor vs shared analysis** (§5.1) — decide, then the registry keys on
   the right thing (a stream + cursor per client for replay; a shared pipeline for live).
3. The external store for `replicas > 1` stays out of scope here; note it is the
   known blocker and that the provider contract should not make it harder (no
   provider state held in the web process).

### A8 — Provider contract, example, no infra in repo

1. **Name and document the provider contract** (A2 item 1) in `doc/`, with the
   capability table from §2.3 as its first draft.
2. **Move `recorded_io` into the core as `pswamp.streaming.recorded_io` and
   generalise it there** as the reference provider (§8.1 of the port document already
   says it belongs beside `kafka_io.py`). "Generalise" means: fit it to the declared
   contract, drop the web-specific assumptions (one player per web client, wall-clock
   rebasing as a default), and give it the seek / range capabilities the contract
   declares. Ship its **conformance suite** — the provider-side twin of the reference
   subapp: stream N frames, header matches, seek lands where asked if declared, range
   returns the right rows if declared.
3. **Provider selection by configuration**: today the web process reads two env vars
   and the recording path is a constant. A deployment must be able to name a provider
   (module path or entry point) and pass it its own settings without touching the repo.
   The desktop `config.toml` `[streaming]` block is the precedent for the shape.
4. **Out-of-repo packaging**: a provider a TSO writes must import p-SWAMP's contract and
   nothing else, and be discoverable (entry point group or module path). This also
   answers where `synchrophasor`, `kafka-python` and friends live: with the provider
   that needs them, not in the server image (which the server manifest says
   deliberately: *"none of that belongs in a headless server image"*).
5. **Kafka adapter as a second, optional example** — the existing `KafkaIO` fitted to
   the declared contract, with the latent defects in §7 fixed, behind a compose profile.

## 5. Cross-cutting tensions (candidate ADRs)

These are the decisions the requirements force that no existing document makes. Each is
a good ADR-005+ candidate (template in `doc/adr/template.md`).

1. **Unit of isolation: per-client pipeline vs shared analysis (A4, A7, A8).** The
   desktop core shares one stream among many consumers; the web PoC copies everything
   per client. The core architecture decides this, and the web registry follows.
   Likely: per-client *identity, cursor and view* over a *shared* analysis of a stream,
   with a per-client stream allowed when the provider is a replay and the viewer wants
   their own clock. The rig document's reasoning for per-client ("a rig for exploring
   recorded data") is true of that replay case and false of live.
2. **Replay controls vs live source (A5).** Decided in spirit by §10.2 (a mode, no dead
   buttons); needs the capability model to be concrete.
3. **Stateless repo vs history and batch (A5, A8).** History lives with the provider;
   the repo never persists. Say it in an ADR so a time-series database in a deployment
   is clearly *outside* the "no database" rule, not a violation of it.
4. **Pickle on the wire (A1, A2, A4).** Replacing it is a breaking change for any
   existing desktop consumer; §11 already flags it. Bundle with the schema work.
5. **Request/response inside a commands-up, state-down contract (A5, A7).** Job id +
   result message, or a query GET with a shaped response; ADR-003 anticipates the
   question.
6. **Where the shared Python lives** (port document §7). Not to be decided here — but
   it bounds where the contract module goes. Both moves keep `pswamp_web/` movable, so
   the contract can be written under `pswamp/streaming/` (the core) now without
   pre-empting §7; AGENTS.md says *"Don't write either destination down as decided."*

## 6. Settled decisions to respect, and questions already open

**Settled** (cite, don't re-argue): client-server is the direction and the analysis
core stays Python (README, ADR-002) · monorepo, new functionality is a module here
(ADR-004) · code-first OpenAPI with `x-websocket-channels`, generated types, manual
`API_VERSION` (ADR-003) · commands up over REST, state down over one downstream socket,
`CommandAck` never carries state (ADR-003, api doc, AGENTS.md — the *browser edge*, which
stays whatever the core becomes) · the analysis applications keep their execution model;
the web layer bridges to them rather than rewriting them (port doc §2, ADR-002) · the
`io=` constructor argument is the declared source-swap point and replay survives as a
mode (port doc §2, §10.2) · no auth and no TSO config in the repo; deployments bolt on
their own (rig doc) · stateless, no database or volume without an explicit ask
(AGENTS.md) · any new service exists in both compose and k8s and is locally testable
with stubs; only complicate the rig with numbers (contributor doc).

**PoC-internal, documented as invariants today but subject to the core architecture:**
one pipeline per client id, capped and evicted; the per-pipeline `Bus` with exactly two
thread→loop crossings; `HubRegistry` as the only builder of a pipeline. AGENTS.md and the
rig doc describe these as rules for the *current* web code, which they are; they are not
decisions about how the core moves data. When the core architecture lands, those
sections of AGENTS.md are rewritten to match it.

**Already open, verbatim where the docs say so:** where shared Python lives (§7:
*"they point in opposite directions and are mutually exclusive"*); whether the package
wants npm in its build (§8.3); where `examples/` goes, whether a root `pyproject.toml`
survives, wheel packaging of data files, whether `tests/` runs in CI (§9.6); live data —
reconnect, backfill, `synchrophasor` pinning, auth, the external store (§10.2);
*"Nothing in the repo currently measures throughput or latency"* (§13); the rig doc's
performance section (*"TBD!"*); and the entire external-contributor process
(`doc/how-the-project-interacts-with-open-source-contributors.md` is a one-line TODO —
A8 will be the first concrete instance of it).

## 7. Side findings (housekeeping, not this track)

Noticed while reading; each is one line so it is not lost. None blocks the work above.

- The multi-TSO commit (`87a9319`) rewrote the root `.gitignore` and dropped the
  client-server block: the `!app/client-web/src/lib/` and `!app/client-web/public/`
  rescue negations, and the `static/` / `.venv` rules. Tracked files are unaffected, but
  a *new* file under those folders (and any new `*.db`) is now silently ignored — the
  failure AGENTS.md describes under "Conventions". Restore the block.
- Dead code: `Bus.subscribe`/`Subscription`, `HubRegistry.session`,
  `RecordingPlayer.unsubscribe`, `POST /time-window/resync` (no caller),
  `streaming/time_series_io.py` (imported by nothing), `monitoring/fft_v2.py` (imports a
  module that does not exist).
- Latent real-Kafka defects, masked by the NQKafka default: `value_serializer` passed
  twice (`test_utils/pmu_csv_to_kafka.py`, `csv_playback/kafka_streamer.py`);
  `consumers_seek_to_beginning` leaks into `KafkaConsumer` kwargs through `BaseIO`;
  `producer.send(msg=…)` is the NQKafka spelling; `handle_status` never flushes
  (`kafka_io.py:215`); `Producer` with an unknown `type` recurses in `__getattr__`.
- `open_console` on `application.commands` is remote code execution into a running
  application for anyone who can write to the topic; fine on a laptop, not on a broker
  in TSO infra. Retire it with the command redesign.
- `LineOutageDetectionApp.set_status` is `pass` (`line_outage_detection.py:80`), so it is
  always `Undefined` in every status view (§11 of the port doc lists it as a domain
  judgement for upstream).
- `app/client-web/README.md` is the untouched Vite template; `app/server-python/` has no
  README. `examples/nordic44_rtsim_multi_tso/readme.md` is byte-identical to the
  single-TSO one and does not describe the multi-TSO mechanism.
- `OfflineTestingAdapter.seek_relative_input_offset(self)` has the wrong arity and
  would raise if the template ever called it with an argument.

## 8. Proposed next steps (STEP 2 candidates, in order)

The order is **core first, web second**: steps 1–5 land the data architecture in
`src/pswamp/` and are provable without a web server; step 6 re-points the web layer at
it; 7–8 extend it.

1. **Write the provider contract and the two schemas as a document plus an ADR draft** —
   the `Protocol` methods split by capability, the measurement schema, the result
   envelope, the topic catalogue, and the unit-of-isolation decision (§5.1). Nothing
   runs yet; this is the thing everyone else's work refers to.
2. **Land the schemas and the JSON-native codec in the core**, replacing the
   JSON-or-pickle encoder, with the deliberate wire-format break the port document §11
   warns about.
3. **Move `recorded_io` into the core, generalise it to the contract, and ship its
   conformance suite.** Already sanctioned by §8.1. Seek and range live here first,
   exercised by core tests and a small CLI — the seek-vs-windows decision (A5 item 2)
   gets made on the cheapest possible case, with no browser involved.
4. **Formalise the module contract and add the module registry in the core** — one
   abstract `run_analysis`, declared output model, status enum, one way to run — with
   one existing application (islanding) moved onto it and a scaffold plus conformance
   test for the next.
5. **The core bus interface** with the in-process implementation, and the topic
   catalogue as data. At this point a module reads a provider and writes a topic
   entirely inside `src/pswamp/`, in-process, with no broker and no web server.
6. **Adapt the web layer to the core.** Replace `Hub` / `Bus` / `HubRegistry` internals
   with the core registry, bus and provider; keep the browser-edge contract and the page
   packages; retire the per-page adapters; expose source control as POSTs on a `source`
   app entry. This is the first user-visible change, and the point at which AGENTS.md's
   web-layer invariants are rewritten.
7. **A broker adapter of the same provider and bus contracts behind a compose profile**,
   as the second shipped example and the proof of A4 — after, not before, a load
   generator and an end-to-end timestamp exist to say whether anything needs to leave
   the process.
8. **The job/correlation-id path for a server-side batch module** over a range query on
   the recorded provider: the first derived-result-only batch report, and the ADR for
   request/response in the contract.

Steps 1–5 change no user-visible behaviour; 1, 5, 7 and 8 each merit their own ADR under
`doc/adr/`.
