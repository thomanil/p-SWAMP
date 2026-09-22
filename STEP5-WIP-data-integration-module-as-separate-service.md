# STEP 5 — A module as a separate service: the streamer's stats module over Kafka

> **Status:** WIP implementation report, fifth step of the data-integration track.
> Companion to STEP 1 (requirements A1–A8), STEP 2 (the `test_pswamp` draft), STEP 3
> (the target architecture) and STEP 4 (the core and the PMU test streamer slice).
> This document describes **code that now exists on this branch**: the pieces of
> `core/` that let a module run in another process, the streamer's stats module
> running that way in the compose and minikube stacks, and the recipe step that
> tells the next module how to do the same. The durable description is
> "Running a module as a separate service" in `doc/server-data-architecture.md`;
> this document is the audit: what was decided and why, what was rejected, what
> was verified, and what is open.
>
> **Decision taken while building, confirmed with the author of the requirements
> before starting:** this lands *without* the measurements STEP 3 principle 7 asked
> for first (§1). Also confirmed: a Kafka-API broker rather than a lighter
> pub/sub, and specifically **vanilla Apache Kafka** (Apache 2.0) in the compose and
> k8s stacks; the pipeline key as the routing key, so the streamer stays per
> client; and this STEP document beside the doc section.
>
> **Numbering:** `stash@{0}` on this branch holds an uncommitted STEP 5 for the
> `GatewayIO` bridge. Whichever of the two lands second renumbers.

## 0. The one-paragraph version

A module is the analysis plus two class attributes -- what it consumes and what it
publishes -- connected to its pipeline by a bus (STEP 4). To run it as its own
service, nothing in the module changes: a `RemoteModule` takes the module's slot in
the pipeline's module list and carries the module's input class out to a Kafka
topic under the pipeline key, and its result class back; a `ModuleHost` in the
worker process runs the real module, one instance per key it sees. The streamer's
`FrameStatsModule` now runs that way in both stacks (`stats-worker` beside `kafka`
and `server` in compose; three Deployments in `k8s/`), switched by one variable that
both sides read, `PMU_TEST_STREAMER_MODULE_TRANSPORT`. The page shows its stats,
every replay command works per client, and CI still runs the module in-process
because the variable is unset there. The recipe in the architecture doc has an
optional eighth step: replace `MyModule()` with `RemoteModule(MyModule, transport,
key)`, add a two-line `worker.py`, set the variable, add the worker service.

## 1. What was asked, and the decision it forced

STEP 1 A4: a server-side module runs in the web server's process or as its own
service, as a deployment choice. STEP 3 §4.7(c) designed the out-of-process case --
a broker, a compose profile, a `k8s/` manifest -- and gated it on principle 7: no
broker before a load generator and an end-to-end timestamp say why. STEP 4 §6
recorded A4 as "proven only in-process".

The request that started this step: *make it simple to lift a module out of the
default in-process runtime and launch it as a separate compose and k8s service;
make the streamer slice run that way, publishing to a topic, with downstream state
and upstream commands still working; add the optional "run your module as a
separate service" section to the recipe.* That is A4 built on the reference slice
before any real module lands on it -- the argument being that a module must be
connectable the same way wherever it runs, and the *shape* of that has to exist
before the next module is written, or it will be written against the in-process
shape only. The numbers principle 7 wanted are now the first thing this should
produce (§6), not the thing it waited for. The override is a decision, recorded
here rather than dressed up as measured.

## 2. The design: three decisions, and what was rejected

**2.1 The hop is a transport, not a provider.** STEP 3 §4.4 sketched `GatewayBus`:
publish by `gateway.produce`, subscribe by one `gateway.consume(Model, now, None)`
per model per process. STEP 4 §8.1 #3 already doubted the seam ("the protocol is
not adapter-shaped"). Building it on the streamer settles it: the streamer's replay
frames are stamped January 2026, and their timestamps go **backwards** at every
loop. A `DataStream` plans segments by coverage and drops anything older than its
watermark; a consume "from now" would never yield a January frame, and a bounded
one would end at the first loop. Time-addressing is exactly the property a module
hop must *not* have. So the hop is a `Transport` (`core/src/pswamp_core/transport/`):
`publish(message, key)` and `subscribe(model, key)`, order of publication, no
timestamps read. What STEP 3 wanted from "a broker as a bus" -- one broker consumer
per model per process, fanned out locally -- is the `Transport` base class. A broker
*as a provider* (retention as history, tail as live) remains a `DataClient` for a
different job, still deferred (§7).

*Rejected:* a broker-backed `Bus` implementation. It would need `publish` to become
awaitable (or hide a task), invent consumer identity and reconnect semantics inside
the bus protocol, and echo-filter every class -- STEP 4 §8.1 #3's list. Keeping the
pipeline's bus in-process and putting the hop in a Module-shaped stand-in changes
no protocol and no `Pipeline` code.

**2.2 The pipeline key is the record key; one topic per class.** Topics are
`pmu.frame`, `pmu.header`, `frame.stats.result` (under an optional
`KAFKA_TOPIC_PREFIX`, configuration per ADR-005), and every record carries the
pipeline key as its Kafka key. The worker consumes each topic once and
demultiplexes by key into one module instance per key, evicting idle ones. The
streamer therefore stays **per client** -- eight clients, three topics, eight
module instances on the worker, exactly what it costs in-process -- and every
replay command keeps working, because the player never left the server. A future
pipeline keyed per stream is the same worker under one fixed key.

*Rejected:* a topic per client (`42.pmu.frame`): topic administration per browser,
pattern subscriptions that see new topics only on metadata refresh, and nothing a
real deployment would do. *Rejected:* a per-message namespace field on `DataModel`
(ADR-005 says no, and STEP 4 dropped `branch` for the same reason). *Rejected:*
making the streamer one shared pipeline to avoid routing: it would make the
per-client commands the streamer exists to demonstrate shared.

**2.3 What `setup` reads is declared and travels retained.** A module that needs
the stream's layout before its first frame declares it -- `Module.setup_models`,
new, `(PmuHeader,)` on the stats module -- and `RemoteModule.setup` publishes those
records under its key with `retained=True`. On Kafka that is a topic created with
`cleanup.policy=compact`, read from its start: the broker's own "newest per key",
so a worker that starts after the pipelines still gets every client's header. The
stats module also listens for `PmuHeader` on its bus in `setup`, so a header the
host hands over late (or a changed layout) re-primes it the same way in-process
and out. That is STEP 4 §8.1 #5 ("headers as bus events") answered at the smallest
scale that works, without touching the `Module` contract beyond one declared
attribute.

*Rejected:* the worker reading the header from its own copy of the providers
(the same image has the sample file): true for the fixture, false for any real
stream, and it would hide the gap. *Rejected:* re-publishing the header on a timer:
compaction does the job without a heartbeat.

> **Superseded 2026-09-22.** None of §2.3 exists any more. `PmuFrame` now
> carries its `PmuHeader` inside it, so nothing has to travel ahead of the
> input: `Module.setup_models`, `publish(..., retained=True)`, the compacted
> topics, the host's per-key context cache and the stats module's bus listener
> were all deleted, and a worker that starts late is primed by the first frame
> it sees. The two rejections above were rejections of ways to get the header
> to the worker *separately*; embedding it made the question moot. See STEP 4
> §8.1 #5's resolution note and `messages/pmu.py`.

**2.4 The switch.** One variable per app, `PMU_TEST_STREAMER_MODULE_TRANSPORT`,
a `name:module.path:ClassName` spec with a `{NAME}_{SETTING}` block -- the provider
scheme reused (`transport_from_env`). The server's `build_pipeline` and the
worker's `main` read the same variable, so the two sides cannot be configured
apart; unset, the module list holds the module itself and no broker is involved,
which is what the tests and CI's `docker run` see. `mem:pswamp_core.transport:InMemoryTransport`
is a legal, portless value and what every hermetic test uses.

**2.5 The broker.** Vanilla Apache Kafka, the `apache/kafka` image (Apache 2.0),
one node in KRaft mode, no ZooKeeper, no volume in compose and an `emptyDir` in
k8s (the "no persistent volume" rule: everything on the topics is a live hop the
server re-primes). `auto.create.topics.enable=false`, so a topic only ever exists
with the config the transport chose (compacted for the header). The client is
`aiokafka`, behind the `pswamp-core[kafka]` extra and imported lazily, so the core
stays pydantic-only unless a host asks; the web backend asks.

## 3. What exists

| Piece | Where | What |
|---|---|---|
| `Module.setup_models` | `core/src/pswamp_core/modules.py` | what `setup` reads from the gateway, declared |
| `Transport`, `TransportSubscription`, `InMemoryTransport`, `transport_from_env` | `core/src/pswamp_core/transport/__init__.py` | the contract; the shared per-model feed with the bus's `Overflow` policies and reconnect; the portless stand-in; the env switch |
| `KafkaTransport` | `core/src/pswamp_core/transport/kafka.py` | one topic per class, record key = pipeline key, compacted topics for retained classes, topics created on first use, lazy `aiokafka` |
| `RemoteModule` | `core/src/pswamp_core/remote.py` | the module's stand-in in a pipeline: `setup` sends the setup records ahead, `run` is an outbox and an inbox |
| `ModuleHost`, `main` | `core/src/pswamp_core/remote.py` | the worker: one bus + module + forwarder per key, pre-start input held (bounded), idle eviction, signals, exit 2 without a spec |
| `stats_module.py` | `app/server-python/src/pmu_test_streamer/` | `setup_models = (PmuHeader,)`; listens for a header on the bus |
| `api.py`: `module_transport`, `stats_modules` | same | the switch; `lifespan` closes the transport |
| `worker.py` | same | `python -m pmu_test_streamer.worker`, two lines over `remote.main` |
| `kafka`, `stats-worker` | `docker-compose.yml` | the broker and the worker beside the server; the variable on both |
| `p-swamp-kafka`, `p-swamp-stats-worker` | `k8s/p-swamp-local.yaml` | the same, as Deployments (+ a ClusterIP Service for the broker) |
| rollout order | `scripts/start-pswamp-in-local-minikube-cluster.sh` | wait for the broker, restart server and worker |
| `smoketest_pmu_test_streamer.py` | `app/server-python/tools/`, step 7 of `scripts/e2e-smoke-test.sh` | play, wait for a state carrying `stats`, stop |
| `pswamp-core[kafka]` | `core/pyproject.toml`, `app/server-python/pyproject.toml`, `uv.lock` | the extra, taken by the backend |

Not changed: the `Bus` protocol, `Pipeline`, `PipelineRegistry`, `Player`, every
message class, the socket message, the commands, the api contract (no bump), the
web client, `frequency_peek`.

## 4. What was verified, and how

- `core/tests/test_remote.py` (9 cases, `InMemoryTransport`): a result comes back
  to the pipeline's bus; two keys are two instances with no cross-talk; a setup
  record published before the host subscribed reaches the module's `setup`; one
  arriving after the first input re-primes through the bus; an idle key is evicted
  and rebuilt; backwards timestamps cross unharmed; retained subscriptions see the
  newest per key; `transport_from_env` and the worker's exit code.
- `core/tests/test_kafka_transport.py`: construction and `from_env` without a
  broker; the round trip by key and the compacted-topic read, **gated** on
  `KAFKA_TEST_BOOTSTRAP_SERVERS` against the compose broker's EXTERNAL listener.
- `app/server-python/tests/test_pmu_test_streamer.py`: the streamer pipeline with
  `RemoteModule` beside a `ModuleHost(FrameStatsModule)`, over one in-memory
  transport: stats match the played frames, results keep coming after the replay
  loops, nothing dropped; the environment picks the stand-in; a header on the bus
  re-primes the module.
- `./scripts/error_check.sh`: lockfile, `py_compile`, ruff over `app/` and `core/`,
  `tsc`, eslint, the contract check -- all green.
- The compose stack (`kafka`, `server`, `stats-worker`), driven by
  `./scripts/e2e-smoke-test.sh`: the streamer's socket carries the module's result
  after play, i.e. a frame crossed the broker to the worker and its stats crossed
  back, matched by timestamp to the frame shown.

## 5. Coverage of the requirements, updated from STEP 4 §6

| Req | Now | Still open |
|---|---|---|
| **A3** module = one class in, one class out | unchanged; `setup_models` added to the declaration | -- |
| **A4** in-process or separate service | **proven on the reference slice**: same module code, same bus message, same page; the placement is one variable in compose and `k8s/` | numbers (§6); a liveness probe for the worker (§7) |
| **A2** topics, publish/subscribe | in-process bus unchanged; between processes, one Kafka topic per class with the pipeline key as record key | a broker as a `Bus` implementation is not wanted (§2.1); a broker as a *provider* is still deferred |
| **A6** module contract | `setup_models` and "listen for your setup records on the bus" are the two things a host-independent module does | `SnapshotApp` bridging (STEP 4's deferral) |
| **A7** commands | untouched: the player stays in the server | a `Command` addressed to a remote module (§7) |
| **A8** no infra in the repo | the broker is a service in compose and `k8s/` with no state; the transport is named by config, the module by nothing | -- |

## 6. What it should measure now

The numbers principle 7 wanted, now the first job of this slice. One is already
in hand, from building it: **frame → worker → stats back on the socket, one
client at 20 Hz over the compose Kafka on a laptop: median 4.5 ms, p90 7.9 ms,
max 9.2 ms** (measured on the wire, 60 frames; the socket pushed 125 messages for
those 60 frames -- the frame, then its stats -- which is what forced the
one-frame grace in `state_message`, §7 #7). The rest:

1. the same latency with eight clients, and against the in-process path
   (in-process the result lands in the same loop turn as the frame, so the
   comparison is really "one push per frame versus two");
2. the publish cost on the server's event loop per pipeline (`send_and_wait` per
   frame; whether batching is needed at 50 Hz × 700 channels);
3. the worker's memory with eight keys live, and the broker's, against the
   server's own `MAX_PIPELINES` sizing;
4. what a broker restart costs: how long the stats are blank, and whether the
   transport's reconnect backoff is right.

## 7. Open points

1. **Worker liveness.** The worker has no probe: nothing routes to it, it exits
   when it cannot serve and the Deployment restarts it, but a stuck loop would
   sit. A heartbeat file the loop touches, or a tiny HTTP `/healthz`, are the two
   cheap answers; neither was faked.
2. **A `Command` to a remote module.** The stand-in carries input and results
   only. A module that takes commands (`Command.target` = its uuid) would need one
   more class out, and the ack semantics of STEP 4 §8.3 decision 2 apply twice.
3. **Ordering across partitions.** The transport creates one partition per topic;
   a deployment that partitions keeps order per key, which a per-key module needs,
   but the `ModuleHost` reads every partition into one asyncio task and nothing
   checks that.
4. **Eviction is idle-based on both sides independently.** The server evicts a
   client's pipeline after `IDLE_EVICT_SECONDS`; the worker evicts the key after its
   own `idle_seconds` of silence. They agree by convention (both 300 s), not by a
   message.
5. **The retained topic grows until compaction runs, and never forgets a key.**
   > *Gone 2026-09-22:* there is no retained topic; see §2.3's note. The original
   > follows. A worker that starts late reads every header ever published, for every
   client that ever connected, until the log cleaner has compacted; it only
   *remembers* them (a module is built on input, not on a header), so the cost
   is a dict entry per key, but it is unbounded on a busy deployment without a
   `min.cleanable.dirty.ratio` / `segment.ms` setting on that topic, and a
   tombstone on pipeline eviction would be the proper end of a key.
6. **`frequency_peek` still runs in-process.** By design -- it is the second
   example and the recipe now tells it how to move -- but it is also the one
   candidate for the shared-live, one-key case that would exercise the host
   under a single key.
7. **Two pushes per frame.** The socket pushes on every change; with the module
   elsewhere a frame and its result are two changes a few milliseconds apart,
   so the page receives twice the messages it did in-process. `state_message`
   keeps the previous frame's stats across that gap (one frame interval, same
   stream) so the page does not blank, but the second push is still sent. A
   settle time in the push loop -- wait a few milliseconds after a frame for its
   result before sending -- would halve the traffic at the cost of that latency
   on every frame; not done, pending the eight-client numbers.
