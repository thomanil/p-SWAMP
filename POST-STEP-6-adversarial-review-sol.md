# Fresh adversarial review after STEP 6

> **Addendum (2026-09-23):** the time-series client this review covers has
> since been renamed the Remote Data Client, since its point is decoupling
> rather than any particular store. The findings below keep the names of the
> reviewed commit. The old-to-new mapping is at the top of the STEP 6 note.

Date: 2026-09-17  
Reviewed branch: `test-and-iterate-data-flow-and-integration-patterns`  
Reviewed HEAD: `ddaba985463ab2659e1ae504844509b114041de3`  
Compared with: `origin/main` at merge base `949ee29e6054b9c8b0aa9f343457facf65cb5721`

## Scope and method

This is a fresh, read-only review of PR #10's stated requirements, STEP 1-6,
the durable architecture documentation, and the implemented path through
`pswamp_core`, the PMU streamer, the remote worker, the time-series example,
the web client, tests, Compose, and Kubernetes.

The requested sibling repository, `../test_pswamp`, is not present in this
workspace. STEP 2 identifies the evaluated source as commit `c6ce9a3` on
`feature/core-structure-utils`, but supplies no public repository URL; a
targeted GitHub search found no public copy. I therefore cannot independently
verify STEP 2's characterization of Louis's repository or compare later work
there. Findings about that contribution are limited to what this branch records
and the code attributed to it here.

Executable evidence gathered during this review:

- `./scripts/error_check.sh`: passed.
- `./scripts/run-python-server-tests.sh`: 220 passed, 30 skipped.
- Kafka-gated integration tests against the Compose broker: 6 passed.
- `./scripts/e2e-smoke-test.sh`: passed, including a frame crossing Kafka to
  `stats-worker`, its result returning to the socket, and the time-series
  REST/Kafka count and bounded-playback paths.
- A two-host probe produced two results with different module UUIDs for one
  input. A remote failing module reached `AppStatus.UNDEFINED`, while no
  `ErrorEvent` reached the pipeline-side bus.

## Executive verdict

This branch is a strong architecture spike and a credible implementation of a
data-access spine. It proves typed PMU messages, capability-aware providers,
history/live planning, replay controls, a typed in-process bus, a basic
coroutine module, one remote stream processor, and one remote time-series
provider on their happy paths.

It does **not** yet prove the proposed target architecture for p-SWAMP as a
whole. In particular, it does not integrate one existing p-SWAMP monitoring
application, does not preserve failure visibility across the process boundary,
does not support scaling the worker horizontally, and cannot move the batch
module to the worker unchanged. Several STEP 3 claims are therefore designs or
intentions, not properties of the implementation.

The right conclusion is not "reject the work." It is: **keep it explicitly as
a spike and data-access foundation; do not present A3, A4, A6, or A7 as fully
covered until the missing hard paths are exercised.**

## Findings

### 1. High: worker replicas duplicate work and results instead of scaling

`KafkaTransport` creates one partition per topic and every consumer uses
`group_id=None` with no committed offsets
([kafka.py](core/src/pswamp_core/transport/kafka.py#L161),
[kafka.py](core/src/pswamp_core/transport/kafka.py#L174),
[kafka.py](core/src/pswamp_core/transport/kafka.py#L205)). Consequently every
`ModuleHost` receives every record and constructs every key. The Kubernetes
manifest states the consequence directly: two workers would each answer every
frame, so the deployment is fixed at one replica
([p-swamp-local.yaml](k8s/p-swamp-local.yaml#L190)).

The review probe confirmed this with the transport contract: one input, two
hosts, two results from distinct module instances. This is relocation, not
independent scaling. It conflicts with A4's motivation that compute-heavy
modules can be split out and scaled independently.

Before claiming scale-out, define work allocation explicitly: partition by
pipeline key, use a consumer group with enough partitions, and test key affinity,
rebalancing, ordering, and duplicate handling. Otherwise narrow A4 to "one
separately deployed singleton worker."

### 2. High: remote module failures do not reach the global error channel

`Module.run` catches `process()` failures and publishes `ErrorEvent` on its bus
([modules.py](core/src/pswamp_core/modules.py#L85)). In a worker that bus is
private to its `ModuleHost`. The host forwards only `output_model`
([remote.py](core/src/pswamp_core/remote.py#L276)), and `RemoteModule` subscribes
only to that result class ([remote.py](core/src/pswamp_core/remote.py#L149)). The
server's `ErrorForwarderModule` can only copy errors that reach the server-side
pipeline bus ([forwarder.py](app/server-python/src/errors/forwarder.py#L24)).

The probe observed the concrete failure: the worker module became
`AppStatus.UNDEFINED`, but the server received no `ErrorEvent`. Broker publish
failures and feed failures are also log-only. The PMU page therefore loses stats
without the error tray explaining why, contrary to the error-channel description
in [server-data-architecture.md](doc/server-data-architecture.md#L782).

Carry operational events across the remote boundary as a first-class topic (or
as a supervised outcome envelope), and expose worker/transport health. Add an
end-to-end test where the remote module raises and the browser error feed receives
the correlated notice.

### 3. High: A4 works for only a narrow kind of module

The remote bridge carries one `input_model`, one `output_model`, and retained
snapshots named by `setup_models`. In the worker, `setup()` receives an
`InMemoryClient` built only from those snapshots
([remote.py](core/src/pswamp_core/remote.py#L251)). It is not connected to the
pipeline's real `DataGateway`.

That is sufficient for `FrameStatsModule`, but not for the branch's own second
module pattern. `RowCountModule` retains the real gateway and queries arbitrary
ranges on each command; it also overrides `run()` to consume `Command`
([row_count_module.py](app/server-python/src/time_series_explorer/row_count_module.py#L79),
[row_count_module.py](app/server-python/src/time_series_explorer/row_count_module.py#L94)).
Commands addressed to a remote module are explicitly listed as absent
([server-data-architecture.md](doc/server-data-architecture.md#L812)). Moving
this module out of process would therefore change or break it.

Define the supported remote-module profile honestly, for example "single-input
stream transform with finite setup context." To satisfy the broader A4, add a
command path and a remote provider/range-query capability, then run the row-count
module remotely without changing its analysis class.

### 4. High: the difficult compatibility claim remains unimplemented

Principle 6 says existing blocking applications keep their execution model and
are bridged rather than rewritten
([STEP3](STEP3-WIP-data-integration-propose-full-architecture.md#L70)). The
implemented `Module` is instead an async, one-message-at-a-time abstraction and
its own docstring says `GatewayIO` is deferred
([modules.py](core/src/pswamp_core/modules.py#L3)). The durable architecture doc
also records that the grid monitor still uses its previous `Hub`, bus, registry,
and existing application threads
([server-data-architecture.md](doc/server-data-architecture.md#L798)).

The PMU streamer proves a newly written arithmetic module, not that Hallvar's
windowed/threaded applications fit this architecture. A6 is therefore not proven
in the meaning that motivated it.

The decisive next slice is not another toy module. Implement `GatewayIO` and run
one real application, preferably islanding, over the new gateway/player/bus.
Assert the established Nordic 44 sanity values and seek/re-prime behavior. This
single test can falsify the central compatibility assumption cheaply.

### 5. Medium: command acknowledgements cannot correlate the result they trigger

`Command` generates a `request_id`, and batch results carry it
([control.py](core/src/pswamp_core/messages/control.py#L31),
[results.py](core/src/pswamp_core/messages/results.py#L49)). But the REST edge
returns only `{status, applied}`
([wire.py](app/server-python/src/pswamp_web/wire.py#L147)). The command is merely
published, then acknowledged as `applied`; the player or module acts later
([api.py](app/server-python/src/pmu_test_streamer/api.py#L353),
[api.py](app/server-python/src/time_series_explorer/api.py#L213)).

For the row-count example, the browser displays the result's request id but
never learned that id from the initiating POST. Two concurrent requests cannot
be tied reliably to their eventual result. A race can also pass the edge's mode
check, then be refused by the player after a 200 response. The branch already
lists browser-facing `request_id` as absent
([server-data-architecture.md](doc/server-data-architecture.md#L805)).

Return `request_id` in `CommandAck` and rename `applied` to `dispatched`, or add
an explicit applied/failed outcome carrying that id. Do not let a dispatch
receipt claim application.

### 6. Medium: moving a module changes its loss and recovery semantics

The remote outbox subscribes with `DROP_OLDEST`; an awaited Kafka publish that
falls behind causes local frame loss. A publish exception increments a counter,
logs occasionally, and continues without retry
([remote.py](core/src/pswamp_core/remote.py#L126)). On consumer failure,
`Transport` reconnects after backoff ([transport](core/src/pswamp_core/transport/__init__.py#L230)),
while Kafka's stream subscription starts at `latest` with no offset
([kafka.py](core/src/pswamp_core/transport/kafka.py#L174)). Records produced during
the outage are therefore not recovered.

These choices can be valid for live telemetry, but they contradict the broad
"nothing changes" placement claim and are unsafe for completeness-sensitive
analysis. There is no gap sequence, dropped-count message, retry policy, or
operator-visible degraded state.

Specify delivery semantics per module/input: latest-value, lossy ordered stream,
or complete at-least-once work. Put sequence/generation metadata where gaps must
be detectable, surface drop counters and transport state, and test broker restart
under load for both live and replay modes.

### 7. Medium: time-series range queries are intentionally unbounded in memory

Each query gets an unbounded `asyncio.Queue`
([time_series_database.py](core/src/pswamp_core/datagateway/clients/time_series_database.py#L69)).
The service may publish an entire historical range immediately while the player
drains it at replay speed. There is also one Kafka result consumer per pipeline,
and each receives the shared topic before locally dropping other query ids
([time_series_database.py](core/src/pswamp_core/datagateway/clients/time_series_database.py#L145)).

The code and STEP 6 disclose this limitation, which is good. It still means the
worked provider contract is safe for the one-minute fixture, not an operational
archive query. A sufficiently large accepted range can exhaust server memory.

Add paging or credit-based flow control, bound the queue, define cancellation
and partial-result behavior under pressure, and share one demultiplexing consumer
per process before using this with production history.

### 8. Medium: the shared PMU model does not yet enforce stream coherence

`PmuHeader` and `PmuFrame` are useful JSON-native types, and `header_id` lets a
consumer detect layout mismatch. However, the models do not validate that a
provided `header_id` matches the header content or that a frame's value/quality
length matches its header. Provider conformance tests one model at a time and
does not check header/frame coherence
([conformance.py](core/src/pswamp_core/datagateway/conformance.py#L45)). Quality
is only a placeholder and is absent from the examples
([pmu.py](core/src/pswamp_core/messages/pmu.py#L137)).

Most importantly, the live example intentionally serves frames without a header
and works only because the paired recording supplies the same layout
([live_client.py](app/server-python/src/pmu_test_streamer/live_client.py#L22)). A
deployment that configures only the live provider gets an undescribed stream.
Thus A1's syntax is demonstrated, but the real live-source semantic contract is
not.

Extend provider conformance with a PMU stream profile covering header presence,
computed header id, value count, units/frequency encoding, quality shape, and
layout change ordering. Make a standalone live provider pass it.

### 9. Medium: worker lifecycle state grows and failure supervision is weak

Retained setup context is kept in `ModuleHost._contexts` for every key ever seen
and is not removed when a slot is evicted
([remote.py](core/src/pswamp_core/remote.py#L221),
[remote.py](core/src/pswamp_core/remote.py#L298)). Kafka compaction retains the
newest header per key and no pipeline tombstone is sent. Random browser ids can
therefore grow both broker and worker state for the process lifetime.

If a generic module's `setup()` raises, its start task fails while the slot
remains. Continued input refreshes `seen`, keeps the slot from idle eviction,
and fills its bounded pending deque; no retry or process failure occurs. The
worker has no health probe, as the Kubernetes manifest acknowledges
([p-swamp-local.yaml](k8s/p-swamp-local.yaml#L226)).

Add explicit pipeline start/stop or retained tombstones, remove context on stop,
supervise `_start` failures, and expose liveness that distinguishes an idle
worker from one unable to host keys.

### 10. Medium: the design process overrode its own evidence gate

STEP 3 principle 7 says no broker or remote module before a realistic load
generator and end-to-end timing justify them
([STEP3](STEP3-WIP-data-integration-propose-full-architecture.md#L85)). STEP 5
explicitly records that this gate was overridden
([STEP5](STEP5-WIP-data-integration-module-as-separate-service.md#L15)). The one
reported latency sample is useful, but it is 60 frames, one client, one laptop;
there is no eight-client load run, resource profile, restart experiment, or
frame-versus-per-PMU comparison.

This is acceptable for a deliberate spike. It is not evidence that Kafka is the
right default remote hop or that the frame wire shape is settled. Either restore
the gate now by collecting the stated measurements, or amend the principle so
future contributors are not held to a rule this architecture did not follow.

### 11. Low: STEP 3 is a historical proposal, not the implemented architecture

STEP 3 centers `GatewayBus`, `GatewayIO`, a thread-hosted player, module registry,
generator, shared live pipelines, and request routing. The implementation learned
that a time-addressed gateway cannot serve as the module hop and introduced a
separate `Transport` instead
([transport](core/src/pswamp_core/transport/__init__.py#L21)). Several other
pieces remain absent.

The durable `server-data-architecture.md` is substantially more accurate and
candid, but readers can still mistake STEP 3's coverage table and walkthroughs
for implemented behavior. Add a prominent superseded-by marker and a compact
design-versus-built matrix, especially around the three distinct communication
contracts now present: provider pull, module transport push, and REST/Kafka
time-series request/result.

## Requirement coverage

| Requirement | Assessment | What is actually established |
|---|---|---|
| A1 shared PMU model | Partly proven | Versioned JSON models and result envelopes work end to end; live header, quality, and cross-record coherence remain open. |
| A2 topic pub/sub | Partly proven | In-process fan-out and one Kafka happy path work; delivery, restart, backpressure, and scale semantics are not a unified contract. |
| A3 plug-in modules | Partly proven | A simple one-input/one-output coroutine module is concise. The batch example already overrides `run`; multi-input, commands, windows, and existing apps need more machinery. |
| A4 in-process or service | Narrowly proven | One stateless stream transform moves to one worker. Remote commands/provider queries and horizontal worker scaling do not work yet. |
| A5 source commands/ranges | Mostly proven | Replay controls, bounded ranges, and a batch range query work end to end. Ack correlation and production-size flow control remain open. |
| A6 application contract | Not yet proven for p-SWAMP | A new async `Module` exists; the promised bridge for existing `SnapshotApp`/`TimeWindowApp` applications is absent. |
| A7 multiple clients/results | Partly proven | Per-client pipelines work on one server replica. Shared live analysis, browser-visible request correlation, authentication, and multi-replica routing remain open. |
| A8 external provider contract | Partly proven | Capability/config contracts, conformance tests, sample providers, and a stub are strong. No external TSO provider or standalone self-describing live provider has been validated here. |

## What withstands adversarial scrutiny

1. **The provider boundary is a real improvement.** Capability declarations,
   half-open ranges, incremental planning, and provider conformance replace an
   implicit nine-method duck type with something testable.
2. **The message direction is good.** Pydantic models, schema versions, UTC
   timestamps, generated browser types, and no pickle at new boundaries are the
   right foundation.
3. **History belongs behind the provider.** This cleanly preserves the repo's
   stateless runtime while allowing deployment-owned archives.
4. **The happy path is genuinely end to end.** The Compose smoke is not a mocked
   architecture diagram: it exercises HTTP, WebSockets, Kafka, a worker, and the
   time-series stub.
5. **The branch records compromises openly.** STEP 4-6 and the durable doc name
   many limitations instead of hiding them. That makes the remaining work
   tractable.
6. **Dependency direction and optionality are disciplined.** `pswamp_core` stays
   small by default, optional transports are lazy, and the web/default in-process
   paths remain runnable without external infrastructure.

## Recommended gates before calling this the target architecture

1. Bridge one real existing monitoring application through `GatewayIO`, including
   seek/re-prime, and assert domain sanity values.
2. Make remote failures and transport degradation reach the browser error feed;
   add worker liveness and supervised startup.
3. Decide whether remote hosting is singleton relocation or horizontal scaling.
   If scaling is required, implement and test partition/group/rebalance semantics.
4. Move a command-driven, range-querying module to the worker unchanged. This is
   the acceptance test for the broad A4 claim.
5. Return `request_id` to the browser and define dispatched/applied/failed command
   outcomes.
6. Add production-shaped load and restart tests: eight clients, realistic frame
   size/rate, broker outage, worker restart, tail latency, dropped frames, memory,
   and CPU versus in-process.
7. Add paging/backpressure to the time-series contract and a PMU-specific provider
   conformance profile with a standalone live implementation.
8. Reconcile STEP 3 and ADR-level claims with the architecture actually built,
   then ask Louis and Hallvar to review the decisions that reshape their work:
   frame versus per-PMU messages, async `Module` versus existing applications,
   and provider/transport boundaries.

## Bottom line

The branch has earned confidence as a **working exploration and reusable
data-access foundation**. It has not earned confidence as the final integration
architecture. The most dangerous next move would be to infer generality from the
polish of the demo. The most valuable next move is one adversarial vertical slice:
an existing windowed p-SWAMP application, remotely hosted, commandable, visibly
failing, and exercised under broker restart and realistic load.