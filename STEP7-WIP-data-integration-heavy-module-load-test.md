# STEP 7 — A real module under load: islanding as its own service

Working note behind the seventh step of the data-integration track. STEP 5 ran
a module as its own service over Kafka, but the only module was the streamer's
frame statistics over five stations at 20 Hz: far too light to show whether
the architecture has built-in bottlenecks. This step puts p-SWAMP's own
islanding detector behind the same seam, feeds it the heaviest data in the
repo, turns the rate up until something gives, and makes "the pipeline cannot
keep up" something the person watching is told, not something a log records.

## Goal

Copy the islanding implementation from the desktop package into a core module;
run it in a separate process consuming a topic; give it a page that shows its
results; and publish an error on the error topic whenever the pipeline cannot
keep up with the topic it listens to. Then measure, and write down where it
broke.

## Decisions (asked and settled 2026-09-23)

| Question | Decision |
|---|---|
| Input | The grid monitor's N44 line-trip recording (`pswamp_web/data/n44_line_trip_50hz.npz`: 44 stations, 700 channels, 50 Hz, 70 s, islands 6500/6700/6701 from 20 s) behind a new `DataClient`. Every `PmuFrame` carries all 700 values and the 700-column header: ~41 KB of JSON. |
| Load knob | The page's replay speed, 1x to 50x (50 to 2500 frames/s per client). No load script in the repo; the measurements below drove the page's own endpoints from a throwaway script. |
| "Cannot keep up" | Input dropped by a module's queue, **or** input older than a threshold when read (age = now − the broker record's CreateTime). Reported on falling behind, at most every 5 s while behind, and once on catching up. |
| Document | This note, plus a short section in `doc/server-data-architecture.md`. |

Resolved without asking: the module's result is `IslandingStreamResult`
(topic `islanding.stream.result`), since the grid monitor's `wire.py` already
owns `IslandingResult` in the contract; the send time rides as a pydantic
private attribute, not a field, so no wire model or contract changed for it;
the keep-up check lives in `Module.run`, on by default, so every module has it.

## What the code forced

- **A worker's errors never reached the client.** `ModuleHost` forwarded only
  the module's result class, so a `process` that raised in the worker was a log
  line in the worker and nothing else. The host now forwards `ErrorEvent` too,
  under the key, and `RemoteModule` republishes the ones for its key and its
  module's `source` onto the pipeline bus, where the existing
  `ErrorForwarderModule` takes them to the tray.
- **Falling behind was silent by design.** Every module subscription is
  `DROP_OLDEST`, which is right for a live stream, and its `dropped` counter was
  read by nothing but an every-50th log line. `KeepUpMonitor` reads it.
- **Age needs a send time the message does not carry.** `DataModel._sent_at`,
  set by the transport on receipt (Kafka's record timestamp; the in-memory
  transport's publish call), read through `messages.sent_at()`. It never
  serialises. It assumes the producer's and consumer's clocks agree, which holds
  on one host and on NTP-synced nodes.
- **Two apps on one broker collide.** The streamer's `stats-worker` and the new
  worker would both tail `pmu.frame`, and a browser's client id is the key on
  both pages, so each worker would run its module over the other app's frames.
  `KafkaTransport`'s existing `TOPIC_PREFIX` is the fix: the islanding app uses
  a transport named `islanding` with `ISLANDING_TOPIC_PREFIX=islanding-stream`.
  Nothing enforces this; see open point 6.
- **Three reporters, one `source`.** The module, the host's shared input feed
  and the server-side publisher all report as `source="islanding"` (the tray
  groups by it, `RemoteModule` filters on it) and so needed a separate display
  `label` to read apart.
- **The ErrorForwarderModule must not monitor itself**: its input is
  `ErrorEvent`, so a report about it would feed it. `keep_up = None`.
- **The broker filled the disk.** Found by the load test, not by design review;
  see "The disk" below. Topics are now created with bounded retention, and the
  brokers enforce it every 10 s.

## What exists

Core: `messages/data_model.py` (`_sent_at`, `sent_at`, `stamp_sent_at`);
`transport/kafka.py` and `transport/__init__.py` stamp on receipt;
`modules.py` (`KeepUp`, `KeepUpMonitor`, `Module.keep_up`, `Module.monitor`);
`remote.py` (host forwards `ErrorEvent`; `RemoteModule._errors`; the outbox
and the host's shared feed are monitored). Tests: `core/tests/test_keep_up.py`.

Web backend: `islanding_stream/` — `n44_client.py` (`N44RecordingClient`,
lazy frames, `N44_MEASUREMENTS` to trim columns), `islanding_module.py`
(`detect_islands` and `moving_average` copied verbatim; a 10 s ring buffer of
the `f` columns; one evaluation per data-second; the result carries
`frames_in`, `detect_ms`, `input_age_s`, `input_dropped`), `api.py` (per-client
pipeline, play/stop/speed, a coalescing socket that never carries frames),
`worker.py`. `errors/forwarder.py` opts out of monitoring. Tests:
`tests/test_islanding_stream.py` (conformance, the islands at 30 s, the
window resetting on a loop, the same result through a worker over the
in-memory transport, the pipeline end to end).

Web client: `pages/islanding-stream/` — the island groups, an OK/Islanding
badge, and a keep-up table (frames/s, achieved vs asked speed, detect time,
input age, drops on each side, published). Errors go to the existing tray.

Deploy: `islanding-worker` in compose, `p-swamp-islanding-worker` in
`k8s/p-swamp-local.yaml`, the server's `ISLANDING_*` block in both, the
minikube script rolling it out. Bounded topics (found by the load test, below):
`create_topic` defaults to `LIVE_TOPIC_CONFIGS` (a minute / 256 MB, 10 s and
32 MB segments, 1 s delete delay) in `transport/kafka.py`, and both brokers
set `KAFKA_LOG_RETENTION_CHECK_INTERVAL_MS=10000`.

## What was measured

Two rounds. The **first** ran the server and the worker as host processes
(`uv run` on the Mac) against the compose broker's external listener; it is
kept below because it is how the ack latency showed up, but its absolute
numbers are dominated by Docker Desktop's port forwarding. The **second** is
the real stack: `docker compose up --build`, server, `islanding-worker` and
broker as containers on one compose network, the page's own endpoints driven
by a throwaway script (N clients, each: socket, `play`, `speed`, read the pushed
state and the error tray). CPU is each container's cgroup CPU time over the
step, in % of one core. "Server dropped" is the `RemoteModule` outbox;
"worker dropped" is the module's queue plus the host's shared feed; "age" is
the worst input age the module reported. Hardware: a 12-core MacBook, Docker
Desktop's Linux VM.

### In containers (the real stack)

**Full frames (700 columns, ~41 KB), 10–30 s per step.**

| Clients x speed | Offered frames/s | Achieved speed | Published/s | Server dropped | Worker dropped | Age | Server | Worker | Broker |
|---|---|---|---|---|---|---|---|---|---|
| 1 x 1x | 50 | 1.0x | 50 | 0 | 0 | 2–4 ms | 6–8% | 4–7% | 23% |
| 1 x 5x | 250 | 5.0x | 250 | 0 | 0 | 1–2 ms | 15% | 15% | 31–37% |
| 1 x 10x | 500 | 10x | 510 | 0 | 0 | 1 ms | 21–23% | 23–25% | 30–42% |
| 1 x 20x | 1000 | 19–19.5x | 975 | 0 | 0 | 1–2 ms | 32–34% | 37–39% | 31–35% |
| 1 x 50x | 2500 | **34–45x** | 1400–2000 | 0–23 in 30 s | 0 | 1–2 ms | 42–60% | 48–66% | 46–54% |
| 4 x 5x | 1000 | 5.0x | ~1000 | 0 | 0 | 2 ms | 33% | 36% | 37% |
| 4 x 10x | 2000 | 9.9x | ~2040 | 0 | 0 | 2 ms | 60% | 63% | 45% |
| 4 x 20x | 4000 | 19.9x | ~2300 | ~1700/s | 0 | 3 ms | 86% | 82% | 58% |
| 4 x 50x, 90 s | 10000 | 49.7x | ~2080 | ~7900/s | 0 | 2 ms | **99%** | 74% | 59% |
| 8 x 1x | 400 | 1.0x | 399 | 0 | 0 | 2 ms | 19% | 20% | 29% |
| 8 x 2x | 800 | 2.0x | 810 | 0 | 0 | 3 ms | 28% | 30% | 26% |
| 8 x 5x | 2000 | 5.0x | ~2010 | 0 | 0 | 2 ms | 61% | 61% | 44% |

At everything a real deployment would do -- eight clients at real time is 400
frames/s -- the stack keeps up with a wide margin: **the whole pipeline takes
about a fifth of a core per side at the 8-pipeline cap**, and still keeps up
at 5x for all eight. A single client
keeps up all the way to 50x in the sense the user sees (no drops, results on
time); what it cannot do alone is *go* at 50x (next section).

**A stalled worker** (SIGSTOP for 5 s at 10x, first round): the tray showed
"islanding is not keeping up with pmu.frame … oldest input read 5.0 s after it
was sent" and "the islanding worker is not keeping up with pmu.frame: its
shared input feed is dropping records" (190, then 824 dropped), both repeated
at 5 s, then "caught up after 10 s behind". Under overload the tray showed
"the server-side publisher for islanding cannot publish pmu.frame as fast as
the pipeline produces it" every 5 s per client, and "caught up" once the
speed came back down.

### Profile (cProfile, in the server container, 4 clients x 50x, 20 s)

A second server and a second worker under `python -m cProfile` inside the
server container (the regular `islanding-worker` stopped so only one consumed
the topic), driven from inside the container. cProfile slows Python calls
roughly 2x, so read the shares, not the absolute rates. (It also books the
event loop's idle time to whichever coroutine resumes -- 37 s on the socket's
`push` -- so the server's idle time is hidden there; the worker's shows
honestly as `epoll.poll`.)

Server, per **emitted** frame (~170k in 20 s, ~85% of them then dropped at
the outbox):

| Where | µs per frame |
|---|---|
| `N44RecordingClient.frame` — `ndarray.tolist` 7.5, `PmuFrame` validation of 700 values 14.5, the new private attribute's init 2.5 | ~30 |
| `Player._emit` → `bus._deliver` → subscriber queues | ~18 |
| dropping it again at the outbox (`Subscription._drop_one`) | ~10 |
| the player's own loop (`_run`, `_pace`, a task + `shield` + `wait_for` per frame) | ~5 |

Server, per **published** frame (~26k):

| Where | µs per frame |
|---|---|
| `model_dump_json` of the 41 KB frame | ~73 |
| aiokafka `send_and_wait` → accumulator → sender, each frame its own request | ~50 |

Worker, per consumed frame (~25k; the worker was idle ~75% of the run):

| Where | µs per frame |
|---|---|
| `PmuFrame.model_validate_json` (41 KB, 2800 header strings) | ~110 |
| aiokafka fetch + unpack | ~100 |
| `IslandingModule.process` (append to the window) | ~50 |
| `detect_islands`, once per 50 frames | ~450 per call |

So nothing is exotic: the cost is building, serialising and parsing a 41 KB
self-describing frame, plus the Kafka client's per-request overhead. The
islanding analysis is noise.

### The disk

The second round's first multi-client runs failed with `[Error -1]
UnknownError` on every publish. The broker's `islanding-stream.pmu.frame`
partition had reached **12 GB** and Docker Desktop's VM disk (59 GB, shared
with every image and with minikube) was 100% full. The topics were created
with the broker's defaults: a week's retention and no size cap. "No volume"
made them ephemeral across restarts but not bounded while the broker ran; at
~2 MB/s per real-time client, the rig's own dev stack fills a disk in hours,
and a 50x replay in minutes.

Fixed in two places, both needed. The transport creates every topic with
`LIVE_TOPIC_CONFIGS` (retention a minute or 256 MB, 10 s / 32 MB segments,
1 s delete delay). With that alone the partition still grew to 6.6 GB: the
broker enforces retention only every `log.retention.check.interval.ms`, five
minutes by default. Compose and k8s now set it to 10 s. Re-run at the worst
case (4 x 50x for 90 s, ~80 MB/s), the partition plateaued at ~0.9 GB (the
retained 256 MB plus ~10 s of writes between checks) and fell back once the
load stopped.

### First round: host processes (for the record)

Server and worker on the Mac, broker in Docker via `127.0.0.1:19092`. One
client, full frames: clean to 10x (500/s), then the server-side publish
capped at **~780–850 frames/s** from 20x up, with both processes at only
40–50% of a core; four clients shared the same ~750/s. A producer micro-
benchmark (1500 records against the same broker) showed why:

| Record | `max_batch_size` | Serial `send_and_wait` | 4 concurrent | `send` + `flush` |
|---|---|---|---|---|
| 41 KB | 16 KB (default) | 859/s | 798/s | 788/s |
| 41 KB | 1 MB | 785/s | 1661/s | 2103/s |
| 1.7 KB | 16 KB (default) | 1260/s | 4726/s | 10059/s |
| 1.7 KB | 1 MB | 1392/s | 4194/s | 18932/s |

That is ack latency (~1.2 ms per round trip through the port forward), made
process-wide by a batch size smaller than one frame. Inside the compose
network the same code published ~2000/s for one client: the latency was
mostly the Mac's, which is exactly why the second round was needed. With
frequency-only frames (`N44_MEASUREMENTS=f`, 1.7 KB) eight host clients pushed
~7800 frames/s through a worker at ~50% of a core with nothing dropped.

## Where it breaks

1. **The server's one event loop, at ~2000–2400 full frames/s published in
   total.** With four clients at 20x and up, the server reaches 86–99% of a
   core. Every pipeline, every player, every outbox and the Kafka producer
   share that loop, and the ~120 µs per published frame (JSON 73, producer 50)
   plus ~30 µs per *emitted* frame (building it) fill it. Past that, the
   outbox's drop-oldest queue sheds the excess and the tray says so. This is
   a per-process ceiling, independent of the worker.
2. **Work is thrown away before the choke point.** At 4 x 50x the players
   emit ~10k frames/s and ~85% are built, validated, delivered, then dropped
   at the outbox. The drop is the intended live-stream policy, but it sits at
   the end of the expensive part.
3. **A single player cannot pace below a millisecond.** At 50x a frame is due
   every 0.4 ms. asyncio's epoll wait rounds its timeout up to whole
   milliseconds, so a lone pipeline's loop goes idle, oversleeps, and the
   player -- which drops time rather than bursting when more than a frame late
   -- re-anchors and loses it: 34–45x achieved, at 42–60% CPU. With four
   pipelines the loop never sleeps and every player reaches 49.7x. Nothing is
   dropped; the replay just runs slower than asked, which the page shows as
   "36.3x of 50x".
4. **The worker never fell behind under load in any run**, in either round:
   up to ~2300 full frames/s in containers (74–84% of a core) and ~7800 small
   ones on the host, input age 1–4 ms throughout. Its one consumer per model,
   demultiplexed by key, is not the limit at these rates. It drops only when
   stalled, and says so.
5. **The broker's disk** was the one failure that was not graceful: it took
   the whole Docker VM with it. Now bounded (above).
6. **Not bottlenecks**: the islanding analysis (~0.5 ms a second per client),
   the web socket (it never carries frames), the error path.

## Open points

1. **The server's per-frame costs.** The biggest are the JSON of a 41 KB frame
   (73 µs) and building it (30 µs). Options, cheapest first: skip validation
   when the provider built the frame itself (`model_construct`); serialise the
   header once per layout and splice it in; drop the header from the wire once
   per-frame layout is not needed (the trade-off the self-describing frame
   made); compression on the producer (the repeated header compresses well)
   to cut broker and network bytes, at some CPU.
2. **Producer settings and a pipelined outbox.** `max_batch_size` ≥ 1 MB and a
   small `linger_ms` in `_PRODUCER_OPTIONS`; `send` without awaiting each ack,
   with a bounded in-flight window and failures counted in the delivery
   callback. Latency-bound on the Mac, less so in the compose network; either
   way it removes one request per frame from the loop.
3. **Drop earlier.** A player that knows its only consumer is behind could
   skip building frames it will not deliver (pace by the outbox, not the
   clock), or the provider could hand out raw rows and build frames only for
   what is published.
4. **Sub-millisecond pacing.** Emit every frame already due in one pass and
   allow a small catch-up burst (a few ms) before re-anchoring, instead of one
   timer per frame. Makes a lone pipeline reach 50x and halves the tasks per
   frame.
5. **Scale out, not up.** One server process is one core. Past ~2000 full
   frames/s the answer is more processes (which needs the per-client state to
   leave the process, as `AGENTS.md` notes for replicas), not a faster loop.
6. **Topic namespace per app is configuration, not structure.** Two apps with
   the same input class on one broker must be given different transport names
   and prefixes by hand, or their workers read each other's frames. Deriving
   the prefix from the app (or the module) by default would remove the trap.
7. **Partitions.** One partition per topic keeps total order, which only a
   per-key order needs. Keyed partitioning would let more than one worker
   share a topic (a consumer group) once one worker is not enough.
8. **Retention for a broker we do not configure.** `LIVE_TOPIC_CONFIGS` is set
   on topics the transport creates; a deployment's broker still needs a short
   `log.retention.check.interval.ms`, or its own disk budget. Worth a line in
   the deployment docs when there are some.
9. **Clock skew** makes input age wrong by the skew. Fine on one host;
   across nodes it wants NTP, or a skew allowance in `KeepUp`.
10. **Catching up is judged on arrival**, so a stream that stops while behind
    reports "caught up" on its next message rather than on a timer.
11. **The bus's own "not keeping up" warning** (every 50th drop) now duplicates
    the monitor's report in the log. It can go once the monitor has proved
    itself.
12. **The private `_sent_at` attribute costs ~2.5 µs per `DataModel`
    constructed** (pydantic's private-attribute init), about 8% of building a
    frame. A plain slot or a side table keyed by id would avoid it if it
    matters.
