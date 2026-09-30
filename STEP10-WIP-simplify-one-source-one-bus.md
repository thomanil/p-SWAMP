# STEP 10 (WIP): one active source, and the transport as the only bus

A simplification of the lab branch, not a new capability. Two pieces of
machinery cost more mental model than they bought, and went.

## What was cut, and why

**The planner.** `datagateway/planner.py`, the stitching in `stream.py`,
`priority`, the live hand-off margin, the watermark and gap policy. No shipped
app ever read one stream from two providers; the player even capped each
replay at the history end to stop the planner handing over. Now a
`DataGateway` holds named sources, one active; `SwitchSourceCommand(source)` is
the only way the source changes, from the edge or from a module; the player's
mode is the active source's kind. A client is a history *or* a live feed. The
streamer's `/playback/live` + `/playback/replay` became `POST /playback/source`
(`API_VERSION` 4.0.0).

**Two pub/subs and a bridge.** `InProcessBus` was an in-memory transport scoped
to one key; `RemoteModule` bridged it to the real transport; every app with a
module had an if/else on `*_MODULE_TRANSPORT`, and three had a `worker.py`.
Now the `Transport` is the only pub/sub: one topic per class per app
(`<app>.<model>`), the pipeline key on every record. Modules always run in a
`ModuleHost`; the deployment picks the transport (`PSWAMP_TRANSPORT`): unset,
`InMemoryTransport` with the hosts in the server's lifespan; Kafka, with
`python -m pswamp_core.worker` hosting the families named in
`PSWAMP_WORKER_FAMILIES`. The player stays in the server, so player commands
keep their synchronous 409; module commands are checked where the module runs.

Plus what that made possible: a `PipelineFamily` per app (`<app>/family.py`),
one socket loop (`shared.push_changes`) and one handshake
(`shared.connected_pipeline`) instead of five copies, errors forwarded from
each app's error topic instead of a forwarder module per pipeline, a module
that reads the gateway hostable anywhere (the worker builds one from the same
env), and `PipelineClosed` so a host drops a key when its pipeline goes.

## Changes the plan did not foresee

- **One Kafka consumer per process.** With every module behind the transport
  the server listened to ~50 topics, one `AIOKafkaConsumer` each, and the broker
  answered with `NodeNotReadyError` floods. `KafkaTransport` now reads every
  topic its process listens to with one consumer, assigned by hand, keeping
  its position on topics it already held when a new one is added.
- **The in-memory transport is honest**: JSON round trip and exact-class
  topics, so CI catches what Kafka would.
- Steps 3 and 4 of the plan landed together: step 3's interim adapters (the bus
  pipeline on the new module API) would have been deleted by step 4.
- `push_changes` treats a client gone mid-send as a disconnect, not an error.
- Playwright gained `E2E_BASE_URL` (run against a server that is already up)
  and `E2E_KEEP_STACK` (don't tear down a stack it did not start).

## Measured

**The in-memory JSON round trip** of a 700-channel N44 frame (42 KB) costs
~0.16 ms (dump 0.06, parse and `header_id` 0.10): a ceiling of ~6400
frames/s on one core. The islanding stream in one container (bare
`docker run`), fresh, one client:

| requested | before (bus) | after (in-memory transport) |
|---|---|---|
| 10x | 10.0x | 10.0x |
| 50x | 38.4x | 34.7x – 38.2x |

(A first reading of 7x/12x was contention: Playwright's auto-playing pipelines
outlive their sockets for 300 s in the same container.)

**Size**, working tree against the original branch tip (`4f8ecc6`); lines
are non-blank, and in brackets Python without comments or docstrings:

| | original | simplified |
|---|---|---|
| `core/src` | 5607 (3744) | 5255 (3461) |
| server app `src` | 3786 (2524) | 3406 (2244) |
| core + app tests | 4128 (3747) | 3995 (3595) |
| docs (`doc/*.md`, AGENTS.md, core README) | 4080 | 3440 |
| e2e specs (Playwright) | 112 | 492 |
| diff against `main`, excluding the generated contract and STEP notes | +22443 / −536 | +21370 / −550 |

**Structure** (Python, `core/src` + app `src`): functions 490 → 458, classes
119 → 114, branch points 1096 → 976. **Concepts**: publish/subscribe
mechanisms 2 → 1; transport variables in compose/k8s 8 → 3; worker entry
points 3 → 1 (generic); in-process-or-remote branches in the apps 12 → 0;
copies of the socket handshake 5 → 1 and of the push loop 6 → 1; compose
services 6 → 5; gateway routing concepts (segments, priority, watermark, gap
policy, hand-off) all gone; the architecture doc 1116 → 740 lines, with the
diagrams doc (a comparison with Louis Pauchet's draft) dropped and its class
diagram of the messages folded in.

## Verified

Every page and control, by a Playwright spec per page written before the
change and green on the unchanged branch (23 specs), in compose (Kafka and the
workers), in a bare `docker run` (22, the error-tray case needing compose)
and on minikube over the port-forward (22). The wire smoke test in compose and
single process, the remote data stub check, the Kafka round trip against the
compose broker, 272 unit tests, `error_check.sh`.
