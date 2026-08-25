# WIP: ongoing LLM-assisted review before final merge

Review started 2026-08-21 before merging the client-server work back into the
upstream p-SWAMP repository.

## Scope

The reviewed range is:

```text
b054b305ce2b3acc3324bd556da3c538ddd3a7ad..1c0227d
```

It contains three commits:

```text
3b8bcdc Overlay client server poc into existing pswamp project
df53bb6 Make state per-unique client/browser profile
1c0227d Turn upstream commands into REST POSTs + doc
```

This is a large addition: approximately 24,000 lines across 129 files. The
review concentrated on the controlling architecture rather than attempting a
line-by-line audit of generated lockfiles or vendored UI components:

- dependency direction between the existing package and web backend,
- backend lifecycle and per-client pipeline ownership,
- browser identity, REST commands and WebSocket state delivery,
- image construction, CI and Kubernetes deployment,
- consistency of the architecture documentation,
- available validation and test coverage.

## Overall assessment

The architectural direction is sound.

- The original Python + Qt implementation remains intact beside the web stack.
- The dependency runs one way: `app/server-python` depends on the root
  `p-swamp` package; root `src/pswamp/` does not depend on `app/`.
- FastAPI's `server.py` is mostly composition and transport wiring, while the
  p-SWAMP web adapter remains self-contained under `pswamp_web/`.
- Commands travel upstream as explicit REST `POST` operations and state travels
  downstream over WebSockets. This gives commands observable HTTP semantics
  without creating two competing state-delivery paths.
- All sockets in a browser profile share a persisted client id, allowing the
  five grid-monitor sockets to resolve to one per-client replay pipeline.
- The client and API are shipped in one image and served from one origin.
- Runtime base-path discovery is consistent with the remote ingress rewrite and
  avoids producing a deployment-specific client build.
- A single replica is currently the correct deployment shape because all state
  is process-local.

The main merge blocker found so far is the global pipeline-cap race described
below. The deployment policy and stale documentation should also be resolved
before the architecture is presented to upstream collaborators as authoritative.

The review continued in a Linux Codespace on 2026-08-21. That environment could
run the full quality gate, build the production client and image, execute focused
registry reproductions, and smoke-test the resulting container.

## Findings

### 1. High: concurrent clients can exceed `MAX_PIPELINES`

`HubRegistry.acquire()` serializes creation with a lock keyed by client id. It
then calls `_make_room()`, awaits `asyncio.to_thread(hub.start, ...)`, and only
after startup inserts the new entry into `_entries`.

Different client ids use different locks. Consequently, several simultaneous
first connections can all observe available capacity before any of their hubs is
registered. They can then start and insert more pipelines than `MAX_PIPELINES`.
The simplest reproduction is a registry with a cap of one and two concurrent
`acquire()` calls for different ids while startup is delayed.

This invalidates the memory bound used to size the container manifests. Fix by
serializing global admission or reserving a slot before the startup await, with
cleanup when startup fails. Add a focused test using concurrent distinct client
ids; the existing per-client race test should separately verify that several
sockets for one id still construct only one hub.

Relevant code:

- `app/server-python/src/pswamp_web/hub.py`, `HubRegistry.acquire`, around lines
  357-384.
- `app/server-python/src/pswamp_web/hub.py`, `_make_room`, around lines 427-442.

### 2. High: remote deployment uses a mutable image reference

`k8s/p-swamp-rndp.yaml` deploys `ghcr.io/thomanil/p-swamp:latest`, and
`doc/pswamp-server-infra-ops.md` instructs operators to run `rollout restart`
after CI moves that tag.

This conflicts with the immutable `sha-<full sha>` release model documented in
`AGENTS.md` and already emitted by CI. A mutable tag makes it harder to identify,
reproduce and roll back the version running in the cluster, and introduces
registry/proxy-cache ambiguity.

Choose one policy and make the manifest, operations guide and `AGENTS.md` agree.
The current governing guidance favors pinning each remote rollout to its CI SHA
tag.

### 3. Medium: failed or interrupted pipeline startup is not contained

`HubRegistry.acquire()` awaits `hub.start()` before registering the hub, but it
does not stop the hub or discard its per-client lock if startup raises. This is
not merely stale bookkeeping: `Hub.start()` launches three application threads
one by one, then starts the player, and sets `_started = True` only at the end.
An exception after any thread has launched leaves an unregistered, partially
started hub. Calling the current `Hub.stop()` would not help in that state because
it immediately returns while `_started` is false.

A focused probe replacing `Hub` with a fake whose `start()` raises produced
`{"live": 0, "stop_called": false, "lock_retained": true}`. The registry
therefore reports no live pipeline while retaining failed-client bookkeeping and
performing no cleanup.

Cancellation and shutdown expose the same ownership gap more severely because
`asyncio.to_thread()` does not stop its worker when the awaiting task is
cancelled:

- Cancelling `acquire()` while a delayed fake `start()` was running left
  `live == 0`, did not call `stop()`, and retained the client lock. A real
  `Hub.start()` can therefore finish launching threads after the socket task that
  owned it has gone away, with no registry entry through which to reclaim them.
- Calling `stop_all()` during the same delayed startup returned successfully;
  after the worker was released, `acquire()` registered a live, unstopped hub.
  The focused probe ended with `{"live_after_shutdown": 1,
  "hub_stopped": false}`.

Make `Hub.start()` exception-safe for partial construction, and make the registry
own pending as well as registered hubs. Startup cancellation must wait for or
otherwise reclaim the worker result, and `stop_all()` must prevent late
registration. Add tests for exceptions, cancellation and shutdown during
startup, verifying no threads, entries or locks remain and that a later acquire
can retry cleanly.

Relevant code:

- `app/server-python/src/pswamp_web/hub.py`, `Hub.start`, around lines 97-167.
- `app/server-python/src/pswamp_web/hub.py`, `Hub.stop`, around lines 169-199.
- `app/server-python/src/pswamp_web/hub.py`, `HubRegistry.acquire`, around lines
  357-391.
- `app/server-python/src/pswamp_web/hub.py`, `HubRegistry.stop_all`, around lines
  402-424.

### 4. Medium: the time-window counter is not atomic with its data

`CountingTimeWindowLabeled.append()` calls the base `append()`, which writes the
row while holding the window lock and then releases it, before incrementing
`n_appended` outside that lock. `snapshot()` reads the counter and window under
the lock and claims they describe the same instant, but a waiting snapshot can
acquire the lock between those two operations.

This normally delays a delta by one tick without losing it. During a full send
on connect, resync or channel selection, however, the full window can already
contain the new row while carrying the previous counter value. The next append
then sends that same row again. A deterministic interleaving produced a full
snapshot containing timestamp `1.0` with count `0`, followed by an append with
count `1` that also contained timestamp `1.0`.

Increment the counter under the same lock as the row write. Since the base class
owns that lock internally, this likely needs a small upstream-safe append helper
or an override that performs the three writes together rather than wrapping the
locking base method. Add a forced-interleaving test that verifies a full frame
followed by an append contains each timestamp exactly once.

Relevant code:

- `app/server-python/src/pswamp_web/replay.py`,
  `CountingTimeWindowLabeled.append` and `snapshot`, around lines 73-93.
- `src/pswamp/utils/time_window.py`, `TimeWindow.append`, around lines 72-83.
- `app/server-python/src/pswamp_web/time_window/api.py`, `build_message`, around
  lines 72-125.

### 5. Medium: rapid channel toggles overwrite each other

`ChannelPicker.toggle()` derives every request from the server-confirmed
`selected` prop. It does not apply an optimistic local selection or disable the
picker while the command and resulting full socket message are in flight. Two
clicks before that message arrives therefore both derive from the same old set;
the second request replaces rather than extends the first.

This reproduced against the built production image. Starting from eight
channels, clicking stations 3244 and 3245 immediately emitted two request bodies
that each contained nine indices but differed only in the last index. The final
server-pushed chart contained nine channels and station 3245; the user's 3244
selection was lost.

Keep pending selection locally and reconcile it when the full message arrives,
or serialize selection commands while preserving queued toggles. Add a component
or browser test that performs two immediate additions and expects ten selected
channels containing both stations.

Relevant code:

- `app/client-web/src/pages/grid-monitor/time-window/ChannelPicker.tsx`,
  `selectedIdx` and `toggle`, around lines 58-80.
- `app/client-web/src/pages/grid-monitor/time-window/useTimeWindowSocket.ts`,
  `selectChannels`, around lines 136-145.

### 6. Medium: the line-outage detector drops an event at index zero

`LineOutageDetectionApp.run_analysis()` finds changed channel indices, then uses
`if not any(event_channels)` to decide whether each disconnect/connect group is
empty. A sole event at detector-relative index `0` makes `any([0])` false, so the
detector updates its internal line state but omits that transition from the
returned event list. The web store and panel can never display it.

A focused two-channel invocation with only current channel zero below threshold
returned `events: []` while changing `line_outage_state` to `[False, True]`. In
the committed recording, detector-relative index zero maps to station 3000's
`I[L3000-3020]_Magnitude` channel. The detector file predates the reviewed
commits, but the reviewed web stack newly imports and presents it, so this
pre-existing core defect is now user-visible in the added line-outage panel.

Check the array length rather than the truthiness of its integer values, and add
a focused core test covering a sole transition at index zero for both disconnect
and reconnect. Remove or route the adjacent raw `print()` through logging; the
production container currently dumps large NumPy station/channel lists on every
transition and recording loop.

Relevant code:

- `src/pswamp/monitoring/line_outage_detection.py`, `run_analysis`, around lines
  43-63.
- `app/server-python/src/pswamp_web/hub.py`, construction of
  `LineOutageDetectionApp`, around lines 132-145.
- `app/server-python/src/pswamp_web/stores.py`, `LineOutageStore.handle`, around
  lines 91-113.

### 7. Medium: the remote deployment omits health probes

The remote manifest explicitly leaves probes as a TODO even though `/healthz`
now exists and the local manifest uses it for both readiness and liveness.
Without readiness, a new pod may receive traffic before the server is accepting
requests. Without liveness, Kubernetes cannot recover an alive but unresponsive
server process.

Mirror the local probe definitions unless the remote platform imposes a
documented reason not to.

### 8. Medium: documentation still describes the superseded shared pipeline

**FIXED** (cleanup pass, see `doc/WIP-api-cleanup-context.md` step 8). All four
present-tense statements now match `HubRegistry`:

- `app/server-python/src/server.py`'s SERVICES note;
- `AppStatusPanel.tsx`, which claimed every browser sees the same rows;
- `PhasorsPanel.tsx`, which called the measurement window shared;
- both spots in `doc/WIP-context-port-from-qt-to-web-frontend.md`.

Worth recording why it mattered: pipeline ownership determines memory sizing,
replay semantics and whether multiple replicas are valid, so a stale sentence
here is not cosmetic.

### 9. Medium: the new stack has no behavioral tests

No tests were found under `app/`. `scripts/error_check.sh` checks lockfile
consistency, Python compilation/format/lint, TypeScript and ESLint, but does not
run behavioral tests.

Initial high-value coverage should include:

- concurrent `HubRegistry` admission at and below capacity,
- one pipeline for concurrent sockets sharing a client id,
- idle eviction, reconnect cancellation, startup failure/cancellation and
  shutdown during startup,
- isolation between two client ids,
- REST command acknowledgement followed by WebSocket state delivery,
- initial and incremental p-SWAMP wire serialization, including non-finite
  measurement values,
- atomic full/append boundaries in the time-window delta protocol,
- rapid channel selection without lost toggles,
- line-outage transitions at detector-relative index zero,
- root and prefixed client routing/deep links.

The scaffold demos use `WebSocket.send_json()` with integer/string-only payloads.
That is not presently the same correctness failure as passing p-SWAMP measurement
models containing `NaN` through the standard JSON encoder. The strict
`pswamp_web.wire.send_state()` requirement remains appropriate for p-SWAMP data;
the demos' untyped dictionaries are a consistency and testability concern rather
than a demonstrated merge blocker.

### 10. Low: idle eviction retains one lock per expired client

The idle evictor acquires the client's lock and calls `_evict()`. `_evict()` only
removes a lock when it is not locked, so the idle path necessarily leaves it in
`_locks` after the entry and pipeline are gone. Nothing removes it when the
context exits. A focused acquire/release with `idle_seconds=0` ended with
`live == 0` and the client id still present in `_locks`.

This is much smaller than a leaked pipeline, but it grows with every distinct
browser id the process has ever idled out. Remove an unused lock after leaving
the critical section while preserving the existing protection against queued
same-client waiters. Add a churn test that evicts many unique ids and verifies
the lock table returns to its baseline size.

Relevant code:

- `app/server-python/src/pswamp_web/hub.py`, `_evict_when_idle` and `_evict`,
  around lines 444-481.

### 11. Low: the review range and current HEAD fail `git diff --check`

For the exact `b054b305..1c0227d` range, trailing whitespace was reported in:

- `doc/client-server-rig.md`, lines 39, 52, 167, 476 and 482,
- `doc/possible-performance-issues-to-followup.md`, line 5.

`doc/client-server-rig.md` also has an extra blank line at EOF, reported at line
484 in that range. At current HEAD, the later auth documentation adds trailing
whitespace at lines 349, 350, 353, 356 and 357; the original later locations have
shifted to lines 495 and 501, with the blank EOF at line 503. Clean all of these
before the final merge.

## Validation performed

- Confirmed the worktree was clean before and after the read-only review.
- Inspected the complete changed-path list and directory distribution for
  `b054b305..HEAD`.
- Traced the server composition, client id, shared socket hook, REST command
  helper, `HubRegistry`, Dockerfile, Compose setup, CI workflow, both Kubernetes
  manifests, dependency manifests and key architecture/operations documents.
- Ran `git diff --check b054b305..HEAD`; it failed only on the documentation
  whitespace listed above.
- Queried VS Code diagnostics. The only reported client error was the missing
  local `vite/client` type definition because `node_modules` is not installed in
  this environment.

The initial Windows review could not run the complete gate because `node`, `npx`
and `uv` were unavailable. The Linux Codespace continuation removed that
limitation and performed the following additional validation:

- Ran `./scripts/error_check.sh`; all lockfile, Python compile, Ruff lint/format,
  TypeScript and ESLint checks passed.
- Ran `npm --prefix app/client-web run build`; the production Vite build passed.
- Ran `docker build -t p-swamp:review .`; the complete production image built.
- Started that image and verified `/healthz`, the `/timeline` SPA deep-link
  fallback with `Cache-Control: no-cache`, and the generated OpenAPI document.
- Reproduced the capacity race with two delayed, concurrent clients against a
  cap of one; both acquired and the registry reported `2/1 live`.
- Reproduced missing startup-failure cleanup with a raising fake hub; no entry was
  registered, but `stop()` was not called and the client lock remained.
- Cancelled an acquire during delayed startup; the worker completed without
  registration or cleanup. Called `stop_all()` during delayed startup; a live,
  unstopped hub registered after shutdown returned.
- Forced a snapshot between the time-window row write and counter increment; the
  same timestamp appeared in the full snapshot and the following append.
- Automated two immediate channel additions against the built image; both POSTs
  derived from the old eight-channel set and the final chart retained only the
  second addition.
- Invoked the line-outage detector with a sole transition at relative index zero;
  it changed internal state but returned an empty event list. Confirmed that index
  maps to the recording's 3000-3020 branch current.
- Ran idle eviction with a zero delay; the pipeline was removed but its client
  lock remained.
- Re-ran `git diff --check` for both the exact `b054b305..1c0227d` range and
  current HEAD; both failed only on the documentation whitespace distinguished
  above.

These checks are necessary but do not replace the missing behavioral test suite.

## Suggested merge order

1. Fix and test global pipeline admission and complete startup/shutdown ownership.
2. Fix the time-window counter boundary, rapid channel selection behavior and
  index-zero line-outage event loss.
3. Add the focused backend lifecycle and protocol tests listed above.
4. Pin the remote deployment to an immutable image and update the operations
   guide.
5. Add remote readiness and liveness probes.
6. ~~Reconcile stale process-wide/per-client documentation.~~ Done — see §8.
7. Remove whitespace errors and run `./scripts/error_check.sh` plus an image
   build.

This document is intentionally a WIP. Keep findings here until they are either
fixed and verified or explicitly accepted with rationale before the final merge.