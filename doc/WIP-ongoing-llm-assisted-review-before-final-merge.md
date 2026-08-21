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

### 3. Medium: the remote deployment omits health probes

The remote manifest explicitly leaves probes as a TODO even though `/healthz`
now exists and the local manifest uses it for both readiness and liveness.
Without readiness, a new pod may receive traffic before the server is accepting
requests. Without liveness, Kubernetes cannot recover an alive but unresponsive
server process.

Mirror the local probe definitions unless the remote platform imposes a
documented reason not to.

### 4. Medium: documentation still describes the superseded shared pipeline

The implementation and primary guidance now define one p-SWAMP pipeline per
client, but stale process-wide descriptions remain:

- `app/server-python/src/server.py` says the pipeline is process-wide and shared.
- `doc/WIP-context-port-from-qt-to-web-frontend.md` describes `hub.py` as the
  process-wide pipeline near line 57.
- The same WIP document says state is process-wide near line 894.

These statements are especially risky because pipeline ownership determines
memory sizing, replay semantics and whether multiple replicas are valid. Update
historical discussion to clearly mark the old design, and make present-tense
descriptions match `HubRegistry` and `AGENTS.md`.

### 5. Medium: the new stack has no behavioral tests

No tests were found under `app/`. `scripts/error_check.sh` checks lockfile
consistency, Python compilation/format/lint, TypeScript and ESLint, but does not
run behavioral tests.

Initial high-value coverage should include:

- concurrent `HubRegistry` admission at and below capacity,
- one pipeline for concurrent sockets sharing a client id,
- idle eviction, reconnect cancellation and startup-failure cleanup,
- isolation between two client ids,
- REST command acknowledgement followed by WebSocket state delivery,
- initial and incremental p-SWAMP wire serialization, including non-finite
  measurement values,
- root and prefixed client routing/deep links.

The scaffold demos use `WebSocket.send_json()` with integer/string-only payloads.
That is not presently the same correctness failure as passing p-SWAMP measurement
models containing `NaN` through the standard JSON encoder. The strict
`pswamp_web.wire.send_state()` requirement remains appropriate for p-SWAMP data;
the demos' untyped dictionaries are a consistency and testability concern rather
than a demonstrated merge blocker.

### 6. Low: the review range fails `git diff --check`

Trailing whitespace was reported in:

- `doc/client-server-rig.md`, including lines 39, 52, 167 and 349-357,
- `doc/possible-performance-issues-to-followup.md`, line 5.

`doc/client-server-rig.md` also has an extra blank line at EOF. Clean these before
the final merge.

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

The full `./scripts/error_check.sh` did not run because this Windows shell could
not find `node`, `npx` or `uv` on `PATH`. This is an environment limitation, not
evidence that the checks pass or fail. Run the complete gate in the configured
development environment before merging.

## Suggested merge order

1. Fix and test global pipeline admission.
2. Add the focused backend lifecycle and protocol tests listed above.
3. Pin the remote deployment to an immutable image and update the operations
   guide.
4. Add remote readiness and liveness probes.
5. Reconcile stale process-wide/per-client documentation.
6. Remove whitespace errors and run `./scripts/error_check.sh` plus an image
   build.

This document is intentionally a WIP. Keep findings here until they are either
fixed and verified or explicitly accepted with rationale before the final merge.