# Review of the client-server rig

## Review scope

This reviews commit `0fd300fa572c92f1219588c0f176a88a4175be70`
(`Overlay client server poc draft into existing pswamp project`) against its
parent.

The commit adds the FastAPI and React client-server stack, the Kubernetes and
container deployment path, CI, generated API contracts, development scripts,
and contributor guidance. It contains 144 changed files, 30,488 insertions, and
700 deletions.

This document records review findings only. No fixes were made as part of the
review.

Every finding below was re-verified independently against the diff rather than
carried over on trust; the verification method is stated per finding, and the
reproductions were re-run in the server's own environment.

## Overall assessment

The architecture is thoughtfully documented, the generated API contract is
useful, and `error_check.sh` passes end to end. Much of the lifecycle code — the
eviction and lock ordering, the thread-to-loop bus seam, the SPA fallback, the
runtime base-path discovery — is correct under scrutiny. However, the rig should
not yet be treated as ready for unrestricted contributor work.

The review confirmed two high-severity availability defects in code that ships
in the image and one contributor-tooling defect (all three from the initial
pass), and a fresh look surfaced **one additional availability defect** — an
unbounded per-client lock leak in the pipeline registry. There are also
important test and ownership gaps that reduce confidence in future changes.

The single highest priority is finding 1: under a concurrent-connect burst it
can push the sole pod past its memory limit and get it OOM-killed, taking every
client down with it.

## Findings

### 1. High: concurrent clients can bypass `MAX_PIPELINES`

**Status: confirmed, reproduced deterministically.**

`HubRegistry.acquire` protects pipeline creation with a per-client lock:

- [`HubRegistry.acquire`](../app/server-python/src/pswamp_web/hub.py#L345)
- [`HubRegistry._make_room`](../app/server-python/src/pswamp_web/hub.py#L414)

Different client IDs therefore execute pipeline creation concurrently:

1. Each request sees capacity available in `_make_room()`.
2. Each begins constructing a hub via `asyncio.to_thread` at
   [`hub.py:368`](../app/server-python/src/pswamp_web/hub.py#L368), yielding
   control.
3. The new entry does not count toward `live` until it is registered at
   [`hub.py:372`](../app/server-python/src/pswamp_web/hub.py#L372).

Consequently, a burst of distinct clients can all pass the capacity check
before any entry is registered. This defeats the memory and thread safety limit
that Kubernetes resource sizing relies upon.

The behavior was reproduced deterministically with a lightweight stand-in for
`Hub`:

```text
pipeline started for client 0 (1/2 live)
pipeline started for client 1 (2/2 live)
pipeline started for client 2 (3/2 live)
max_pipelines=2, live=3
```

With real hubs, each excess entry adds roughly 30 MB and four threads. A
concurrent connection burst can therefore exceed the documented eight-pipeline
bound and potentially cause the pod to run out of memory.

The impact is sharper than "exceeds a documented bound." The Kubernetes
manifest sizes [`limits.memory`](../k8s/p-swamp-local.yaml#L98) at exactly
`base + 8 x 30 MB`, and the stack is `replicas: 1` with `strategy: Recreate`.
A concurrent-connect burst — trivial to induce, since CORS is open and the
client id is explicitly not a credential — can therefore push resident memory
past the limit and get the **sole replica OOM-killed, taking down every
connected client**. A secondary consequence: the polite `1013` "at capacity"
refusal in [`connected_hub`](../app/server-python/src/pswamp_web/hub.py#L547)
only reliably fires for non-overlapping acquires, so the cap is effectively
unenforced under precisely the concurrent load it exists to bound.

The behaviour was re-verified at higher concurrency during this review: six
concurrent distinct clients produced six live pipelines against a cap of two,
with `acquired=6 refused=0` — confirming the `1013` refusal path never fired
under the overlapping load.

### 2. High: the PMU streamer permits permanent, unauthenticated CPU amplification

**Status: confirmed by code inspection.** The ticker rate is
`TICKS_PER_SECOND = SAMPLE_HZ * STATIONS_PER_FRAME = 100`
([`model.py`](../app/server-python/src/pmu_test_streamer/model.py#L25)).

The deployed PMU demo retains every client state indefinitely in
[`pmu_test_streamer/api.py`](../app/server-python/src/pmu_test_streamer/api.py#L57).
More importantly:

- [`get_state()`](../app/server-python/src/pmu_test_streamer/api.py#L60)
  creates state even for a REST command with no socket connected.
- [`play()`](../app/server-python/src/pmu_test_streamer/api.py#L212) marks that
  state as playing.
- The global [`ticker()`](../app/server-python/src/pmu_test_streamer/api.py#L153)
  walks every remembered state at 100 Hz, advancing all playing entries at
  [`api.py:175`](../app/server-python/src/pmu_test_streamer/api.py#L175).
- Disconnecting at
  [`api.py:274`](../app/server-python/src/pmu_test_streamer/api.py#L274) neither
  pauses nor removes the state.

Because
`POST /api/pmu-test-streamer/playback/play?client_id=<new-id>` needs no existing
socket or authentication, repeated requests with distinct IDs create permanent
background work. Each ID adds 100 state updates and empty socket-registry
deliveries per second until process restart.

Fair caveats on severity: this is the demo already slated for retirement, the
amplification is CPU and memory rather than network (a send to a socketless
client is a no-op), and the whole surface is deliberately unauthenticated.
Unlike the reference app's tiny static leak, however, this one grows *ongoing*
per-request CPU work, and it ships in every image. Retiring it, or at minimum
pausing on disconnect and bounding `states`, is the clean move before
onboarding.

Although this is described as an old demo, it is still included in the
production image and registered in
[`APPS`](../app/server-python/src/server.py#L75). It therefore exposes an
avoidable CPU-exhaustion path in every deployment.

### 3. Medium: the advertised subapp generator does not safely handle its label input

**Status: confirmed, injection demonstrated.** Rendering the generator's two
interpolations with the label `Operator's View` produces the nav line
`{ to: '/ops-view', label: 'Operator's View', end: false }` — an unterminated
TypeScript string literal. (The Python `AppEntry` description line survives an
apostrophe, but a double quote breaks it, and a backslash or newline breaks
both.)

[`generate-new-subapp.sh`](../scripts/generate-new-subapp.sh#L24) accepts an
arbitrary navigation label, but validates only the slug. The label is
interpolated directly into:

- a double-quoted Python string at
  [`generate-new-subapp.sh:159`](../scripts/generate-new-subapp.sh#L159);
- a single-quoted TypeScript string at
  [`generate-new-subapp.sh:195`](../scripts/generate-new-subapp.sh#L195).

A plausible label such as `Operator's View` produces invalid TypeScript.
Quotes, backslashes, or newlines can similarly break one or both generated
languages.

This is worsened by the generator's mutation order. It creates directories and
files beginning at
[`generate-new-subapp.sh:120`](../scripts/generate-new-subapp.sh#L120), then
discovers registry-anchor problems later through
[`edit()`](../scripts/generate-new-subapp.sh#L138). A failure leaves a
partially generated subapp and possibly partially patched registries for the
contributor to untangle. The bad-label case is worse still: generation
*succeeds*, and the breakage only surfaces later as a confusing `tsc` error in
the generated nav file, by which point the two folders and four registry
patches are already on disk with no rollback — and a re-run fails with "already
exists." The label should be escaped per target language (or `json.dumps`-ed),
and the whole operation made atomic.

Because this script is presented as the primary path for newcomers to add
functionality, this is a meaningful collaboration-tooling defect.

### 4. Medium/Low: the pipeline registry leaks a lock per distinct client id

**Status: confirmed, reproduced.** This was not in the initial pass; it came out
of a fresh read of the registry's eviction paths.

The registry keeps a per-client `asyncio.Lock` in
[`HubRegistry._locks`](../app/server-python/src/pswamp_web/hub.py#L309) to
serialise a client's concurrent first-connects. Two eviction paths remove a
pipeline, and they clean the lock differently:

- The idle path holds the client's own lock —
  [`_evict_when_idle`](../app/server-python/src/pswamp_web/hub.py#L432) calls
  [`_evict`](../app/server-python/src/pswamp_web/hub.py#L447) from inside
  `async with lock:`. The lock cleanup at
  [`hub.py:454`](../app/server-python/src/pswamp_web/hub.py#L454) only drops the
  lock when `not lock.locked()`, so on this path the entry is removed but the
  lock is **retained forever**.
- The capacity path (`_make_room` evicting another client) does not hold the
  victim's lock, so it *does* reclaim it.

The idle path is the common one — a client that closes its tab is evicted after
`IDLE_EVICT_SECONDS` (300 s). So `_locks` grows monotonically with the number of
distinct client ids the process has ever seen and is essentially never reclaimed
in normal operation. This was reproduced: after five clients connect,
disconnect, and idle-evict, `live` is 0 (no pipelines) but all five locks remain
in `_locks`; the capacity path, by contrast, correctly drops its victim's lock.

This is the same "permanent object per distinct id" leak class the review flags
in findings 2 and the reference app — but here it is inside the very registry
built to *guarantee* bounded resource use, and it is reachable by the same
unauthenticated, id-cycling client the CORS note acknowledges. The objects are
small, so this is slow, not acute; the reason it is not merely cosmetic is that
it quietly defeats the bounded-RSS property the registry exists to provide. The
`not lock.locked()` guard is deliberate (it prevents a documented double-pipeline
race), so a fix must reclaim the lock *after* the idle path releases it, or move
to a sweep or refcount scheme — not simply always-pop.

## Readiness and coverage gaps

These are not additional confirmed runtime bugs, but they materially reduce
confidence in the large addition.

### The smoke test does not exercise the real application

The CI smoke test intentionally exercises only the reference counter, not the
actual grid-monitor pipeline or browser UI. This limitation is documented in
[`smoketest.sh`](../scripts/smoketest.sh#L18).

The reference app proves routing, REST commands, WebSocket delivery, client IDs,
and basic image assembly. It does not prove that the p-SWAMP pipeline starts,
that its applications remain alive, or that the React panels render and invoke
their controls.

### The new lifecycle and concurrency code has no unit tests

There are no unit tests for `HubRegistry`, pipeline eviction and capacity,
replay behavior, or the React client. The capacity race above is exactly the
kind of defect that a concurrent registry test should catch.

### The root desktop application receives only syntax checking in the new gate

The static gate checks root desktop Python only for syntax and does not run its
tests. This matters because the commit also contains a large rewrite of the
root [`uv.lock`](../uv.lock).

The full desktop test suite was not run in this pass (it needs Kafka/NQKafka,
GUI, and simulation infrastructure), so the effect of the root `uv.lock` rewrite
on the desktop application is unverified here.

### Open-source contributor guidance is unfinished

[`how-the-project-interacts-with-open-source-contributors.md`](how-the-project-interacts-with-open-source-contributors.md#L3)
is still only a TODO. This is a notable gap immediately before inviting more
contributors into the repository.

### Ownership coverage is incomplete

[`CODEOWNERS`](../CODEOWNERS#L41) still contains ownership TODOs and does not
assign owners to most of the new grid-monitor backend and frontend
implementation. Contributors changing those areas will not automatically
attract knowledgeable reviewers.

Repository settings are outside the diff, so the documented branch-protection
requirements should also be verified before onboarding contributors. In
particular, `static-errorcheck` and `smoketest` should be configured as required
checks for `main`.

## What held up under scrutiny

Not everything examined is a problem; several load-bearing parts were checked
specifically because they are the ones a subtle bug would hide in, and they are
correct:

- **The static gate is green.** [`scripts/error_check.sh`](../scripts/error_check.sh)
  passes in full (tsc, eslint, `uv lock --check`, `py_compile`, ruff `-F`, and
  the generated-contract check), re-run during this review.
- **Registry eviction ordering is otherwise sound.** Apart from the lock leak in
  finding 4, the capacity/idle interplay is careful: victim selection and removal
  in [`_make_room`](../app/server-python/src/pswamp_web/hub.py#L414) /
  [`_evict`](../app/server-python/src/pswamp_web/hub.py#L447) have no `await`
  between choosing a victim and popping it, so a reconnecting client is not torn
  down mid-flight, and `Hub.stop` is idempotent so the two evictors cannot
  double-free.
- **The thread/loop seam holds.** The bus crosses from p-SWAMP's daemon threads
  into the event loop only via `call_soon_threadsafe`, preserving the documented
  two-seam rule; nothing outside `pswamp_web/` introduces a thread.
- **The contract guard works.** `api_contract.check_apps` does refuse startup
  when an app serves a socket but exports no `WS_MESSAGE`, closing the "silently
  drops out of the contract" hole for the total-omission case.
- **The image and manifest are coherent.** The [`Dockerfile`](../Dockerfile)
  layering, non-root user, and digest-pinned multi-arch bases are careful; the
  [`k8s manifest`](../k8s/p-swamp-local.yaml) single-replica / `Recreate` /
  probe / securityContext choices are internally consistent and match the stated
  in-memory-state constraint.
- **Base-path handling is correct.** [`basePath.ts`](../app/client-web/src/lib/basePath.ts)
  and [`servers.ts`](../app/client-web/src/lib/servers.ts) correctly resolve both
  the origin root and the stripped `/p-swamp/` reverse-proxy prefix.

## Validation performed

This was an independent pass; the items below are what *this* review actually
executed, not what the earlier draft reported.

- Reviewed commit `0fd300fa572c92f1219588c0f176a88a4175be70` against its parent
  (144 files, ~30.5k insertions).
- Ran [`scripts/error_check.sh`](../scripts/error_check.sh): passed (tsc, eslint,
  `py_compile`, ruff, `uv lock --check`, generated-contract check).
- **Reproduced finding 1** with a stubbed `Hub`: six concurrent distinct clients
  built six live pipelines against a cap of two (`acquired=6 refused=0`).
- **Reproduced finding 4** with a stubbed `Hub`: after five clients idle-evicted,
  `live` returned to 0 but all five locks stayed in `_locks`; the capacity path
  reclaimed its victim's lock correctly.
- **Demonstrated finding 3** by rendering the generator's interpolations with the
  label `Operator's View`, yielding an unterminated TypeScript string.
- Confirmed finding 2 by reading the streamer's ticker/state lifecycle
  (`TICKS_PER_SECOND = 100`, no pause-on-disconnect, unbounded `states`).
- Confirmed the worktree was otherwise unchanged before this document was added.

The review did **not** re-run the Dockerfile build, the built-image smoke test, a
live five-socket grid-monitor connection, the full legacy desktop suite, a
minikube deployment, or any browser-driven UI test. Several legacy tests require
Kafka/NQKafka, GUI components, or simulation infrastructure, and the repository
contains no browser test layer. Claims of that kind in the earlier draft were not
independently re-verified here and should be treated as unconfirmed.

## Addendum: independent re-verification and follow-up fixes (2026-09-01)

A later independent pass re-checked every finding directly against the code and
**agrees with all four findings and every readiness gap**. It also applied fixes
for three of them. Corrections, clarifications and status below.

**Commit mapping.** This document reviews `0fd300fa…`; the same logical commit is
now `58e7e22cfe4d381e96b5e12092dd170692c4179d` in history (a reword/rebase). The
two differ in only two files — `AGENTS.md` and `AppLayout.tsx` — and none of the
findings' target files changed between them, so every finding transfers to
`58e7e22` unchanged (finding 3's cited `generate-new-subapp.sh` line numbers still
match).

**Finding 1 — confirmed; one clarification.** The bypass requires ≥`MAX_PIPELINES`
*distinct* client ids arriving within one construction window. The normal
single-browser case is safe by design: the grid monitor's five sockets share one
client id and therefore one lock, so they build one pipeline. "Trivial to induce"
holds for an adversarial or mass-concurrent burst, not for ordinary single-user
load. **Fixed** (below).

**Finding 2 — confirmed; wording and severity.** "A send to a socketless client is
a no-op" is right, but the ticker still *builds* `state_message(state)` (a
`visible_window()` plus a pydantic model) each tick for every zombie, so there is
genuine per-tick CPU — only the delivery is a no-op. Severity is a judgement call:
High is defensible as unbounded ongoing work shipping in every image, but the
retirement status, the unauthenticated-by-design surface, and the modest
per-zombie cost put it as low as Medium. **Left as-is** pending the streamer's
planned retirement; the clean fix is to retire it, or at minimum pause on
disconnect and bound `states`.

**Finding 3 — confirmed; "injection" overstates it.** The label is typed by the
same person running the generator, so this is a self-inflicted footgun in the
onboarding tool rather than a security-boundary crossing — but a real
tooling-robustness defect, Medium. **Fixed** (below).

**Finding 4 — confirmed as written**, including that the `not lock.locked()` guard
is deliberate. **Fixed** (below).

### Follow-up fixes applied

- **Finding 1** — `HubRegistry` now counts pipelines *under construction* against
  the cap. `acquire` reserves a slot (`_pending`) before the off-loop `Hub.start`,
  and `_make_room` tests `live + _pending >= max_pipelines`, so a concurrent burst
  of distinct clients is admitted only up to the cap and the rest get the `1013`
  refusal. Verified with a stubbed `Hub`: a six-way concurrent burst against a cap
  of two yields `acquired=2 refused=4`, `live=2`.
- **Finding 4** — the idle path no longer leaks a lock. Reclamation is keyed on a
  new `_acquiring` counter (incremented *before* the lock is taken, so a queued
  waiter counts) instead of `lock.locked()`, so the idle evictor can drop the lock
  it is holding while still refusing to drop one a concurrent connect is racing on.
  Verified: after five clients idle-evict, `_locks` is empty and `live` is 0.
- **Finding 3** — `generate-new-subapp.sh` now escapes the label at both code sites
  (a single-quoted TS literal and a Python string literal) and validates it against
  the characters that would break the prose sites it also lands in (docstrings, JSX
  text, JS comments). Generation is now **atomic**: the whole change set — rendered
  files and registry patches — is computed in memory and committed only once
  everything validates, with rollback on a write error, so bad input can no longer
  leave a half-generated subapp. Verified end-to-end: generating a subapp with the
  label `Operator's View` now passes `error_check.sh` (it previously emitted
  unterminated TypeScript).

The readiness/coverage gaps largely still stand, but the specific "no unit tests
for `HubRegistry`" gap is now **closed**: `app/server-python/tests/test_hub_registry.py`
is a committed suite that pins both registry fixes (and fails on the pre-fix code),
run with `./scripts/run-python-server-tests.sh`. Broader web-backend coverage is
still thin, and the desktop suite (repo-root `tests/`) remains infra-bound and
ungated — it has its own deliberately-run script, `./scripts/run-core-python-tests.sh`,
and is intentionally kept out of `error_check.sh` and CI.
