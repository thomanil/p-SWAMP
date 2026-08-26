# WIP: client-server stack cleanup

Working notes for an in-progress cleanup pass over the client-server stack,
branch `cleanup-api-layer`. **Temporary** — delete this file when the last step
lands and the durable conclusions have been folded into `AGENTS.md`.

Kept so the work can be picked up mid-way: each step below is one commit, and the
status table says where the pass got to.

## Why

A review of both halves (React/TS client, FastAPI backend) asked what a new team
member would trip over. The findings were all variations on one theme: the same
idea expressed several ways, with prose in `AGENTS.md` keeping the copies in
step rather than code making the copies impossible.

## The steps, in order

| # | Step | Status |
|---|------|--------|
| 1 | Collapse the `shared.py` / `pswamp_web` twins | done |
| 2 | One send path, one per-client view registry | done |
| 3 | Extract the repeated WebSocket endpoint tail | done |
| 4 | Drop the hand-written snake→camel mapping in the client hooks | done |
| 5 | Move the panel `variant` convention into `<Panel>` | done |
| 6 | Delete `pmu-test-streamer` | **deferred — keep it for now** |
| 7 | Make the `APPS` registry explicit and self-checking | done |
| 8 | Fix stale per-client comments | done |

### 1. Collapse the twins

`pswamp_web/` may not import outward, so it can `git mv` upstream into the
desktop package later. That rule was being satisfied by *duplicating* four
things — `ClientId`, `CommandAck`, `read_client_id`/`CLIENT_ID_PATTERN` and the
stdout logger — plus ~90 lines in `api_contract.py`
(`collapse_titled_twins`/`_rewrite_refs`) whose only job was to reunify the
duplicates in the published contract.

The rule never required it. It forbids `pswamp_web` importing *outward*; the
rest of the backend importing *inward* is fine, and stays fine after the move
(the web backend already depends on `p-swamp`). So: one definition in
`pswamp_web/`, `shared.py` re-exports it.

Deleted: the four twins, `pswamp_web/log.py`'s justification, `collapse_titled_twins`,
`_rewrite_refs`, both `ConfigDict(title="CommandAck")` hacks, and every
"change one, change the other" note attached to them.

### 2. One send path

There were three ways to push a message: `send_state(ws, model)` in
`pswamp_web`, `manager.send_to_client(...)` in the scaffold apps, and a raw
`ws.send_text(model.model_dump_json())` on those apps' connect path. The 126-line
file everyone is told to copy used two of them, and neither was the one the real
application uses.

`ConnectionManager` and `SessionRegistry` were also the same data structure — a
per-client registry of live views — solving the same problem. Unified into
`sessions.py`: `SessionRegistry[T]` plus `SocketRegistry`, a `SessionRegistry[WebSocket]`
that accepts, registers and sends. `send_state` is now the only serialiser, so
the NaN guard exists once.

### 3. The endpoint tail

Five endpoints ended in the same ~15 lines of pusher-task + receive-loop +
cancel/suppress. Extracted to `pswamp_web/pump.py`:

- `serve_ticks(ws, hz, build)` — `app_status`, `phasors`, `time_window`
- `serve_updates(ws, queue, build)` + `event_queue(bus, topics)` — `islanding`,
  `line_outage`

`build` returning `None` means "nothing new", which is what `time_window` needs.

### 4. The client's field renaming

~150 lines across seven hooks existed only to rename `app_name` → `appName`. It
failed *silently*: a `useMemo` that omits a new field is valid TypeScript, so a
field added server-side simply never reached the UI — the exact failure the
generated contract exists to abolish.

Components now read `Wire[...]` types directly. Hooks that genuinely transform
kept their logic (`useLineOutageSocket`'s `branchesOf`,
`useTimeWindowSocket`'s ring buffer); the pass-through ones lost the `useMemo`.

### 5. Panel variant

Six panels each repeated three `variant === 'focused' ? … : undefined`
ternaries. The convention (focused gets a subtitle and no self-link; dashboard
gets the reverse) now lives inside `<Panel>`.

### 7. The APPS registry

`getattr(module, "WS_MESSAGE", None)` meant forgetting the export dropped an app
out of the contract silently. `APPS` entries are now `AppEntry(slug, module,
description)` records, `TAG_DESCRIPTIONS` (a second registry keyed by the same
slug) is folded in, and `api_contract.check_apps` — called from `install`, which
`server.py` runs at import — raises if a package with a WebSocket route exports
no `WS_MESSAGE`. The slug also replaced `prefix[len("/api/"):]`, which was
slicing the url back apart in three places.

### 8. Stale per-client comments

`server.py` and two panels still said the PMU pipeline was process-wide and
shared, which the move to one pipeline per client had made false — in the two
files a newcomer opens first. §8 of
`doc/WIP-ongoing-llm-assisted-review-before-final-merge.md` had this on its list;
it is now closed there too.

## Checking the work

`./scripts/error_check.sh` is the gate, but it never runs the server. There is no
test suite in this repo, so each step here was also smoke-tested in-process
against the real app — health, both scaffold apps' connect/command/push round
trips, a bad client id refused on both transports, all five p-SWAMP sockets
pushing a first message with no bare `NaN` in it, a channel-selection command
producing a `full` message, a command with no open view 404ing, and the grid
model. That script lives in the session scratchpad rather than the repo (adding a
test framework is not this pass's job); reproduce it with
`uv run --with httpx python -` from `app/server-python`, driving
`fastapi.testclient.TestClient(server.app)`. Worth turning into a real test file
one day — it is the closest thing to an integration test the stack has.

## Step 6, deferred

Deleting the `pmu-test-streamer` subapp is the cheapest remaining reduction —
~500 lines across both halves, plus `sample_data.txt`, a nav entry, contract
entries and a paragraph in most of the docs, all for a demo that `AGENTS.md`
already describes as superseded. It stays for now by explicit decision. Every
step above kept it working and treated it as a first-class app, so nothing here
makes removing it harder later.

What that costs meanwhile: it is a second, differently-shaped example sitting
next to the one people are told to copy. `reference-subapp` is the one to copy;
the streamer is worth reading only for its ticker.

## Ground rules for this pass

- No behaviour changes. Same wire format, same URLs, same UI. The two contract
  diffs across the six commits are a docstring and one added tag description.
- `./scripts/error_check.sh` green before each commit; contract regenerated and
  committed where it moved.
- One commit per step, subject prefixed `cleanup(N):`.
- Where a step changed the scaffold templates or the registries the generator
  patches, `./scripts/generate-new-subapp.sh` was run for real afterwards, the
  generated subapp driven over its socket, and the tree reset.

## What this did not touch

Deliberately out of scope, and still true of the tree:

- **No test suite.** The stack has no automated tests at all; this pass leaned on
  a throwaway in-process script (above) and `error_check.sh`. That is the largest
  remaining gap, and the first thing a new team member will miss.
- **The two-registry-per-app spelling.** A slug is still written by hand in the
  package name, the page folder, the route, the nav item and two consts in
  `servers.ts`. The generator writes all of them, so the cost lands only on a
  rename done by hand.
- **`doc/` volume.** ~4,400 lines of prose against ~9,000 of hand-written code.
  Steps 1-3 and 7 deleted several "keep X and Y in sync" instructions by making
  X and Y one thing, which is the only kind of documentation cut worth making
  automatically; the rest is a judgement call nobody has made yet.
