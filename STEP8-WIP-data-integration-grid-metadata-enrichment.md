# STEP 8 — A CIM reference on PMU frames, stubbed in the gateway

Working note behind the eighth step of the data-integration track. Frames leave
their providers with a channel layout and values: which station each column is
from, by the label the stream uses, and nothing about that station in the
grid. Analyses will want to know which grid (CIM) data applies to what they
are looking at. This step puts the **seam** for that in place, and nothing
more: the gateway stamps an optional `cimReferenceId` on each frame early in
the pipeline, and something later in the pipeline picks it up again.

The reference is a **stub**. It is a configured placeholder, not the result of
a CIM lookup: no module in the slice uses grid data yet, so there is no grid
model, no metadata source and no lookup behind it.

## Decisions (asked and settled)

| Question | Decision |
|---|---|
| Shape | **An optional `PmuHeader.cimReferenceId: str | None`.** A reference to the grid data relevant for the frame, not the data itself, and not a new frame type or a separate message. |
| Who sets it | **Only the gateway, early, along the way.** Providers never set it: a provider's frame arrives with `None`. |
| Where | **An enricher in the gateway's stream**, `CimReferenceEnricher`, deciding the reference once per layout in `reference_for`. |
| What it stamps today | **A configured placeholder** (`ISLANDING_STREAM_CIM_REFERENCE`, default `n44-stub`, `none` to switch off). A CIM-backed enricher overrides `reference_for` and nothing else changes. |
| Picking it up | **Read it off the frame.** It travels in the header, so a module reads it wherever it runs, a worker included, with no configuration of its own. The islanding module returns it with each result as `cim_reference_id`. |

### Why the header, and not the alternatives

A reference is **per layout and static**: it describes what the columns
*are*, not what one instant measured. That is what the header already is.

- **A subclass (`EnrichedPmuFrame`)** would make the type explicit, but a
  message's topic is derived from its class: it would publish on
  `enriched.pmu.frame`, so the worker hop (which tails `pmu.frame`), every
  module's `input_model` and every provider's `supported_models` would change
  or have to learn about subclasses.
- **A separate message** on the bus would undo the self-describing frame that
  was chosen deliberately (commit 88af5ca removed the separate `pmu.header`
  topic): a worker or a late subscriber would again need priming from a
  second topic.
- **The grid data itself on the header** would ride on every hop. Measured
  in an earlier take, 44 stations' position, voltage level and area cost
  +16 % bytes on a full N44 frame and x4 on a frequency-only one.
- **An optional reference** is additive on the wire -- the message models
  ignore fields they do not know, so an old reader skips it and a new reader
  of an old frame sees `None` -- and changes no topic, no provider and no
  transport. `header_id` stays the hash of the layout alone, so a module that
  derives column indexes does not re-derive when a reference arrives.

### Why the gateway's stream

`DataStream._iterate` is the one path every reader of the gateway goes through
-- the player, and any module that queries a range itself (the explorer's
`RowCountModule`). An **enricher** step there, just before `yield` (after the
watermark and de-duplication), means every reader sees the same stamped
frame, and nothing upstream (providers) or downstream (player, bus, modules,
transport, worker) changes. The gateway opens and closes enrichers with its
clients, so a real enricher can load its CIM model in `open`;
`gateway_from_env(..., enrichers=...)` needed no change, since it already
forwards keyword arguments to `DataGateway`.

## What exists

Core:
- `PmuHeader.cimReferenceId`, optional, outside `header_id`.
- `datagateway/enrich.py`:
  - `Enricher`, a base with async `open`/`close` and a synchronous,
    I/O-free `enrich`;
  - `CimReferenceEnricher`, which asks `reference_for` once per `header_id`
    and shares one stamped header across every frame with that layout, so per
    frame it is a lookup and a shallow copy. The stub `reference_for` returns
    the reference it was built with.
- `DataGateway(enrichers=...)` and `DataStream`.
- Tests: `core/tests/test_enrich.py`, including a per-layout subclass as the
  shape a real enricher takes.

Web backend:
- `islanding_stream/api.py` builds the gateway with the stub enricher from
  `ISLANDING_STREAM_CIM_REFERENCE`.
- The islanding module reads `cimReferenceId` off each frame and returns it
  as `cim_reference_id` with its result, in-process and in the worker.
- Tests in `test_islanding_stream.py`: pipeline frames stamped, the module
  returning the reference in-process and across the worker hop, and `None`
  with the enricher off.

Things the implementation relies on, and would break quietly if changed:

- **A frame whose header already carries a reference is not touched.** It
  was stamped before: by a second enricher, or read back off a topic.
- **A layout `reference_for` answers `None` for passes through unstamped**,
  and the answer is cached per layout like any other.

## Open points

1. **A real reference.** The first CIM-backed `reference_for`: work out
   which grid data a layout's stations belong to and name it. The join from a
   stream's station labels (a C37.118 name or IDCODE) to CIM mRIDs is a table
   the deployment owns.
2. **Resolving the reference.** Nothing turns the reference back into grid
   data yet. When an algorithm needs the data, it needs a lookup by id, in
   the server and in a worker.
3. **Time validity.** A replay of last year's data would be stamped against
   today's grid model. A versioned CIM source would want the stream's time
   range.
4. **What reaches the browser.** The streamer and explorer sockets send whole
   `PmuFrame`s, so the reference reaches those pages; the islanding socket
   sends it as `cim_reference_id`. An opaque id is harmless. Once something
   resolves it into grid detail, that detail must not follow it to the
   browser: the edge should then send view models with only what a page
   draws.
5. **`model_copy` carries cached hashes.** `header_id` is a
   `cached_property`, and pydantic's `model_copy(update=...)` copies the cache
   with the fields. So a header copied with a *changed* layout keeps the old
   hash. The enricher relies on this legitimately (its copy keeps the layout),
   but anyone else who copies a header to change its columns gets a stale
   `header_id`. That predates this step. Rebuilding the model, or clearing
   the cache on copy, fixes it.
