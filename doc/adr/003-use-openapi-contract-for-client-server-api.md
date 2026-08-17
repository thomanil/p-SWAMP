# ADR-003: Use an OpenAPI contract for the client-server api

- Status: Accepted
- Date: 2026-08-25

## Context

Splitting the stack in two ([ADR-002](002-use-python-backend-with-typescript-frontend.md))
left the message shapes as an agreement between a Python backend and a
TypeScript client. The client's types started as hand-written mirrors of the
pydantic models, which drift silently: Vite strips types rather than checking
them, so a renamed field compiles fine and shows up as an empty panel in the
browser instead of an error.

The api has two halves and only one of them is HTTP. Commands go up as POSTs;
state comes down over WebSockets, which OpenAPI has no notion of — and the
socket half is exactly the half carrying the domain payloads.

## Decision

We will publish the api as a single generated OpenAPI document,
`doc/api/openapi.json`, committed to the repo and generated from the FastAPI app
by `scripts/generate-api-contract.sh`. The socket half rides in the same
document, as `components.schemas` entries plus an `x-websocket-channels`
extension.

The generation runs code-first: the Python is the source of truth and the
document is derived from it, never hand-authored.

The client's wire types are generated from it into
`app/client-web/src/api/schema.ts`; page hooks say `Wire['SomeState']` rather
than hand-copying a model. `scripts/error_check.sh` fails while either artifact
is stale, so the contract cannot silently fall behind the code.

## Consequences

A backend model change becomes a `tsc` error in the client rather than a blank
panel — including typo'd command paths and wrong request bodies, since
`postCommand` is typed against the same document. The document also serves as
the api's documentation for free, at `/docs` and in the repo.

In exchange, an api change is now a two-step change: edit, then regenerate and
commit both artifacts. Nothing warns you in between, which is why the check is
in `error_check.sh` and the pre-push hook. The generated types are the wire
vocabulary only — the camelCase mapping stays hand-written on the client side —
and breaking changes still need `API_VERSION` bumped by hand.

The `x-websocket-channels` extension is ours, so no third-party tool understands
it. That is acceptable while it is read by our own generator and by people.

## Alternatives considered

- **Hand-written types on both sides.** What we had. Free until it is wrong, and
  it is wrong silently.
  
- **Spec-first: hand-author the contract, generate both sides from it.** The
  standard design-first pattern (`datamodel-code-generator`, OpenAPI Generator,
  TypeSpec), and the direction we deliberately did not take. Worth revisiting if the contract
  has to outlive its implementation, or if the socket protocol grows
  bidirectional channels.
- **AsyncAPI for the socket half.** The standards-correct answer, but a second
  spec format and a second generator to maintain for seven one-way channels.

- **gRPC or protobuf.** A real schema across both halves, but it would replace
  the browser-native HTTP + WebSocket transport and add a build step and a proxy
  for what is a small api.
