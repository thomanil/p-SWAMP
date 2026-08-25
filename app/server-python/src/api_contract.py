"""The published api contract: document metadata, and the socket half of it.

`server.py` mounts the apps; this module describes them. It is deliberately not
an app package -- it exposes no `router`, never appears in `APPS`, and holds no
domain logic -- so it sits beside `shared.py` rather than under it.

Why this exists
==

FastAPI already describes the *upstream* half of the api for free: every command
is a POST with an explicit `operation_id`, a pydantic body and a `CommandAck`
reply, so `/openapi.json` has always listed them. The *downstream* half was
invisible, because OpenAPI describes HTTP operations and every byte of state in
this system arrives over a WebSocket. The message models existed -- they are the
whole of `pswamp_web/wire.py` -- they were simply never published, so the web
client hand-maintained a TypeScript mirror of each one and a renamed field failed
at runtime rather than in any check.

This module closes that gap without a second spec format. It merges the socket
message schemas into `components.schemas` and records which channel carries which
under a vendor extension, so one document describes both directions and one
generator turns it into TypeScript.

How a package joins the contract
==

By exporting a name. `server.py` already discovers optional package features with
`getattr(module, "lifespan", None)`; socket messages work the same way::

    # timeline/__init__.py
    WS_MESSAGE = TimelineState

`ws_channels` walks the very `APPS` list `server.py` mounts and picks up whatever
each package exports under that name. There is no second registry to keep in
step, and `scripts/generate-new-subapp.sh` needs no extra anchor to patch -- a
scaffolded subapp is in the contract the moment it is generated, because its
template exports the name. An app with no socket (`pswamp_web/grid/`, which is
HTTP only) simply omits it.

The vendor extension
==

Channels land at the document root as `x-websocket-channels`::

    {"path": "/api/time-window/ws", "app": "time-window",
     "direction": "server-to-client",
     "message": {"$ref": "#/components/schemas/TimeWindowSlice"}}

An extension rather than AsyncAPI, deliberately: a second spec format would mean
a second generator, a second drift check and a second thing for another team to
learn, to describe seven one-way channels. `x-` keys are legal OpenAPI and every
generator ignores what it does not know, so the document stays valid and the
schemas -- which is what codegen actually consumes -- come through as ordinary
`components.schemas` entries.
"""

import json
from typing import Literal

from fastapi import FastAPI
from fastapi.openapi.utils import get_openapi
from pydantic import BaseModel
from pydantic.json_schema import models_json_schema

# --- document metadata -------------------------------------------------------

API_TITLE = "p-SWAMP api"

# Bump on a BREAKING contract change -- a removed or renamed field, a removed
# operation, a narrowed type. That is the point of it: the version is the one
# thing in the generated document a reviewer can look at to tell whether a diff
# is additive or not, and `doc/api/openapi.json` is committed so the bump shows
# up in the pull request beside the change that forced it.
#
# Not read from pyproject.toml on purpose: this project is `package = false`, so
# there is no installed distribution to read a version from, and the api's
# compatibility is not the same fact as the server's release number anyway.
API_VERSION = "1.0.0"

API_DESCRIPTION = """\
The api between the p-SWAMP web client and its state server.

**Two directions, two transports.** Anything a user triggers goes up as an HTTP
`POST` under `/api/<app>/`; everything the server has to say comes down a
WebSocket at `/api/<app>/ws`, which is downstream only. A command's reply is a
small `CommandAck`, never state, so state has exactly one path and there is no
ordering to reconcile between two of them.

**The operations below are therefore only half the api.** The socket half is
described by the `x-websocket-channels` extension at the root of this document,
whose message schemas are ordinary `components.schemas` entries — so a code
generator picks them up like any other model even though OpenAPI itself has no
notion of a socket.

**Every request carries `?client_id=`**, a numeric string the browser resolves
once per profile and persists. It is the sharding key for all server state and
all of a browser's sockets must send the same one. It is *not* authentication and
does not pretend to be: supply someone else's id and you drive their session.

This document is generated from the server and committed at `doc/api/openapi.json`;
`scripts/error_check.sh` fails if the two disagree. See `doc/the-client-server-api.md`.
"""

# One line per app, keyed by its url name (the `/api/<app>` segment, which is also
# the tag `server.py` applies). Purely descriptive: a missing entry costs nothing
# but an untagged-looking group in Swagger UI.
TAG_DESCRIPTIONS = {
    "timeline": "Scaffold demo: a scrolling number sequence with playback controls.",
    "pmu-test-streamer": "Scaffold demo: replays sample PMU records line by line.",
    "app-status": "Health and status of the running p-SWAMP monitoring applications.",
    "grid": "The static Nordic 44 topology. HTTP only — no socket.",
    "time-window": "The measurement window: channel selection and the sample stream.",
    "islanding": "Islanding detection results and the alarms derived from them.",
    "line-outage": "Detected line disconnect/reconnect events.",
    "phasors": "Voltage phasors, referred to a rotating reference.",
}


class HealthStatus(BaseModel):
    """Reply to `GET /healthz`, the liveness/readiness probe."""

    status: Literal["ok"] = "ok"


def openapi_tags(apps) -> list[dict]:
    """Tag metadata for the document, in the order `APPS` mounts the apps."""
    tags = []
    for prefix, _module in apps:
        name = prefix[len("/api/") :]
        tag = {"name": name}
        description = TAG_DESCRIPTIONS.get(name)
        if description:
            tag["description"] = description
        tags.append(tag)
    return tags


# --- the socket half ---------------------------------------------------------


def ws_channels(apps) -> list[tuple[str, str, type[BaseModel]]]:
    """`(channel path, app name, message model)` for every app with a socket.

    Discovered from the packages themselves via `WS_MESSAGE`, exactly as
    `server.py` discovers `lifespan`. Order follows `APPS`, so the generated
    document is stable across runs.
    """
    channels = []
    for prefix, module in apps:
        message = getattr(module, "WS_MESSAGE", None)
        if message is None:
            continue
        channels.append((f"{prefix}/ws", prefix[len("/api/") :], message))
    return channels


def _merge_schemas(components: dict, new: dict) -> None:
    """Add generated schemas to the document, refusing to redefine one.

    Some models are reachable from both halves of the api -- `Channel` sits under
    `GET /api/time-window/channels` *and* inside `TimeWindowSlice` -- so an
    overlap is normal and an identical redefinition is fine. A *differing* one is
    not: it would mean the document quietly disagreed with itself about a shape,
    which is the exact failure a published contract exists to prevent, and it
    would be invisible in review. Fail loudly instead.
    """
    for name, schema in new.items():
        existing = components.get(name)
        if existing is not None and existing != schema:
            raise RuntimeError(
                f"Two different schemas are both called {name!r}. Give one an "
                f"explicit `model_config = ConfigDict(title=...)` -- see the "
                f"CommandAck twins in shared.py and pswamp_web/wire.py.\n"
                f"  existing: {json.dumps(existing, sort_keys=True)[:400]}\n"
                f"  new:      {json.dumps(schema, sort_keys=True)[:400]}"
            )
        components[name] = schema


def _rewrite_refs(node, renames: dict[str, str]) -> None:
    """Point every `$ref` at its schema's new name, in place."""
    if isinstance(node, dict):
        ref = node.get("$ref")
        if isinstance(ref, str):
            name = ref.rsplit("/", 1)[-1]
            if name in renames:
                node["$ref"] = f"#/components/schemas/{renames[name]}"
        for value in node.values():
            _rewrite_refs(value, renames)
    elif isinstance(node, list):
        for value in node:
            _rewrite_refs(value, renames)


def collapse_titled_twins(schema: dict) -> dict:
    """Give a deliberately duplicated model one name in the document.

    `CommandAck` is declared twice on purpose -- once in `shared.py` and once in
    `pswamp_web/wire.py`, because that package may not import the rest of the web
    backend (it is written to move into the desktop package as `pswamp/web/`).
    Pydantic sees two classes of one name and disambiguates them by MODULE PATH,
    so the reply to all fourteen commands would otherwise publish as
    `shared__CommandAck` and `pswamp_web__wire__CommandAck`.

    That is bad twice over: it presents one concept under two names, and it bakes
    a module path into the public contract -- a path that is documented to be
    moving, which would silently rename a schema in every consumer's generated
    code the day that move happens.

    So: where several disambiguated schemas share an explicit `title` (the
    `model_config = ConfigDict(title=...)` on both twins) and are structurally
    identical apart from their prose description, collapse them to that title and
    repoint every `$ref`. Twins that have genuinely DIVERGED keep their separate
    machine-chosen names -- at that point they are two different shapes, and
    saying so loudly is right.
    """
    components = schema.get("components", {}).get("schemas", {})
    by_title: dict[str, list[str]] = {}
    for key, body in components.items():
        title = body.get("title")
        if title and title != key:
            by_title.setdefault(title, []).append(key)

    renames: dict[str, str] = {}
    for title, keys in by_title.items():
        # Nothing was disambiguated, or a distinct class already owns the name.
        if len(keys) < 2 or title in components:
            continue
        bodies = [
            {k: v for k, v in components[key].items() if k != "description"}
            for key in keys
        ]
        if any(body != bodies[0] for body in bodies[1:]):
            continue
        # Keep the first spelling's description; APPS order makes that stable.
        canonical = dict(components[sorted(keys)[0]])
        for key in keys:
            del components[key]
            renames[key] = title
        components[title] = canonical

    if renames:
        _rewrite_refs(schema, renames)
    return schema


def inject_ws_channels(schema: dict, apps) -> dict:
    """Merge the socket message schemas into `schema` and list the channels.

    Mutates and returns the document.
    """
    channels = ws_channels(apps)
    if not channels:
        return schema

    # One call for ALL the models rather than one per model: they share nested
    # definitions (`Channel`, `Alarm`, `ReplayStatus`), and generating them
    # separately would emit each shared definition several times under
    # generator-chosen names instead of one `$ref` they all point at.
    #
    # "serialization" because these are outbound messages -- it is the shape
    # `model_dump_json` actually puts on the wire, which is what the client parses.
    models = [(model, "serialization") for _path, _app, model in channels]
    _keys, defs = models_json_schema(
        models,
        ref_template="#/components/schemas/{model}",
    )

    components = schema.setdefault("components", {}).setdefault("schemas", {})
    _merge_schemas(components, defs.get("$defs", {}))

    schema["x-websocket-channels"] = [
        {
            "path": path,
            "app": app,
            # Downstream only, everywhere. A socket that receives nothing still
            # has a receive loop, but only so a disconnect is noticed promptly --
            # nothing a client sends up one is ever read as a command.
            "direction": "server-to-client",
            "message": {"$ref": f"#/components/schemas/{model.__name__}"},
        }
        for path, app, model in channels
    ]
    return schema


# --- assembly ----------------------------------------------------------------


def build_document(app: FastAPI, apps) -> dict:
    """The finished contract: what `/openapi.json` serves and what the committed
    `doc/api/openapi.json` holds. One function, so the two can never differ."""
    schema = get_openapi(
        title=API_TITLE,
        version=API_VERSION,
        description=API_DESCRIPTION,
        routes=app.routes,
        tags=openapi_tags(apps),
        separate_input_output_schemas=app.separate_input_output_schemas,
    )
    inject_ws_channels(schema, apps)
    return collapse_titled_twins(schema)


def install(app: FastAPI, apps) -> None:
    """Make `app.openapi()` -- and so `/openapi.json`, `/docs` and `/redoc` --
    serve the document above rather than FastAPI's default."""

    def openapi() -> dict:
        if app.openapi_schema is None:
            app.openapi_schema = build_document(app, apps)
        return app.openapi_schema

    app.openapi = openapi
