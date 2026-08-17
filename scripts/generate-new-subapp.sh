#!/usr/bin/env bash
# Scaffold a new subapp — a page and its api — wired into the nav, the route
# table, the ws path table and the backend APPS registry (every step in AGENTS.md's
# "Adding a page" / "Adding a backend api").
#
#   scripts/generate-new-subapp.sh grid-overview "Grid Overview"
#
# What comes out already works: a nav entry, a page at /grid-overview, a WebSocket
# pushing state from a new backend package, and two POST commands over a per-client
# counter. Replacing that counter is the only work left.
#
# Generated files come from scripts/templates/ — edit those, not this script, to
# change what a subapp starts life as. Each is <filename>.template; the suffix is
# stripped on render and keeps editors off them.
#
# The subapp joins the api contract with no registry entry (its package exports
# WS_MESSAGE, api_contract.py collects it); this script regenerates both artifacts
# at the end. Commit them with the rest. NO_CHECK=1 skips the error_check.sh run.
set -euo pipefail

# Run from the repo root regardless of where the script is invoked from.
cd "$(dirname "$0")/.."

if [ $# -ne 2 ]; then
  echo 'usage: scripts/generate-new-subapp.sh <url-name> "<Nav Label>"' >&2
  echo '   eg: scripts/generate-new-subapp.sh grid-overview "Grid Overview"' >&2
  exit 1
fi

# python3 does the work: derive the name's spellings, render the templates, and
# insert into the registries by anchor. (Already required by error_check.sh.)
SLUG="$1" LABEL="$2" python3 - <<'PY'
import keyword
import os
import re
import sys
from pathlib import Path


def die(msg):
    print(f"\033[31m{msg}\033[0m", file=sys.stderr)
    sys.exit(1)


# The name shows up in five shapes; keeping them in step by hand is what this
# script removes. Templates spell them __SLUG__, __PKG__ … __LABEL__.
#
#   slug            grid-overview          URL, page folder, /api prefix
#   pkg             grid_overview          Python package (must be an identifier)
#   name            GridOverview           React component, hook, model class
#   ws_path_const   GRID_OVERVIEW_WS_PATH  ws path const in lib/servers.ts
#   api_path_const  GRID_OVERVIEW_API_PATH REST prefix const, same file
#
# Two path consts because the directions use two transports: state down the
# socket, commands up as POSTs (see AGENTS.md).

slug = os.environ["SLUG"]
label = os.environ["LABEL"]
pkg = slug.replace("-", "_")
name = "".join(word.capitalize() for word in slug.split("-"))
ws_path_const = f"{pkg.upper()}_WS_PATH"
api_path_const = f"{pkg.upper()}_API_PATH"

# The 32-char cap keeps the rendered Python lines short and readable. (They used
# to have to fit ruff's 88-column limit; error_check.sh no longer enforces line
# length — formatting and pycodestyle were dropped from the gate.)
if (
    not re.fullmatch(r"[a-z][a-z0-9]*(-[a-z0-9]+)*", slug)
    or len(slug) > 32
    or keyword.iskeyword(pkg)
):
    die(f"{slug!r} is not usable: give lowercase words joined by hyphens, max 32 chars.")

WEB = Path("app/client-web/src")
PY_SRC = Path("app/server-python/src")
page_dir = WEB / "pages" / slug
api_dir = PY_SRC / pkg

if page_dir.exists() or api_dir.exists():
    die(f"{slug} already exists as a page or an api package — pick another name.")


# --- render scripts/templates/ into the two new folders ---------------------
#
# File *names* carry the tokens too (use__NAME__Socket.ts.template), so a template
# folder maps 1:1 onto the subapp. Every template ends in .template, stripped here.
# The suffix keeps editors and type-checkers off them — a real `.ts` holding
# `__WS_PATH_CONST__` would be a wall of IDE errors on a non-source file. A missing
# suffix is an error, not a no-op, so the convention can't rot into "some of them".


def render(text):
    for token, value in (
        ("__SLUG__", slug),
        ("__PKG__", pkg),
        ("__NAME__", name),
        ("__WS_PATH_CONST__", ws_path_const),
        ("__API_PATH_CONST__", api_path_const),
        ("__LABEL__", label),
    ):
        text = text.replace(token, value)
    return text


sources = [
    (Path("scripts/templates/server-python"), api_dir),
    (Path("scripts/templates/client-web"), page_dir),
]

# Validate every template before writing anything. Bailing out mid-render would
# leave a half-generated subapp with the registries unpatched — worse than the
# name collision above, which is caught before the first mkdir.
for templates, _ in sources:
    if not templates.is_dir():
        die(f"Missing {templates}/ — the templates live beside this script.")
    for template in sorted(templates.iterdir()):
        if not template.name.endswith(".template"):
            die(f"{template} must be named <filename>.template — see the note above.")

for templates, dest_dir in sources:
    dest_dir.mkdir(parents=True)
    for template in sorted(templates.iterdir()):
        dest = dest_dir / render(template.name).removesuffix(".template")
        dest.write_text(render(template.read_text()))
        print(f"  new      {dest}")


# --- the registries ---------------------------------------------------------
#
# Anchored on a pattern, not a line number, and loud if the anchor is gone — a
# silently skipped edit would leave a subapp reachable from nowhere. Each entry
# goes last in its list.


patched = []


def edit(path, pattern, addition, before=False):
    text = path.read_text()
    found = list(re.finditer(pattern, text, re.M))
    if not found:
        die(f"Could not find {pattern!r} in {path} — add the entry by hand.")
    at = found[0].start() if before else found[-1].end()
    path.write_text(text[:at] + addition + text[at:])
    if path not in patched:
        patched.append(path)


server_py = PY_SRC / "server.py"
edit(server_py, r"^import [a-z_][a-z0-9_]*\n", f"import {pkg}\n")
# The description is /docs' group heading for this app; the label is a best guess,
# worth replacing with a real sentence.
edit(
    server_py,
    r"^\]\n",
    f'    AppEntry(\n'
    f'        "{slug}",\n'
    f'        {pkg},\n'
    f'        "{label}.",\n'
    f'    ),\n',
    before=True,
)

# Two separate blocks in that file, each anchored on its own pattern — the
# entry goes after the last line of its own block, not at the end of the other's.
servers_ts = WEB / "lib" / "servers.ts"
edit(
    servers_ts,
    r"^export const \w+_WS_PATH = .*\n",
    f"export const {ws_path_const} = '/api/{slug}/ws'\n",
)
edit(
    servers_ts,
    r"^export const \w+_API_PATH = .*\n",
    f"export const {api_path_const} = '/api/{slug}'\n",
)

app_tsx = WEB / "App.tsx"
edit(
    app_tsx,
    r"^import .*@/pages/.*\n",
    f"import {{ {name}Page }} from '@/pages/{slug}/{name}Page'\n",
)
# Above the catch-all route: below it, the new route would never match.
edit(
    app_tsx,
    r'^ *<Route path="\*".*\n',
    f'          <Route path="{slug}" element={{<{name}Page />}} />\n',
    before=True,
)

edit(
    WEB / "components" / "AppLayout.tsx",
    r"^\]\n",
    f"  {{ to: '/{slug}', label: '{label}', end: false }},\n",
    before=True,
)

for path in patched:
    print(f"  patched  {path}")

print(f"\n\033[1m{label}: page /{slug}, socket /api/{slug}/ws, "
      f"commands POST /api/{slug}/count/…\033[0m")
print("  the api contract is regenerated next — commit doc/api/openapi.json and")
print("  app/client-web/src/api/schema.ts along with the new subapp.")
PY

# Regenerate the contract BEFORE the checks: the new page imports its wire type
# from the TypeScript generated off the new package's WS_MESSAGE, so until this
# runs the subapp does not type-check. Runs even under NO_CHECK=1 — skipping the
# checks saves time, a stale contract just leaves a broken tree.
scripts/generate-api-contract.sh

[ "${NO_CHECK:-0}" = 1 ] || scripts/error_check.sh

cat <<'EOF'

Restart ./scripts/start-local-hotloaded-pswamp-server.sh — a new Python package
needs the rebuild, not just compose watch — and the page is in the nav.
EOF
