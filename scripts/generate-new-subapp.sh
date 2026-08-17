#!/usr/bin/env bash
# Scaffold a new subapp — a page and its api — wired into the nav, the route
# table, the ws path table and the backend APPS registry, i.e. every step in
# README.md's "To add a page" / "To add an api".
#
#   scripts/generate-new-subapp.sh grid-overview "Grid Overview"
#
# What comes out already works: a nav entry, a page at /grid-overview, and a
# WebSocket to a new backend package holding a per-client counter you can bump
# and reset. Replacing that counter is then the only work left.
#
# The generated files come from scripts/templates/ — edit them, not this script,
# to change what a new subapp starts life as.
#
# NO_CHECK=1 skips the scripts/error_check.sh run at the end.
set -euo pipefail

# Run from the repo root regardless of where the script is invoked from.
cd "$(dirname "$0")/.."

if [ $# -ne 2 ]; then
  echo 'usage: scripts/generate-new-subapp.sh <url-name> "<Nav Label>"' >&2
  echo '   eg: scripts/generate-new-subapp.sh grid-overview "Grid Overview"' >&2
  exit 1
fi

# python3 does the work: it derives the other spellings of the name, renders the
# templates and inserts into the registries by anchor rather than by line number.
# It is already a hard requirement of scripts/error_check.sh, so nothing new.
SLUG="$1" LABEL="$2" python3 - <<'PY'
import keyword
import os
import re
import sys
from pathlib import Path


def die(msg):
    print(f"\033[31m{msg}\033[0m", file=sys.stderr)
    sys.exit(1)


# The name shows up in four shapes, and keeping them in step by hand is the
# error-prone part this script exists to remove:
#
#   slug           grid-overview           the URL, the page folder, the /api prefix
#   pkg            grid_overview           the Python package (has to be an identifier)
#   name           GridOverview            the React component, the hook, the model class
#   ws_path_const  GRID_OVERVIEW_WS_PATH   the ws path const in lib/servers.ts
#
# The templates in scripts/templates/ spell these __SLUG__, __PKG__, __NAME__,
# __WS_PATH_CONST__ and __LABEL__.

slug = os.environ["SLUG"]
label = os.environ["LABEL"]
pkg = slug.replace("-", "_")
name = "".join(word.capitalize() for word in slug.split("-"))
ws_path_const = f"{pkg.upper()}_WS_PATH"

# The 32-char cap keeps the rendered Python inside ruff's 88-column line limit,
# which error_check.sh enforces on it moments later.
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
# File *names* carry the tokens too (use__NAME__Socket.ts), so a template folder
# maps 1:1 onto what the subapp gets.


def render(text):
    for token, value in (
        ("__SLUG__", slug),
        ("__PKG__", pkg),
        ("__NAME__", name),
        ("__WS_PATH_CONST__", ws_path_const),
        ("__LABEL__", label),
    ):
        text = text.replace(token, value)
    return text


for templates, dest_dir in (
    (Path("scripts/templates/server-python"), api_dir),
    (Path("scripts/templates/client-web"), page_dir),
):
    if not templates.is_dir():
        die(f"Missing {templates}/ — the templates live beside this script.")
    dest_dir.mkdir(parents=True)
    for template in sorted(templates.iterdir()):
        dest = dest_dir / render(template.name)
        dest.write_text(render(template.read_text()))
        print(f"  new      {dest}")


# --- the registries ---------------------------------------------------------
#
# Anchored on a pattern rather than a line number, and loud if the anchor is gone
# — a silently skipped edit would leave a subapp reachable from nowhere. Every
# entry goes last in its list.


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
edit(server_py, r"^\]\n", f'    ("/api/{slug}", {pkg}),\n', before=True)

edit(
    WEB / "lib" / "servers.ts",
    r"^export const \w+_WS_PATH = .*\n",
    f"export const {ws_path_const} = '/api/{slug}/ws'\n",
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
    f"  {{ to: '/{slug}', label: '{label}', isIndex: false }},\n",
    before=True,
)

for path in patched:
    print(f"  patched  {path}")

print(f"\n\033[1m{label}: page /{slug}, api /api/{slug}/ws\033[0m")
PY

[ "${NO_CHECK:-0}" = 1 ] || scripts/error_check.sh

cat <<'EOF'

Restart ./scripts/start-local-hotloaded-pswamp-server.sh — a new Python package
needs the rebuild, not just compose watch — and the page is in the nav.
EOF
