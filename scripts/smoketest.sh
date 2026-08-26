#!/usr/bin/env bash
# The client-server rig's end-to-end smoke test — "does the whole thing still
# work?", as one command.
#
#   scripts/smoketest.sh                          start a server, test it, stop it
#   SMOKETEST_URL=http://host:port scripts/smoketest.sh   test a server already running
#
# This is the manual test automated. The routine it replaces: start the server,
# start the web client, open the Reference example, click the buttons, check the
# counter landed on the right number. Doing that by hand proves routing, the
# socket, the client id, a POST command and the per-client state all still hold —
# and it proves it about five minutes after you wanted to know.
#
# It is deliberately the Reference example and not a p-SWAMP page: that app exists
# to be the stable, boring path through every part of the stack (see AGENTS.md),
# so a failure here is a broken rig rather than a changed feature.
#
# Complements scripts/error_check.sh rather than overlapping it: that one is
# static and never starts the app, this one only starts the app. Run both.
#
# LATER: this is the cheap 80%, not the finished job. It exercises the WIRE — the
# api, the socket, the state — and would not notice a button that stopped calling
# its command, a panel that renders nothing, or a route that 404s in the browser.
# Closing that gap means driving a real browser, and Playwright is the intended
# tool: `npx playwright test` against this same container, one spec per page,
# starting with the very click-through this script stands in for. Doing it now
# would mean a browser download and a second test framework before the first
# assertion; doing it later is a strict addition — this script stays valid as the
# fast, dependency-free layer underneath it.
#
# What it checks, in order:
#   1. /healthz answers                     — the process is serving
#   2. / serves the built web client        — the client is baked into the image
#   3. a deep link serves the shell too     — SPAStaticFiles' history fallback
#   4. a missing asset still 404s           — that fallback isn't swallowing everything
#   5. the api describes itself             — /openapi.json has the commands in it
#   6. the counter flow                     — POST commands in, state down the socket
#
# Steps 1-5 are curl; step 6 is tools/smoketest_reference_subapp.py, because a
# WebSocket is the one thing bash cannot speak. That helper needs no new
# dependency — `websockets` is already in the server's locked environment via
# uvicorn[standard].
#
# Every step runs even if an earlier one fails, so one run reports everything
# that is broken. Exits non-zero if any step failed.
set -uo pipefail

# Run from the repo root regardless of where the script is invoked from.
cd "$(dirname "$0")/.." || exit 1

# Same PATH repair as error_check.sh: a GUI git frontend or a CI shell may not
# have ~/.local/bin (uv) on PATH.
prepend_path() { case ":$PATH:" in *":$1:"*) ;; *) [ -d "$1" ] && PATH="$1:$PATH";; esac; }
prepend_path "$HOME/.local/bin"
export PATH

BASE_URL="${SMOKETEST_URL:-http://127.0.0.1:8000}"
# Only true when this script brought the stack up, so it only ever tears down
# what it started — a developer's own running server is left alone.
STARTED_STACK=0

FAILURES=()
section() { printf '\n\033[1m==> %s\033[0m\n' "$1"; }
pass() { printf '    \033[32m✓\033[0m %s\n' "$1"; }
fail() { printf '    \033[31m✗\033[0m %s\n' "$1"; FAILURES+=("$1"); }

# check <label> <cmd...> — run a step, remember it if it fails.
check() {
  local label="$1"; shift
  if "$@" >/dev/null 2>&1; then pass "$label"; else fail "$label"; fi
}

cleanup() {
  if [ "$STARTED_STACK" -eq 1 ]; then
    printf '\nStopping the server…\n'
    docker compose down --remove-orphans >/dev/null 2>&1
  fi
}

# Poll until the server serves. Unconditional, not just on the path that starts
# the stack: with SMOKETEST_URL the caller may well have launched the server a
# moment ago (CI does exactly that), and without this the first check would race
# it and fail on a server that was merely still booting.
#
# --connect-timeout, not just -m: an address with no route drops packets rather
# than refusing, so an attempt otherwise runs to its full timeout and a dead host
# sits silent for the whole budget. Prints a dot per attempt for the same reason.
wait_for_healthz() {
  local attempts=30
  for ((i = 1; i <= attempts; i++)); do
    if curl -fsS --connect-timeout 1 -m 3 "$BASE_URL/healthz" >/dev/null 2>&1; then
      [ "$i" -gt 1 ] && printf '\n'
      pass "GET /healthz answers"
      return 0
    fi
    printf '.'
    sleep 1
  done
  printf '\n'
  fail "GET /healthz answers (no response after ${attempts}s)"
  return 1
}

# --- Preflight ---------------------------------------------------------------
missing=()
needed=(curl uv)
[ -n "${SMOKETEST_URL:-}" ] || needed+=(docker)
for tool in "${needed[@]}"; do
  command -v "$tool" >/dev/null 2>&1 || missing+=("$tool")
done
if [ "${#missing[@]}" -ne 0 ]; then
  printf '\033[31msmoketest: required tool(s) not found on PATH: %s\033[0m\n' "${missing[*]}"
  exit 1
fi

# --- Get a server to test ----------------------------------------------------
#
# The container, not `uv run src/server.py`: the image is what ships, and it is
# the only one that serves the built web client, which half the checks below are
# about.
section "Server under test"
if [ -n "${SMOKETEST_URL:-}" ]; then
  echo "    Using the server at $SMOKETEST_URL (not managing its lifecycle)"
elif [ -n "$(docker compose ps --status running -q server 2>/dev/null)" ]; then
  echo "    Reusing the compose server already running at $BASE_URL"
else
  echo "    Starting the compose server (this builds the image on a cold run)…"
  # --wait blocks until the healthcheck in docker-compose.yml passes, so there is
  # no polling loop here. --build for the reason the dev script gives: without it
  # Compose happily runs a stale image.
  if ! docker compose up -d --build --wait; then
    printf '\033[31msmoketest: the server did not come up\033[0m\n'
    docker compose logs --tail 50 server
    docker compose down --remove-orphans >/dev/null 2>&1
    exit 1
  fi
  STARTED_STACK=1
  trap cleanup EXIT INT TERM
  echo "    Server up at $BASE_URL"
fi

# --- 1-5: the HTTP surface ---------------------------------------------------
section "HTTP surface"

wait_for_healthz

# Small predicates rather than `bash -c "…"` strings: the quoting in a nested
# shell is where this kind of script usually rots.
body_matches() { curl -fsS -m 5 "$BASE_URL$1" | grep -q "$2"; }
status_is() {
  [ "$(curl -s -o /dev/null -w '%{http_code}' --connect-timeout 2 -m 5 "$BASE_URL$1")" = "$2" ]
}

# The shell, served from the same origin as /api. `id="root"` is the mount point
# in app/client-web/index.html, and an /assets/ reference proves this is the BUILT
# client rather than the dev shell, which points at /src/main.tsx instead.
serves_built_client() {
  local body
  body="$(curl -fsS -m 5 "$BASE_URL$1")" || return 1
  grep -q 'id="root"' <<<"$body" && grep -q '/assets/' <<<"$body"
}
check "GET / serves the built web client" serves_built_client "/"

# A hard refresh on a page route has to work, which is SPAStaticFiles returning
# index.html for a 404 outside assets/ and api/.
check "GET /reference-subapp serves the shell (deep link)" \
  body_matches "/reference-subapp" 'id="root"'

# …and that fallback must not answer for everything, or a wrong asset or endpoint
# URL would silently return HTML instead of failing loudly.
check "GET /assets/does-not-exist.js still 404s" \
  status_is "/assets/does-not-exist.js" 404

# The api describes itself, and the Reference example's commands are in it. Not a
# staleness check — error_check.sh owns that — just proof the document is served.
check "GET /openapi.json describes the Reference example's commands" \
  body_matches "/openapi.json" 'reference_subapp_bump'

# --- 6: the counter flow, over the socket ------------------------------------
section "Reference example (commands up, state down)"
uv run --project app/server-python \
  python app/server-python/tools/smoketest_reference_subapp.py "$BASE_URL" \
  || FAILURES+=("counter flow")

# --- Report ------------------------------------------------------------------
if [ "${#FAILURES[@]}" -ne 0 ]; then
  printf '\n\033[31mSmoke test FAILED (%d):\033[0m\n' "${#FAILURES[@]}"
  for f in "${FAILURES[@]}"; do printf '  - %s\n' "$f"; done
  if [ "$STARTED_STACK" -eq 1 ]; then
    printf '\nLast 50 lines of server log:\n'
    docker compose logs --tail 50 server
  fi
  exit 1
fi

printf '\n\033[32mSmoke test passed — the rig works end to end.\033[0m\n'
