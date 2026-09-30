#!/usr/bin/env bash
# Prove both generators still produce working apps -- without touching this
# working tree.
#
#   ./scripts/check-generators.sh
#
# 1. Snapshot the working tree (committed or not, templates included) into a
#    throwaway git worktree.
# 2. There, run generate-new-subapp.sh (a counter) and
#    generate-new-module-with-frontend.sh (a module and its page).
# 3. error_check.sh over the result, the generated module's tests, and an import
#    of the module-worker's families list as patched in docker-compose.yml and
#    k8s/p-swamp-local.yaml.
# 4. Boot the server in one process (in-memory transport, so the server hosts the
#    module), then tools/smoketest_generated.py: a bump comes back on the
#    counter's socket, and a module result comes down the module app's socket.
#
# Not covered: the Kafka path (no broker here); the worker patch is checked by
# importing it. e2e-smoke-test.sh runs this as its last step, and so CI does.
# CHECK_GENERATORS_PORT moves the server off 8765.
set -euo pipefail

# Run from the repo root regardless of where the script is invoked from.
cd "$(dirname "$0")/.."
REPO="$(pwd -P)"

# Same PATH repair as error_check.sh (minimal PATH under GUI git frontends).
prepend_path() { case ":$PATH:" in *":$1:"*) ;; *) [ -d "$1" ] && PATH="$1:$PATH";; esac; }
prepend_path "$HOME/.local/bin"
export PATH

section() { printf '\n\033[1m==> %s\033[0m\n' "$1"; }
die() { printf '\033[31mcheck-generators: %s\033[0m\n' "$1" >&2; exit 1; }

for tool in git uv npx curl tar; do
  command -v "$tool" >/dev/null 2>&1 || die "$tool not found on PATH"
done

COUNTER=check-counter
MODULE=check-module
PORT="${CHECK_GENERATORS_PORT:-8765}"
BASE="http://127.0.0.1:$PORT"

if curl -fsS -o /dev/null "$BASE/healthz" 2>/dev/null; then
  die "something already answers on port $PORT; set CHECK_GENERATORS_PORT"
fi

WORK="$(mktemp -d "${TMPDIR:-/tmp}/pswamp-generators.XXXXXX")"
TREE="$WORK/tree"
SERVER_PID=""

cleanup() {
  status=$?
  if [ -n "$SERVER_PID" ]; then
    kill "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
  fi
  # The symlink first, so removing the worktree never reaches the real node_modules.
  if [ -L "$TREE/app/client-web/node_modules" ]; then rm "$TREE/app/client-web/node_modules"; fi
  git -C "$REPO" worktree remove --force "$TREE" >/dev/null 2>&1 || true
  git -C "$REPO" worktree prune
  rm -rf "$WORK"
  exit "$status"
}
trap cleanup EXIT INT TERM

section "Snapshot this working tree into $TREE"
# A commit object of the tracked files as they are now; nothing is stashed. It
# needs an identity, which a CI checkout lacks; the object is never referenced.
rev="$(GIT_AUTHOR_NAME=check-generators GIT_AUTHOR_EMAIL=check@localhost \
  GIT_COMMITTER_NAME=check-generators GIT_COMMITTER_EMAIL=check@localhost git stash create)"
git worktree add --detach --quiet "$TREE" "${rev:-HEAD}"
# Untracked files that are not ignored (a new template, say) come along too.
git ls-files -z --others --exclude-standard > "$WORK/untracked"
if [ -s "$WORK/untracked" ]; then
  tar --null -T "$WORK/untracked" -cf - | tar -xf - -C "$TREE"
fi
# Reuse this checkout's node_modules; without one, error_check.sh installs it there.
if [ -d app/client-web/node_modules ]; then
  ln -s "$REPO/app/client-web/node_modules" "$TREE/app/client-web/node_modules"
fi

cd "$TREE"

section "Generate a counter subapp and a module app"
NO_CHECK=1 scripts/generate-new-subapp.sh "$COUNTER" "Check counter"
NO_CHECK=1 scripts/generate-new-module-with-frontend.sh "$MODULE" "Check module"

section "Static checks over the generated tree"
scripts/error_check.sh

section "The generated module's tests"
scripts/run-python-server-tests.sh -k "${MODULE//-/_}"

section "The module-worker's families import, as patched"
# The same anchors the generator patches by: the module-worker's own list.
(cd app/server-python/src && MODULE_APP="$MODULE" uv run --project .. python - <<'PY'
import os
import re
from pathlib import Path

from pswamp_core.worker import load_families

for path, pattern in (
    ("docker-compose.yml", r'^  module-worker:\n(?:.*\n)*?\s*PSWAMP_WORKER_FAMILIES: "?([^"\n]*)'),
    ("k8s/p-swamp-local.yaml",
     r'name: p-swamp-module-worker\n(?:.*\n)*?\s*- name: PSWAMP_WORKER_FAMILIES\n\s*value: "?([^"\n]*)'),
):
    found = re.search(pattern, (Path("../../..") / path).read_text(), re.M)
    assert found, f"{path}: no module-worker families list"
    apps = [family.app for family in load_families(found.group(1))]
    assert os.environ["MODULE_APP"] in apps, f"{path}: {apps}"
    print(f"    {path}: {', '.join(apps)}")
PY
)

section "Boot the server in one process and drive both apps"
(
  cd app/server-python
  unset PSWAMP_TRANSPORT
  HOST=127.0.0.1 PORT="$PORT" exec .venv/bin/python src/server.py
) > "$WORK/server.log" 2>&1 &
SERVER_PID=$!

for _ in $(seq 1 60); do
  curl -fsS -o /dev/null "$BASE/healthz" 2>/dev/null && break
  if ! kill -0 "$SERVER_PID" 2>/dev/null; then
    cat "$WORK/server.log"
    die "the server exited during startup"
  fi
  sleep 0.5
done
curl -fsS -o /dev/null "$BASE/healthz" || { cat "$WORK/server.log"; die "the server never answered /healthz"; }

if ! uv run --project app/server-python python app/server-python/tools/smoketest_generated.py \
    "$BASE" "$COUNTER" "$MODULE"; then
  printf '\n--- server log ---\n'
  tail -n 60 "$WORK/server.log"
  die "a generated app does not work"
fi

printf '\n\033[32mBoth generators produce working apps.\033[0m\n'
