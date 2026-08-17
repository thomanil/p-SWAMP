#!/usr/bin/env bash
# Auto-fix the lint and formatting issues that scripts/error_check.sh reports —
# the write counterpart to that read-only checker. Run this to clean up, then
# run error_check.sh to confirm what's left (type errors and unsafe lint hits
# that can't be fixed mechanically still need a human).
#
# It edits files in place, so review the diff afterwards. Same scope as the
# checker:
#
#   Web client (app/client-web, TypeScript/React)
#     - eslint . --fix .... apply ESLint's auto-fixable rules.
#
#   Python (app/server-python)
#     - ruff check --fix .. apply ruff's safe lint fixes (unsafe fixes are left
#                           for you; error_check.sh will still flag them).
#     - ruff format ....... rewrite files to ruff's format.
#
# Note: there is no type-check fixer here — tsc errors are reported by
# error_check.sh and fixed by hand.
set -uo pipefail

# Run from the repo root regardless of where the script is invoked from.
cd "$(dirname "$0")/.." || exit 1

# GUI git frontends / IDEs launch with a minimal PATH that omits user tool dirs —
# ~/.local/bin (uv) and nvm's node dir. Re-add the usual spots so this script
# works no matter who invokes it (mirrors error_check.sh).
prepend_path() { case ":$PATH:" in *":$1:"*) ;; *) [ -d "$1" ] && PATH="$1:$PATH";; esac; }
prepend_path "$HOME/.local/bin"                  # uv
if ! command -v npx >/dev/null 2>&1; then         # node via nvm: newest installed
  newest_node_bin="$(ls -d "$HOME"/.nvm/versions/node/*/bin 2>/dev/null | sort -V | tail -1)"
  [ -n "$newest_node_bin" ] && prepend_path "$newest_node_bin"
fi
export PATH

section() { printf '\n\033[1m==> %s\033[0m\n' "$1"; }

# --- Web client: ESLint --fix -----------------------------------------------
section "Web client (app/client-web)"
(
  cd app/client-web || exit 1
  # A fresh checkout has no node_modules — install from the lockfile so eslint
  # doesn't fail on missing deps (mirrors start-local-hotloaded-pswamp-web-client.sh).
  if [ ! -d node_modules ]; then
    echo "Installing web client dependencies (first run)…"
    npm ci
  fi
)
( cd app/client-web && npx --no-install eslint . --fix )

# --- Python: ruff --fix + ruff format ---------------------------------------
section "Python (app/server-python)"

# Every .py under app/, excluding caches.
if find app -name '*.py' -not -path '*/__pycache__/*' -print -quit | grep -q .; then
  # Same locked ruff as error_check.sh — pinned in the `dev` dependency group of
  # app/server-python/pyproject.toml, so the writer and the read-only gate can
  # never drift apart and "fix" files into a state the pre-push hook rejects.
  # Same explicit --select E,F too, so this only fixes what that gate actually
  # complains about (see the note there).
  RUFF=(uv run --project app/server-python --only-group dev ruff)
  "${RUFF[@]}" check --select E,F --fix app
  "${RUFF[@]}" format app
else
  echo "  (no Python files found)"
fi

section "Done"
printf 'Auto-fixes applied. Review the diff, then run scripts/error_check.sh to verify.\n'
