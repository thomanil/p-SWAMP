#!/usr/bin/env bash
# Static "does it even hold together?" check for the whole repo — run this before
# committing or pushing. It is the stable entrypoint for "is the code sound?":
# the checks below may change, but callers keep running one command.
#
# This is the single definition of "is the code sound?" in the repo: the
# .githooks/pre-push hook runs it, and so does CI
# (.github/workflows/build-and-publish-image.yml calls this script rather than
# reimplementing the checks in YAML, so local and server can't drift apart).
# Locally it is the fast gate — catch it here rather than in a pipeline.
#
# This script is read-only: it reports problems and never edits your files. To
# auto-fix the fixable lint/formatting issues it surfaces, run the sibling
# scripts/autofix_lint_formatting.sh.
#
# It never starts the app or hits the network for app state; it only reads the
# source. Two halves:
#
#   Web client (app/client-web, TypeScript/React)
#     - tsc -b ........ full type-check (project refs, noEmit) — catches type
#                       errors, unused locals/params, bad imports.
#     - eslint . ...... lint (the repo's flat eslint.config.js, react-hooks etc).
#
#   Python (app/server-python)
#     - uv lock --check  pyproject.toml vs uv.lock (dependency manifest and
#                       lockfile in sync — read-only, never rewrites the lock).
#     - py_compile .... parse/compile every module → hard syntax/AST errors.
#     - ruff check .... lint + error analysis (pyflakes F-rules catch undefined
#                       names, unused imports/vars; pycodestyle E-rules catch
#                       style errors). Run from the locked `dev` dependency
#                       group, so nothing to pre-install.
#     - ruff format ... formatting drift (check-only, never rewrites here).
#
# All checks run even if an earlier one fails, so you see every problem in one
# pass; the script exits non-zero if any check failed.
set -uo pipefail

# Run from the repo root regardless of where the script is invoked from.
cd "$(dirname "$0")/.." || exit 1

# GUI git frontends (magit/Emacs, IDEs) launch hooks with a minimal PATH that
# omits user tool dirs — ~/.local/bin (uv) and nvm's node dir — so a push
# that's fine from a terminal fails here with "uv/npx: not found". Re-add the
# usual spots so this script works no matter who invokes it.
prepend_path() { case ":$PATH:" in *":$1:"*) ;; *) [ -d "$1" ] && PATH="$1:$PATH";; esac; }
prepend_path "$HOME/.local/bin"                  # uv
if ! command -v npx >/dev/null 2>&1; then         # node via nvm: newest installed
  newest_node_bin="$(ls -d "$HOME"/.nvm/versions/node/*/bin 2>/dev/null | sort -V | tail -1)"
  [ -n "$newest_node_bin" ] && prepend_path "$newest_node_bin"
fi
export PATH

# Fail early with a clear message if a required tool still isn't reachable,
# rather than deep inside a check with a cryptic "command not found".
missing=()
for tool in node npx uv python3; do
  command -v "$tool" >/dev/null 2>&1 || missing+=("$tool")
done
if [ "${#missing[@]}" -ne 0 ]; then
  printf '\033[31merror_check: required tool(s) not found on PATH: %s\033[0m\n' "${missing[*]}"
  printf 'PATH=%s\n' "$PATH"
  exit 1
fi

# Collect the names of failed checks so we can report them all at the end.
FAILURES=()
section() { printf '\n\033[1m==> %s\033[0m\n' "$1"; }
run() {
  # run <label> <cmd...> — execute a check, remember it if it fails.
  local label="$1"; shift
  if "$@"; then
    printf '\033[32m  ✓ %s\033[0m\n' "$label"
  else
    printf '\033[31m  ✗ %s\033[0m\n' "$label"
    FAILURES+=("$label")
  fi
}

# --- Web client: TypeScript + ESLint ----------------------------------------
section "Web client (app/client-web)"
(
  cd app/client-web || exit 1
  # A fresh checkout has no node_modules — install from the lockfile so tsc/eslint
  # don't fail on missing deps (mirrors start-local-hotloaded-pswamp-web-client.sh).
  if [ ! -d node_modules ]; then
    echo "Installing web client dependencies (first run)…"
    npm ci
  fi
)
run "tsc (web type-check)" bash -c 'cd app/client-web && npx --no-install tsc -b'
run "eslint (web lint)"    bash -c 'cd app/client-web && npx --no-install eslint .'

# --- Python: lockfile + AST/compile + ruff lint + ruff format ---------------
section "Python (app/server-python)"

# Dependency manifest vs lockfile — the Python counterpart of what `npm ci`
# enforces on the web side. uv.lock must still match pyproject.toml, so nobody
# can add a dependency, forget to re-lock, and only discover it when the Docker
# build's `uv export --locked` fails. `--check` never writes, so this
# script stays read-only; `--offline` keeps the pre-push hook off the network.
# Fix a failure with: (cd app/server-python && uv lock)
if [ -f app/server-python/pyproject.toml ]; then
  run "uv lock --check (deps vs lockfile)" \
    uv lock --check --offline --project app/server-python
fi

# Every .py under app/, excluding caches. NUL-delimited to survive odd paths.
mapfile -d '' -t PY_FILES < <(find app -name '*.py' -not -path '*/__pycache__/*' -print0)

if [ "${#PY_FILES[@]}" -eq 0 ]; then
  echo "  (no Python files found)"
else
  # AST/syntax: compile each module. Third-party deps aren't needed just to parse.
  run "py_compile (Python syntax/AST)" python3 -m py_compile "${PY_FILES[@]}"

  # Lint + error analysis. ruff's version is pinned in the `dev` dependency group
  # of app/server-python/pyproject.toml and locked in uv.lock, so the pre-push
  # hook and every dev run the exact same linter — a floating `uvx ruff` could
  # fail a push on code nobody touched. `--project` points uv at that manifest
  # without changing directory, so the `app` path below still resolves from the
  # repo root; `--only-group dev` installs just ruff, not fastapi/uvicorn.
  # E + F is pycodestyle errors + pyflakes — i.e. real lint and error analysis,
  # which is all this gate is meant to be. Selected EXPLICITLY rather than relying
  # on ruff's defaults: as of ruff 0.16 those also pull in opinionated style
  # families (I, B, S, BLE, ASYNC…) that flag deliberate code — e.g. the blind
  # `except` around a dying WebSocket send — and would fail the build on taste
  # rather than on bugs. Add a family here on purpose if you want it, don't
  # inherit one.
  RUFF=(uv run --project app/server-python --only-group dev ruff)
  run "ruff check (Python lint)"   "${RUFF[@]}" check --select E,F app
  run "ruff format (Python style)" "${RUFF[@]}" format --check app
fi

# --- Summary ----------------------------------------------------------------
section "Summary"
if [ "${#FAILURES[@]}" -eq 0 ]; then
  printf '\033[32mAll checks passed.\033[0m\n'
  exit 0
fi
printf '\033[31m%d check(s) failed:\033[0m\n' "${#FAILURES[@]}"
printf '  - %s\n' "${FAILURES[@]}"
printf '\nTo auto-fix the fixable lint/formatting issues, run:\n'
printf '  scripts/autofix_lint_formatting.sh\n'
exit 1
