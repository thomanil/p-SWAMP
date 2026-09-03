#!/usr/bin/env bash
# Generate the published api contract and the TypeScript the client reads it
# through. Two committed artifacts:
#
#   doc/api/openapi.json               the contract (for humans and codegen)
#   app/client-web/src/api/schema.ts   TypeScript types, generated from it
#
# Both come from the server's own document (tools/dump_openapi.py), so the
# committed copy cannot disagree with what the server serves.
#
#   generate-api-contract.sh            regenerate both, in place
#   generate-api-contract.sh --check    read-only: fail if either is stale
#
# --check is what error_check.sh runs — it makes a changed endpoint a failing
# check rather than a surprise in someone's browser. Commit both files with the
# change. Needs the FULL server env (it imports the app for its document), but
# starts no server and binds no port.
set -uo pipefail

# Run from the repo root regardless of where the script is invoked from.
cd "$(dirname "$0")/.." || exit 1

SPEC="doc/api/openapi.json"
TYPES="app/client-web/src/api/schema.ts"

CHECK=0
if [ "${1:-}" = "--check" ]; then
  CHECK=1
elif [ $# -ne 0 ]; then
  echo "usage: $0 [--check]" >&2
  exit 2
fi

# Same PATH repair as error_check.sh (minimal PATH under GUI git frontends).
prepend_path() { case ":$PATH:" in *":$1:"*) ;; *) [ -d "$1" ] && PATH="$1:$PATH";; esac; }
prepend_path "$HOME/.local/bin"
if ! command -v npx >/dev/null 2>&1; then
  newest_node_bin="$(ls -d "$HOME"/.nvm/versions/node/*/bin 2>/dev/null | sort -V | tail -1)"
  [ -n "$newest_node_bin" ] && prepend_path "$newest_node_bin"
fi
export PATH

missing=()
for tool in npx uv; do
  command -v "$tool" >/dev/null 2>&1 || missing+=("$tool")
done
if [ "${#missing[@]}" -ne 0 ]; then
  printf '\033[31mgenerate-api-contract: required tool(s) not found on PATH: %s\033[0m\n' "${missing[*]}"
  exit 1
fi

# The generator is a devDependency, so `npx --no-install` uses the pinned version
# rather than fetching latest. A partial install can leave node_modules present
# but without the generator (common in dev containers), so check for the binary.
if [ ! -x app/client-web/node_modules/.bin/openapi-typescript ]; then
  echo "Installing web client dependencies (generator unavailable)…"
  (cd app/client-web && npm ci) || exit 1
fi

# Where to write. In --check mode that is a scratch dir we diff against the
# committed files; a trap cleans it up on every exit path.
if [ "$CHECK" -eq 1 ]; then
  TMP="$(mktemp -d)" || exit 1
  trap 'rm -rf "$TMP"' EXIT INT TERM
  OUT_SPEC="$TMP/openapi.json"
  OUT_TYPES="$TMP/schema.ts"
else
  OUT_SPEC="$SPEC"
  OUT_TYPES="$TYPES"
fi

uv run --project app/server-python python app/server-python/tools/dump_openapi.py "$OUT_SPEC" || exit 1

# openapi-typescript emits types only -- no runtime, no `enum` (tsconfig.app.json
# sets erasableSyntaxOnly). Run from app/client-web so npx --no-install resolves
# the binary there (and errors on a miss rather than downloading); hence the
# absolute paths.
ABS_SPEC="$(cd "$(dirname "$OUT_SPEC")" && pwd)/$(basename "$OUT_SPEC")"
mkdir -p "$(dirname "$OUT_TYPES")" || exit 1
ABS_TYPES="$(cd "$(dirname "$OUT_TYPES")" && pwd)/$(basename "$OUT_TYPES")"
(cd app/client-web && npx --no-install openapi-typescript "$ABS_SPEC" -o "$ABS_TYPES") >/dev/null || exit 1

if [ "$CHECK" -eq 0 ]; then
  if git diff --quiet -- "$SPEC" "$TYPES"; then
    echo "No changes in api contract -> no changes in clientside schemas"
    exit 0
  fi
  echo "Updated api contract json and clientside schema to match python source."
  printf '\ncurrent diff:\n'
  git --no-pager diff --stat -- "$SPEC" "$TYPES"
  exit 0
fi

# --- --check: report every stale artifact, not just the first ----------------
stale=0
for pair in "$SPEC:$OUT_SPEC" "$TYPES:$OUT_TYPES"; do
  committed="${pair%%:*}"
  fresh="${pair##*:}"
  if [ ! -f "$committed" ]; then
    printf '\033[31m  %s is missing\033[0m\n' "$committed"
    stale=1
    continue
  fi
  if ! diff -u "$committed" "$fresh" >/dev/null 2>&1; then
    printf '\033[31m  %s is out of date:\033[0m\n' "$committed"
    diff -u "$committed" "$fresh" | head -40
    stale=1
  fi
done

if [ "$stale" -ne 0 ]; then
  printf '\nThe api contract does not match the code. Regenerate and commit it:\n'
  printf '  scripts/generate-api-contract.sh\n'
  exit 1
fi
exit 0
