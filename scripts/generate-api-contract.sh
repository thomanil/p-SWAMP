#!/usr/bin/env bash
# Generate the published api contract, and the TypeScript the web client reads it
# through. Two artifacts, both committed:
#
#   doc/api/openapi.json                 the contract itself, for humans and for
#                                        any team's own code generator
#   app/client-web/src/api/schema.ts     TypeScript types, generated from it
#
# Both are produced from the running server's own document (see
# app/server-python/tools/dump_openapi.py), so there is exactly one description
# of this api and the committed copy cannot disagree with what the server serves.
#
#   scripts/generate-api-contract.sh            regenerate both, in place
#   scripts/generate-api-contract.sh --check    read-only: fail if either is stale
#
# --check is what scripts/error_check.sh runs, which is what makes a changed
# endpoint a failing quality check rather than a runtime surprise in someone
# else's browser. Regenerate and commit both files alongside the change.
#
# Unlike the rest of error_check.sh this needs the FULL server environment, not
# just the pinned linter -- it imports the app to ask it for its document. It
# still starts no server and binds no port.
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

# Same PATH repair as error_check.sh: GUI git frontends launch hooks with a
# minimal PATH that omits ~/.local/bin (uv) and nvm's node dir.
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

# The web client's devDependencies carry the generator, so `npx --no-install`
# resolves the version pinned in package-lock.json rather than fetching whatever
# is newest. A partial install can leave node_modules present without the
# generator, which is common in development containers.
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

# openapi-typescript emits types only -- no runtime, and no `enum`, which matters
# because tsconfig.app.json sets erasableSyntaxOnly.
#
# Run from app/client-web, because npx resolves a binary from the CURRENT
# directory's node_modules and --no-install makes a miss an error rather than a
# silent download (same reason error_check.sh cds before calling npx). Both paths
# are therefore made absolute first.
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
