#!/usr/bin/env bash
# Pull every dependency in the repo forward to the newest version it is allowed
# to have, then hand you the diff to review.
#
# WHY THIS EXISTS
#
#   Staying near the head of your dependencies is the cheapest supply-chain
#   defence there is: a fix that is already published protects nobody until it
#   is in the lockfile, and a repo that upgrades once a year does it in one
#   scary 400-package jump instead of a boring monthly one.
#
# HOW IT IS MEANT TO BE USED
#
#   This script does NOT decide anything. It produces a candidate diff:
#
#     1. run it on a branch,
#     2. read the "What actually moved" report it prints — every version change
#        in all three lockfiles, which is the thing `git diff` is bad at showing
#        (a lockfile diff is ~95% per-wheel sha256 hashes) — then the manifests,
#        and look hard at any major-version jump,
#     3. run the app (server + web client) and click through it,
#     4. open a pull request, so a human other than you sees the same diff.
#
#   It edits four manifests and their three lockfiles in place and touches
#   nothing else. Never run it and commit the result unread: the whole point of
#   the exercise is the reading.
#
# WHAT IT TOUCHES
#
#   Web client (app/client-web)
#     - npm-check-updates -u ... rewrite package.json's ranges to the newest
#                                published versions (this is the step `npm
#                                update` will NOT do — that one stays inside the
#                                existing ^ranges). Run with --peer, so a bump
#                                that no installed package's peerDependencies
#                                would accept is skipped rather than written and
#                                left for `npm install` to choke on. ncu prints
#                                what it held back and which package required it.
#     - npm install ............ re-resolve, rewriting package-lock.json. Falls
#                                back to a from-scratch resolve (lock and
#                                node_modules deleted) if the incremental one
#                                hits ERESOLVE — see the comment at that step.
#
#   Desktop package (repo root: pyproject.toml + uv.lock)
#     - uv lock --upgrade ...... re-resolve the whole transitive closure to the
#                                newest versions the ranges permit.
#
#   Web backend (app/server-python: pyproject.toml + uv.lock)
#     - uv lock --upgrade ...... same, and re-reads the p-swamp path dependency
#                                (--upgrade implies --refresh, which is what a
#                                plain `uv lock` fails to do — see AGENTS.md).
#                                Locked after the root, so it resolves against
#                                the root manifest as just upgraded.
#
# WHAT IT DELIBERATELY DOES NOT DO
#
#   It does not widen a Python version range. `uv lock --upgrade` respects the
#   bounds in pyproject.toml, and uv has no npm-check-updates equivalent, so a
#   dependency pinned like `fastapi>=0.115.6,<0.116` stays on 0.115.x no matter
#   how often you run this. That is a feature: those caps were written on
#   purpose. The report at the end lists exactly which direct dependencies are
#   held back by their own range and what version they could reach — bumping one
#   is a hand edit to pyproject.toml followed by another run of this script.
#
# OPTIONS (environment variables)
#
#   TARGET=latest|minor|patch   how far to move the npm ranges (default: latest,
#                               i.e. including major-version jumps). `minor` is
#                               the useful setting for a routine no-surprises
#                               pass.
#   NO_CHECK=1                  skip the scripts/error_check.sh run at the end.
#   VERBOSE=1                   list transitive version changes in the report
#                               too, not just the direct dependencies.
#
# This script needs the network (it asks npm and PyPI what exists), which makes
# it the one script here that is not reproducible offline.
set -uo pipefail

# Run from the repo root regardless of where the script is invoked from.
cd "$(dirname "$0")/.." || exit 1

TARGET="${TARGET:-latest}"

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

missing=()
for tool in node npm npx uv git python3; do
  command -v "$tool" >/dev/null 2>&1 || missing+=("$tool")
done
if [ "${#missing[@]}" -ne 0 ]; then
  printf '\033[31mupdate-dependencies: required tool(s) not found on PATH: %s\033[0m\n' "${missing[*]}"
  printf 'PATH=%s\n' "$PATH"
  exit 1
fi

# The manifests and lockfiles this script is allowed to rewrite. Also the list
# it diffs at the end — if a run changes anything outside these seven files,
# something is wrong.
MANIFESTS=(
  app/client-web/package.json
  app/client-web/package-lock.json
  pyproject.toml
  uv.lock
  app/server-python/pyproject.toml
  app/server-python/uv.lock
)

# Snapshot the lockfiles before anything touches them, so the report at the end
# can say what actually MOVED. A git diff can't be trusted for that: the locks
# may already have been dirty when this started (see the warning below), and
# HEAD would then be the wrong baseline.
BEFORE_DIR="$(mktemp -d)"
trap 'rm -rf "$BEFORE_DIR"' EXIT INT TERM
snapshot_before() {
  # snapshot_before <path> — stash a copy under a flattened name.
  [ -f "$1" ] && cp "$1" "$BEFORE_DIR/$(echo "$1" | tr / _)"
}
snapshot_before uv.lock
snapshot_before app/server-python/uv.lock
snapshot_before app/client-web/package-lock.json

FAILURES=()
section() { printf '\n\033[1m==> %s\033[0m\n' "$1"; }
step() {
  # step <label> <cmd...> — run one upgrade step, remember it if it fails.
  # Steps are independent (npm can succeed while PyPI is unreachable), so a
  # failure is recorded and the rest still run; you get one report at the end.
  local label="$1"; shift
  if "$@"; then
    printf '\033[32m  ✓ %s\033[0m\n' "$label"
  else
    printf '\033[31m  ✗ %s\033[0m\n' "$label"
    FAILURES+=("$label")
  fi
}

# A dirty manifest before we start means the diff you review afterwards is a
# mix of your edits and the script's, which defeats the purpose. Warn loudly
# rather than refuse — sometimes you deliberately bumped a cap by hand first
# (that is the documented way to get past a version cap) and want this run on
# top of it.
DIRTY="$(git diff --name-only -- "${MANIFESTS[@]}" 2>/dev/null)"
if [ -n "$DIRTY" ]; then
  printf '\033[33mNote: these manifests already have uncommitted changes:\033[0m\n'
  printf '  %s\n' $DIRTY
  printf '\033[33mThe diff below will mix them with this run'"'"'s.\033[0m\n'
fi

# --- Web client: package.json ranges, then the lockfile ---------------------
section "Web client (app/client-web) — target: $TARGET"

# npm-check-updates is the tool that rewrites the RANGES in package.json;
# `npm update` only moves the lockfile within the ranges already there, so on
# its own it can never leave a ^18 behind. Run via npx rather than added as a
# devDependency: it is a maintenance tool, not part of the build, and nothing
# in CI or the Docker image needs it. Pinned to a major so a future ncu release
# can't change this script's behaviour under us; -u writes package.json in place.
#
# --peer is not optional here. Without it ncu happily writes a version that
# nothing else in the tree accepts — the first run of this script bumped
# typescript to ~7.0.2 against typescript-eslint@8's peer range of <6.1.0, and
# every downstream step then failed on one ERESOLVE with no clue which of the
# 22 bumps caused it. With --peer that bump is skipped and reported by name.
# It reads the peerDependencies of what is currently INSTALLED, so it is only as
# current as node_modules: a second run after a big upgrade can sometimes move
# something further, once the package that was holding it back has itself moved.
step "npm-check-updates (rewrite package.json ranges)" \
  bash -c "cd app/client-web && npx --yes npm-check-updates@23 -u --peer --target '$TARGET'"

# Re-resolve against the rewritten ranges. `npm install` (not `npm ci`): ci
# installs the lockfile as-is and would undo everything above.
#
# Two attempts, and the second one is not paranoia. npm resolves incrementally
# against the lockfile it already has, and an existing lock can anchor the tree
# to a version that the newly-bumped ranges cannot be reconciled with — even
# with node_modules deleted. Seen on the first real run of this script:
# @vitejs/plugin-react 6.0.2 -> 6.1.0 died on ERESOLVE over @babel/core, reached
# through a chain of *optional* peer dependencies, while the very same
# package.json resolved cleanly in an empty directory. So when the incremental
# install fails, throw the lock away and resolve from scratch, which is what an
# upgrade pass wants anyway.
#
# Incremental first, though, and not the other way round: a from-scratch resolve
# re-picks every transitive package, so the lockfile diff you have to review is
# far bigger than the upgrade actually was.
NPM_INSTALL_OK=1
if ( cd app/client-web && npm install ); then
  printf '\033[32m  ✓ npm install (re-resolve package-lock.json)\033[0m\n'
else
  printf '\033[33m  ! npm install failed against the existing lockfile — retrying from scratch\033[0m\n'
  printf '    (deleting app/client-web/package-lock.json + node_modules; the lockfile\n'
  printf '     diff will be large because every transitive version is re-picked)\n'
  if ( cd app/client-web && rm -rf node_modules package-lock.json && npm install ); then
    printf '\033[32m  ✓ npm install (clean re-resolve of package-lock.json)\033[0m\n'
  else
    printf '\033[31m  ✗ npm install (re-resolve package-lock.json)\033[0m\n'
    FAILURES+=("npm install (re-resolve package-lock.json)")
    NPM_INSTALL_OK=0
  fi
fi

# A conflict that survives a from-scratch resolve is a real one: two of the
# bumps genuinely disagree. npm's ERESOLVE output names both packages but not
# the way out, so spell it out.
if [ "$NPM_INSTALL_OK" -eq 0 ]; then
  printf '\033[33m\n  Even a clean resolve failed, so two of the bumps genuinely disagree.\n'
  printf '  In order of preference:\n'
  printf '    1. re-run with TARGET=minor — usually a major bump is what did it;\n'
  printf '    2. revert the one offending range by hand in app/client-web/package.json\n'
  printf '       (npm names both packages in the error) and re-run this script;\n'
  printf '    3. only then consider --legacy-peer-deps, and not from this script —\n'
  printf '       it installs a tree npm itself considers broken.\n\033[0m'
fi

# --- Desktop package at the repo root ---------------------------------------
section "Desktop package (repo root)"

# The root project is the p-swamp desktop package — and, since the merge, the
# analysis core the web backend imports, so it is in the shipped image too and
# its lock is not somebody else's problem. Note its ranges are mostly open
# (`numpy>=2.2.5`), so --upgrade really does reach latest here; the exact pins
# (PySide6==6.8.3) and the git dependencies stay put by construction.
# Locked FIRST, so the web backend below resolves against the result.
step "uv lock --upgrade (root: re-resolve uv.lock)" \
  uv lock --upgrade

# --- Web backend ------------------------------------------------------------
section "Web backend (app/server-python)"

# --upgrade implies --refresh, which is also what makes uv re-read the p-swamp
# path dependency. A plain `uv lock` here would print "Resolved N packages" and
# quietly ignore the root manifest we just changed — the trap AGENTS.md warns
# about, and the reason the two locks are done in this order and never
# separately.
step "uv lock --upgrade (app/server-python: re-resolve uv.lock)" \
  uv lock --upgrade --project app/server-python

# --- What is still held back ------------------------------------------------
section "Held back by a version range (needs a hand edit)"

# Everything reachable has now been taken. What `uv tree --outdated` still flags
# is a DIRECT dependency whose own constraint in pyproject.toml is the thing
# standing in the way — the Python gap this script can't close, since uv has no
# npm-check-updates. Depth 1 on purpose: a transitive package held back by some
# other package's requirement is not something you can act on here. Drop --depth
# to see those anyway.
held_back() {
  # held_back <label> [uv args...] — print only the lines uv annotated with a
  # newer version. `uv tree --outdated` prints the WHOLE tree and marks the
  # outdated entries with "(latest: …)", so without this filter a section headed
  # "held back" lists all 30 direct dependencies and buries the three that are.
  local label="$1"; shift
  local out rc
  out="$(uv tree --outdated --depth 1 "$@" 2>/dev/null)"; rc=$?
  printf '\n%s:\n' "$label"
  if [ "$rc" -ne 0 ]; then
    printf '  (uv tree failed — re-run `uv tree --outdated --depth 1 %s` to see why)\n' "$*"
    return
  fi
  out="$(printf '%s\n' "$out" | grep '(latest:')"
  if [ -z "$out" ]; then
    printf '  (nothing — every direct dependency is at the newest version published)\n'
  else
    # Strip the tree glyphs; with most rows filtered out they connect nothing.
    printf '%s\n' "$out" | sed -E 's/^[^A-Za-z0-9@_]*/  /'
  fi
}

held_back "Root (pyproject.toml)"
held_back "Web backend (app/server-python/pyproject.toml)" --project app/server-python

# npm has no equivalent gap — ncu just rewrote the ranges — but a peer
# dependency conflict can still pin something below latest, and that shows up
# here. `npm outdated` exits non-zero merely for having output, hence the `|| true`.
printf '\nWeb client (npm outdated — peer-dependency holdbacks):\n'
( cd app/client-web && npm outdated ) 2>&1 | sed 's/^/  /' || true

# --- What actually moved ----------------------------------------------------
section "What actually moved"

# A lockfile diff is a terrible summary of an upgrade: ~95% of its changed lines
# are per-wheel sha256 hashes, so one numpy bump rewrites ~40 lines while moving
# one version. Measured on the first real run of this script: 1773 changed lines
# in the root uv.lock, of which 1521 were hash lines and 72 were `version =`.
# So parse the before/after pairs and print the versions themselves — this, not
# the diff, is the thing worth reading first.
version_delta() {
  # version_delta <label> <lockfile> <manifest> — compare the snapshot against
  # the file now, splitting DIRECT dependencies (the ones named in the manifest,
  # which are yours) from transitive ones (which are your dependencies' choices).
  # The split is what makes this readable: a single `radix-ui` bump drags ~70
  # @radix-ui/* packages with it, and listing all of them buries the four lines
  # you actually needed to see. VERBOSE=1 prints the transitive detail too.
  local label="$1" path="$2" manifest="$3"
  local before="$BEFORE_DIR/$(echo "$path" | tr / _)"
  printf '\n%s (%s):\n' "$label" "$path"
  if [ ! -f "$before" ] || [ ! -f "$path" ]; then
    printf '  (no before/after pair to compare)\n'
    return
  fi
  VERBOSE="${VERBOSE:-}" python3 - "$before" "$path" "$manifest" <<'PYEOF'
import json, os, re, sys

def read_lock(path):
    """name -> comma-joined set of versions. A package can be resolved at two
    versions at once: uv forks a resolution when requires-python spans a
    boundary (numpy 2.4.6 for <3.12, 2.5.2 for >=3.12), and npm nests a second
    copy when two dependents disagree."""
    text = open(path, encoding="utf-8").read()
    out = {}
    if path.endswith(".json"):
        # npm: every entry under `packages`, keyed by the last node_modules/
        # segment so a nested copy collapses onto the same name.
        for key, meta in (json.loads(text).get("packages") or {}).items():
            if not key or "version" not in meta:
                continue          # the "" root entry, plus link/workspace stubs
            out.setdefault(key.rsplit("node_modules/", 1)[-1], set()).add(meta["version"])
    else:
        # uv.lock: TOML, but only two fields per [[package]] block are wanted,
        # so a regex avoids needing a TOML parser at all.
        for block in text.split("[[package]]")[1:]:
            n = re.search(r'^name = "(.*)"$', block, re.M)
            v = re.search(r'^version = "(.*)"$', block, re.M)
            if n and v:
                out.setdefault(n.group(1), set()).add(v.group(1))
    return {k: ", ".join(sorted(v)) for k, v in out.items()}

def read_direct(path):
    """The names the manifest itself asks for — everything else is transitive."""
    if path.endswith(".json"):
        m = json.load(open(path, encoding="utf-8"))
        names = set()
        for field in ("dependencies", "devDependencies", "optionalDependencies"):
            names |= set(m.get(field) or {})
        return names
    # pyproject.toml. tomllib is 3.11+, which this project requires anyway, but
    # the report is a nicety and must not take the script down on an older
    # interpreter — hence the regex fallback.
    text = open(path, encoding="utf-8").read()
    try:
        import tomllib
        data = tomllib.loads(text)
        reqs = list(data.get("project", {}).get("dependencies", []))
        for group in (data.get("project", {}).get("optional-dependencies") or {}).values():
            reqs += group
        for group in (data.get("dependency-groups") or {}).values():
            reqs += [r for r in group if isinstance(r, str)]
    except Exception:
        reqs = re.findall(r'^\s*"([^"]+)"\s*,?\s*$', text, re.M)
    # Strip extras and any version/url specifier, then normalise the way PyPI
    # does (case-insensitive, - and _ equivalent) so these match the lock's keys.
    out = set()
    for req in reqs:
        name = re.split(r"[\s\[<>=!~;@]", req.strip(), maxsplit=1)[0]
        if name:
            out.add(name.lower().replace("_", "-"))
    return out

before, after, direct = read_lock(sys.argv[1]), read_lock(sys.argv[2]), read_direct(sys.argv[3])

def classify(names):
    return sorted(n for n in names if n in direct), sorted(n for n in names if n not in direct)

changed = {k for k in before.keys() & after.keys() if before[k] != after[k]}
added   = after.keys() - before.keys()
removed = before.keys() - after.keys()

if not (changed or added or removed):
    print("  (nothing moved — everything was already at the newest version its range allows)")
    raise SystemExit

verbose = os.environ.get("VERBOSE") == "1"
for kind, names, fmt in (
    ("changed", changed, lambda n: f"{before[n]} -> {after[n]}"),
    ("added",   added,   lambda n: f"added {after[n]}"),
    ("removed", removed, lambda n: f"removed (was {before[n]})"),
):
    d, _ = classify(names)
    for name in d:
        print(f"  {name:<34} {fmt(name)}")

t_changed, t_added, t_removed = (classify(x)[1] for x in (changed, added, removed))
if verbose:
    for names, fmt in ((t_changed, lambda n: f"{before[n]} -> {after[n]}"),
                       (t_added,   lambda n: f"added {after[n]}"),
                       (t_removed, lambda n: f"removed (was {before[n]})")):
        for name in names:
            print(f"    (transitive) {name:<22} {fmt(name)}")

d_changed, d_added, d_removed = (classify(x)[0] for x in (changed, added, removed))
print(f"\n  direct:     {len(d_changed)} upgraded, {len(d_added)} added, {len(d_removed)} removed")
print(f"  transitive: {len(t_changed)} upgraded, {len(t_added)} added, {len(t_removed)} removed"
      + ("" if verbose else "   (VERBOSE=1 to list)"))
PYEOF
}

version_delta "Desktop package" uv.lock                          pyproject.toml
version_delta "Web backend"     app/server-python/uv.lock        app/server-python/pyproject.toml
version_delta "Web client"      app/client-web/package-lock.json app/client-web/package.json

# --- The diff to review -----------------------------------------------------
section "Diff to review"
git --no-pager diff --stat -- "${MANIFESTS[@]}"
printf '\nThe line counts above are mostly per-wheel hashes, not upgrades — read the\n'
printf 'list before them for that. What the lockfile diff IS good for is spotting a\n'
printf 'package that appeared without being asked for. The manifests are the\n'
printf 'decision and are small enough to read in full:\n'
printf '  git --no-pager diff -- app/client-web/package.json pyproject.toml app/server-python/pyproject.toml\n'

# --- Does it still hold together? -------------------------------------------
# The upgrade is only half the job; the other half is finding out what it broke.
# Same gate as the pre-push hook and CI, so a green run here means the same
# thing it means there. It does NOT run the app — a type-clean upgrade can still
# break at runtime, which is what step 3 in the header is for.
if [ "${NO_CHECK:-}" = "1" ]; then
  section "Skipping error_check.sh (NO_CHECK=1)"
else
  section "Running scripts/error_check.sh"
  ./scripts/error_check.sh || FAILURES+=("error_check.sh")
fi

# --- Summary ----------------------------------------------------------------
section "Summary"
if [ "${#FAILURES[@]}" -eq 0 ]; then
  printf '\033[32mDependencies updated and checks passed.\033[0m\n'
  printf 'Now: read the diff, run the app, then open a PR.\n'
  exit 0
fi
printf '\033[31m%d step(s) failed:\033[0m\n' "${#FAILURES[@]}"
printf '  - %s\n' "${FAILURES[@]}"
printf '\nThe working tree may hold a partial upgrade. `git checkout --` the\n'
printf 'manifests and lockfiles to start over, or fix the failure and re-run.\n'
exit 1
