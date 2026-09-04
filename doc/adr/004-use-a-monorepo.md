# ADR-004: Keep the project in a single monorepo

- Status: Accepted
- Date: 2026-09-04

## Context

p-SWAMP is growing into several distinct modules — sets of functionality that
could each stand alone. We have to decide whether they live together in one
repository or each get their own. We are several sub-teams working remotely and
asynchronously, and the modules share code, data contracts and a build.

## Decision

We will keep all of p-SWAMP's modules in this single repository, organised into
clearly separated modules within it. New functionality is added as a module here
rather than as a new repository.

## Consequences

One checkout, one CI pipeline and one place to run the whole system, so a change
that spans modules is a single atomic commit and pull request — no coordinating
versioned releases across repos just to keep them working together. Shared code
and the client/server contract ([ADR-003](003-use-openapi-contract-for-client-server-api.md))
stay in step, and cross-team review happens in one place.

The cost is that we must hold module boundaries by discipline rather than by repo
walls: without a sensible module structure and clear ownership (see `CODEOWNERS`),
the modules can quietly grow tangled. The repository and its CI also get heavier
as modules accumulate.

## Alternatives considered

- **A separate repository per module.** Gives each module hard boundaries and its
  own release cadence, but turns every cross-module change into coordinated,
  versioned releases across repos — heavy friction for a small, fast-moving,
  cross-team project, and easy for modules to drift out of sync.
- **A monorepo now, split later if needed.** This is effectively what we chose:
  start together for low friction, and extract a module to its own repo only if a
  concrete need (independent release, separate access) ever justifies it.
