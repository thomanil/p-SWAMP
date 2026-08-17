# ADR-001: Record architecture decisions

- Status: Accepted
- Date: 2026-08-25

## Context

We are several sub-teams working remotely and asynchronously, so the decisions
that shape the whole system get made in calls, chat and PR comments — and then
lost. The code shows *what* we settled on, never *why*, and never what we
rejected. Several choices in this repo hold a lot up and look like mistakes
until someone explains them. The main branch is also published to the public
Linux Foundation Energy mirror, where readers have nobody to ask.

## Decision

We will record significant architectural decisions as short markdown ADRs in
`doc/adr/`, reviewed through the normal pull request flow. One decision per
file, numbered in order: context, decision, consequences, alternatives. Records
are immutable once merged — we supersede rather than rewrite. `README.md` here
holds the index and the practical instructions.

## Consequences

The reasoning behind a decision outlives the call it was made on, and proposing
an ADR gives a cross-team decision one place to be discussed *before* it is
built.

It costs a little writing per decision, and only works if we keep the bar high:
a half-populated ADR directory is worse than none. ADRs are also not
documentation — anyone reading one has to check it against the code and against
any later ADR.

We started using ADRs a little after project start, so the first
records are retrospective write-ups of decisions already made.

## Alternatives considered

- **Keep explaining decisions in `AGENTS.md`.** It documents the system as it is
  today, so it has no room for rejected options or for "we changed our minds, and
  here is why" — the history keeps getting overwritten.
- **A decisions page in a wiki or Teams.** Easier to write, but unversioned,
  unreviewed, drifts from the code, and never reaches the public mirror.
- **A heavier format (MADR with scoring, an RFC process).** More ceremony than a
  research project this size will sustain; people would skip it.
