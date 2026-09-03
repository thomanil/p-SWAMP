# Architecture Decision Records

Short notes recording an architectural decision, why we made it, and what it
costs. One decision per file.

An ADR is a record of a moment, not documentation of the current system. Once
merged it is not edited — if we change our minds we write a new ADR and mark the
old one superseded, so the reasoning survives the decision. For how the system
works *now*, read `AGENTS.md` and the rest of `doc/`.

**Writing one:** copy `template.md`, name it `NNN-<decision>.md` with the next
free number (`003-use-openapi-contract-for-client-server-api.md`), keep it to a
page, and open a pull request like any other change.

**When:** a choice that is expensive to reverse, cuts across sub-teams, or would
make a newcomer ask "why on earth is it like this?". Everyday choices inside one
team's own code do not need one.
