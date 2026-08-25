# How we work together

We are multiple teams with different strengths and areas of expertise/focus,
collaborating together remotely and asynchronously. These are some basic practices that
we use to work effectively without tripping over each other.

## Areas of focus for each sub-team

We cross over and help each other out where we can, but these are our core areas:

*SINTEF*: Domain knowledge, core algorithms, core/backend functionality

*SINTEF Digital*: TODO

*IFE*: User experience, frontend implementation

*RISE*: Domain knowledge, core/backend algorithms

*STATNETT/UNICUS*: End to end architecture, integration, overall developer experience

## Communication norms

Its a good idea to keep a running conversation between us. These are the primary ways we keep each other in the loop:

- "Ongoing" text chat in shared Teams chat (_TBD/TODO, needs to be cleared on Statnett side_)
- Some writing to each other in pull requests (see git tips below)
- Statnett/Unicus may send ops/PSA messages on shared email list from time to time (scheduled downtime, upgrades etc)
- If we need to do focused problem solving/knowledge sharing across the sub-teams, we set up video calls ad-hoc as needed

_TODO some clarification about how teams resolve shared tasks/dependencies, task tracking etc_

_TODO Also need to clear in statnett how to set up a shared teams type text chat without security issues_

## Git workflow, mandatory bits

These are the minimum things we need to do to not trip over each other:

- `main` branch is protected, eg. you cannot push commits directly to it

- Instead, use feature branches, and create pull requests to merge your code into main

- We do not require other people to approve your branches before you merge.
However, we have some automated tests and checks. If your branch breaks any of these,
you will not be able to merge until those are fixed in your branch.

## "git hygiene", collaboration tips

A few additional tips to consider:

- **Consider writing up a little description/context in every pull request.** Even if you are only working alone on a section of the project, writing up some sentences of description in each pull request
to teach the rest of us what you are working on/towards. Sometimes others may be able to give tips and advice, too :)

- **If you are working on a large chunk of functionality/code, it does not have to go in with a single big bang pull request.**
If possible, try to split it into multiple self-contained PRs(as long as it does not break things for others along the way).
Integrating back into `main` often helps you avoid surprises and code merge conflicts.

- **If you need/want to keep your feature branch more long lived, it is a good idea to rebase it from the main branch daily.**
Eg. switch to main branch, pull the latest changes to it, go back to your own branch, run `git rebase main`.
This in effect "redoes" your branch so that it looks as if you
branched it off of the latest version of `main`. If you have been working on the same places in the repo as others, you may get some conflicts when you do this.
However its much easier to resolve these conflicts early and often rather than after a week or two of steady work!

- **We kindly suggest using _"squash and merge"_ when you merge pull requests.** (If that is not what the green merge button already says in your pull request,
  then try clicking the little dropdown button on its right side and you should find the squash option).
  This makes each of your pull requests merge into main as a single atomic commit, which makes the main branch easier to reason about if anything goes wrong :)

- **There is a `CODEOWNERS` file at the top of the repo, listing who owns which parts of the code.**
When your branch touches somebody's files, GitHub adds them as a reviewer on your pull request automatically.
This does not block you from merging, it just makes sure Github notifies the right people about the change. The header
comment in `CODEOWNERS` explains the details, and tells you how to add yourself if you "own" an area the repo.

## Always keep the repo testable/runnable locally

The main branch should always be runnable. Eg. anytime we push an update to the public Linux Foundation Energy mirror,
the project should be in a state where it can be launched and played with by interested developers/stakeholders.

When you introduce new functionality, we highly suggest you include some unit tests that automate regression testing, eg.
if something gets broken down the line, an automated test should be able to catch it. If some test is very hard to automate,
ask the rest of the projects participants for ideas! If the functionality is still truly hard to automoate testing,
a fallback can be to at least write a .md checklist about how that service/component/part can be tested manually.

If/when we introduce more subprocesses, services etc, make sure they are defined both in the kubernetes deployments as well as the
local docker compose file for local testing, so it is replicated in some way locally.

If you introduce some infrastructure that only "truly work" in the deployed cloud env, we should also make sure that it remains
testable locally. Eg. synthetic data, stubs/mocks etc to make sure the project can still be verified end-to-end localy.

## Do not put any secrets in this repo

This repo will periodically be published to a public mirror (Linux Foundation Energy).
Do not check in any sensitive api keys, tokens etc.

If this happens by accident, they are hard to remove again fully, so secrets added to the git repo are considered compromised and
have to immediately be updated/rotated in the system they access/belong to.

## "Holding off" sharing some algorithms with published code?

Remember: the main branch of the repo will periodically be published in the public
[Linux Foundation Energy](https://lfenergy.org/projects/p-swamp/] mirror/project] project) project.

If you have any algorithms or other sensitive work that should not land there right away,
raise that concern and lets chat about it together *before* you do a pull request into main - so we can figure out
together if those bits should just live in branch or something else, depending on timeline and sensitivity.

## Only complicate the rig when you have to :)

The project will do some computation intensive operations, and we may well have to split it into multiple processes/services at some point.
But we try to start as simple as we can, and do more elaborate rigging only when/where actually needed, supported by performance numbers and experiments.

When you see that we need to add new infra/services/, keep the rest of the project/team in the loop.

## Write down the decisions that shape the system

Bigger architectural choices are recorded as short **Architecture Decision Records**
in [`doc/adr/`](adr/). Each one is a page or less: the context that forced the
choice, what we decided, what it costs us, and the alternatives we rejected.

Write one when: a choice is expensive to reverse, cuts across sub-teams, or is one
a newcomer would reasonably ask "why is it like this?". Small everyday
choices inside your own team's code do not need one.

An ADR is history, not documentation: we do not rewrite old ones, we write a new
one that supersedes them.
