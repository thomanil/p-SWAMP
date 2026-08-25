# How we work together

We are multiple teams with different strengths and areas of expertise/focus, 
collaborating together remotely and asynchronously. These are some basic practices that 
we use to work effectively without tripping over each other.

## Communication norms

Its a good idea to keep a running conversation between us. These are the primary ways we keep each other in the loop:

- "Ongoing" chat in shared Teams chat (_TBD/TODO, needs to be cleared on Statnett side_)
- Recurring video calls
- Some writing to each other in pull requests

TODO some clarification about how teams resolve shared tasks/dependencies, task tracking etc.

## Git workflow, required bits

- main branch is protected, eg. you cannot push commits directly to it

- Instead, use feature branches, and create pull requests to merge your code into main

- We do not require other people to approve your branches before you merge. 
However, if your branch contains any broken tests or other failing quality checks, 
you will not be able to merge until those are fixed in your branch.

- We kindly suggest using _"squash and merge"_ when you merge pull requests. (If that is not what the green merge button already says in your pull request,
then try clicking the little dropdown button on its right side and you should find the squash option).
This makes each of your pull requests merge into main as a single atomic commit, which makes the main branch easier to reason about if anything goes wrong :)

## "git hygiene" and collaboration tips 

A few additional tips to consider:

- Even if you are only working alone on a section of the project, consider writing up some sentences of description in each pull request
to teach the rest of us what you are working on/towards :)

- If you are working on a large chunk of functionality/code, it does not have to go in with a single big bang pull request.
Multiple PRs along the way is a good idea (as long as it does not break things for others along the way).

- If you need/want to keep your feature branch more long lived, it is a good idea to rebase it from the main branch daily.
Eg. switch to main branch, pull the latest changes to it, go back to your own branch, run `git rebase main`. This "redoes" your branch so that it looks as if you
branched it off of the latest version of `main`. If you have been working on the same places in the repo as others, you may get some conflicts. However its much easier to resolve these
conflicts early and often rather than after a week or two of steady work!

- There is a `CODEOWNERS` file at the top of the repo, listing who owns which parts of the code.
When your branch touches somebody's files, GitHub adds them as a reviewer on your pull request automatically.
This does not block you from merging, it just makes sure Github notifies the right people about the change. The header
comment in `CODEOWNERS` explains the details, and tells you how to add yourself if you "own" an area the repo.

## Do not put any secrets in this repo

This repo will periodically be published to a public mirror (Linux Foundation Energy). 
Do not check in any sensitive api keys, tokens etc.

If you have any algorithms or other sensitive work that should not land in the public mirror right away, 
raise that concern and lets chat about it together before you do a pull request, 
so we can figure out if they should just live in branch or something else 
(depending on the timeline for how long you need to hold them)

## Keep the project testable/runnable locally 

If/when we introduce more subprocesses, services etc, make sure they are defined both in the kubernetes deployments as well as the
local docker compose file for local testing.

If you introduce some parts that only "truly work" in the deployed cloud env, we should also make sure that it remains
testable locally. Eg. synthetic data, stubs/mocks etc to make sure the project can still be verified end-to-end localy.

## Only complicate the rig when you have to

The project will do some computation intensive operations, so we may well have to split it into multiple processes/services at some point.
But we try to start as simple as we can, and do more elaborate rigging only when/where actually needed.