---
name: issue-to-merge
description: Run one disciplined GitHub issue iteration from leaf selection through a focused implementation, independent review, verified pull request, and merge.
---

# Issue to merge

Run exactly one iteration unless the user explicitly asks to continue.

## Select and decide

Choose an open issue with no sub-issues and verify its hierarchy, current-main
behavior, and overlap with open pull requests. Implement it only when the
solution is self-contained and modest. If it is too broad, create independently
shippable child issues, link them to the parent, verify the hierarchy, and stop.

Read the repository map and relevant source before implementation. Treat source,
tests, workflows, manifests, and current GitHub state as authoritative.

## Implement and review

Create a branch from current `origin/main`, make only the focused change, and
add behavior-oriented tests when appropriate. Before final verification, ask a
separate agent to use `$adversarial-review` with the issue, acceptance criteria,
diff, tests, and relevant boundaries. The review must remain read-only.

Fix in-scope blockers and create separate issues for valid out-of-scope
findings. Repeat the review after material changes.

## Verify and ship

Run the narrow tests plus `nix flake check`, inspect the final diff, and create
a signed Conventional Commit with a wrapped explanatory body. Push and open a
pull request that closes the issue. Merge only after required checks pass, then
confirm both the merge and issue closure before stopping.
