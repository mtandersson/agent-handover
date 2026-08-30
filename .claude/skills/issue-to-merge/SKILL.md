---
name: issue-to-merge
description: 'Process one GitHub issue—including requests such as "implement #123"—by enforcing a scope gate, splitting broad work into smaller preferably vertical child issues, or taking one suitable leaf through implementation, independent review, a verified pull request, and merge.'
---

# Issue to merge

Run exactly one iteration unless the user explicitly asks to continue.

## Select and decide

Use an issue identified by the user; otherwise choose an open issue with no
sub-issues. Verify its hierarchy, current-main behavior, and overlap with open
pull requests before implementation.

Apply the scope gate even when the user explicitly says to implement a specific
issue. Naming an issue selects it for this workflow; it does not require the
whole issue to be implemented in one pull request and does not bypass
decomposition. Implement only when the selected issue is self-contained and
modest, with a focused verification boundary.

If the selected issue is broader than that, do not begin implementation or
open a pull request for the parent. Split it into smaller coherent child issues,
link them to the parent, verify the hierarchy, and stop after reporting the
split. A child does not need to be immediately implementable; a later iteration
may apply the same scope gate and split it again. Prefer vertical slices that
express a use case or observable behavior a human can test. Avoid horizontal
splits by technical layer when a meaningful vertical boundary is available.
Do not continue by implementing a child issue in the same iteration unless the
user explicitly asks to continue.

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
