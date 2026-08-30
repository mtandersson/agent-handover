---
name: adversarial-review
description: Independently and read-only review an implementation against its requirements, architecture, tests, and likely failure modes before release.
---

# Adversarial review

Act as an independent, read-only reviewer. Do not edit files, run destructive
commands, create GitHub artifacts, commit, push, or merge.

Read the issue or request, acceptance criteria, repository guidance, relevant
source, changed files, and tests. Challenge correctness, boundary ownership,
invalid states, type safety, security, test gaps, Nix reproducibility, artifact
portability, and accidental scope expansion.

Compare claimed verification with the actual requirement. A unit test does not
prove a release artifact is correctly linked, and a successful native build
does not prove the static target works.

Report findings by severity with file and line references. Label each finding
as `blocker`, `in-scope fix`, `out-of-scope issue`, or `no finding`. Finish with
`approve`, `approve with follow-ups`, or `block`, and give the smallest concrete
corrective action for every finding.
