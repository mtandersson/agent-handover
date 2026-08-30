---
name: writing-tests
description: Write or revise tests as durable specifications of desired behavior rather than descriptions of defects or regressions.
---

# Writing tests

Write each test as a specification of the contract the code must satisfy.

- Name tests for the desired behavior.
- Assert the invariant or postcondition directly.
- Cover the general input class rather than only the value that once failed.
- Keep defect numbers, old behavior, and regression language out of names and
  comments.

After the implementation is correct, a new teammate must be able to read the
test without knowing a defect ever existed and without needing to rename or
rewrite it.
