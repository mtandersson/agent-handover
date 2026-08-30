# Agent guidance

This file provides working guidance for coding agents in this repository.
`AGENTS.md` is a symlink to this file.

## Read the repository map first

Read `README.md` before searching broadly or changing code. Keep it current
when a change alters the supported commands, build outputs, release behavior,
or repository structure. Executable source, manifests, workflows, and tests
remain authoritative if documentation is stale.

## Tooling

All project dependencies come from the locked Nix flake. Use direnv or prefix
commands with `nix develop --command`; do not install project tools globally or
introduce an unmanaged dependency installer.

Run these commands from the repository root:

| Command | Purpose |
| --- | --- |
| `nix flake check` | Run formatting, linting, tests, script checks, and builds |
| `nix build .#gnu` | Build the Linux x86-64 glibc binary |
| `nix build .#musl` | Build the static Linux x86-64 musl binary |
| `nix develop --command cargo test` | Run Rust tests interactively |
| `nix develop --command cargo fmt --check` | Check Rust formatting |
| `nix develop --command cargo clippy --all-targets -- -D warnings` | Run Clippy |

## Tests

Tests specify desired behavior and durable contracts. Name them for what the
software must do, cover meaningful input classes, and avoid wording tied to a
particular defect or regression.

## Commits and pull requests

Use signed Conventional Commits:

```text
<type>(<optional scope>): <imperative summary>
```

Use a lowercase type from `feat`, `fix`, `docs`, `style`, `refactor`, `perf`,
`test`, `build`, `ci`, `chore`, or `revert`. Do not end the summary with a
period. After a blank line, include a concise body explaining why the change
was made and what it contains; wrap body lines at 72 characters or fewer.

Keep each pull request focused on one change. Put unrelated findings in a
separate issue rather than expanding the active change.

## Releases

Qualifying Conventional Commits on `main` publish releases automatically.
Never create release tags, edit generated changelog entries, or change version
metadata by hand unless repairing the release system itself.
