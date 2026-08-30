# Agent guidance

This file provides working guidance for coding agents in this repository.
`AGENTS.md` is a symlink to this file.

## Read the repository map first

Read `README.md` before searching broadly or changing code. Keep it current
when a change alters the supported commands, build outputs, release behavior,
repository structure, configuration contract, or operational guarantees.
Executable source, manifests, workflows, and tests remain authoritative if
documentation is stale.

## Repository and module map

The project is currently a minimal Rust CLI:

- `src/main.rs`: current command entry point and its unit tests.
- `src/config.rs`: private XDG host configuration and path contracts.
- `src/enrollment.rs`: fakeable webhook-token enrollment and private file storage.
- `src/http.rs`: loopback HTTP serving, webhook authentication, and fake dispatch boundary.
- `flake.nix` and `Cargo.toml`: reproducible toolchain, checks, and package
  metadata.
- `.github/workflows`: pull-request validation and automated releases.
- `scripts` and `release-tooling`: release preparation and semantic-release
  support.

As the runner is implemented, preserve these provider-neutral boundaries:

- CLI and HTTP entry points parse input and delegate; they do not own workflow
  policy.
- Orchestration owns discovery, attempt transitions, and recovery without
  depending on a concrete executor.
- Executor adapters translate a prepared task into a provider invocation;
  Codex is the only initial adapter.
- The Notion adapter owns API and webhook wire formats behind a fakeable
  interface.
- Local state owns durable launch authority, process locking, and recovery
  records.

Update this map when concrete modules are added or moved.

## Architecture invariants

- Support Notion Free through the Public API and connection webhooks. Do not
  require paid database automations or webhook actions.
- Treat webhooks as discovery accelerators. Reconcile at `serve` startup and on
  its configured interval; make `run-once` perform the equivalent discovery and
  sequential drain once.
- Refetch current Notion state after an authenticated event. Ignore unrelated,
  duplicate, out-of-order, non-Pending, and non-Codex events.
- Enforce one runner per stable local state directory with a process lock and
  execute one task at a time.
- Use private local run records as automatic-launch authority. Notion status is
  an observable projection, not a distributed lock.
- Create a durable prepared attempt and immutable run ID before remote writes.
  Require both `Running` status and the initial journal attempt to be visible in
  Notion before launch.
- Resolve ambiguous journal creation by querying the run ID; never create a
  second attempt for the same run ID.
- Persist `launch_intent` immediately before spawning an executor. Automatically
  launch a run at most once and never relaunch an interrupted attempt.
- Persist a validated structured result before terminal Notion writes. Recovery
  may replay idempotent journal and status finalization without another launch.
- Convert interrupted launch-boundary attempts to `Error` with an explicit
  `outcome unknown; external effects may have occurred` warning.
- Manual retry requires `Error -> Pending` and creates a new run ID. Never claim
  exactly-once execution; a retry may duplicate non-idempotent effects.
- Bound retries to safe Notion reads and idempotent finalization. Never
  automatically retry agent actions.

## Executor and task boundaries

- Keep orchestration provider-neutral and provider-specific CLI behavior in
  executor adapters.
- Invoke Codex through its supported non-interactive, machine-readable
  interface using the configured executable, working directory, Codex profile,
  sandbox policy, permitted environment, and timeout.
- Never weaken or bypass the configured sandbox implicitly.
- The recursively rendered task page body is the sole instruction source the
  orchestrator supplies. Render nested blocks deterministically and handle API
  pagination.
- Do not automatically traverse relations, linked pages, or attachments. The
  task may explicitly ask Codex to access them through configured tools.
- Validate executor results containing `outcome`, `summary`, `actions`, and
  `warnings`. Treat blocked or incomplete work as an error outcome.

## Privacy and security

- Store host configuration, secrets, and operational state outside the
  repository in XDG locations. Enforce `0700` private directories and `0600`
  sensitive files.
- Never commit real Notion IDs, tokens, personal paths, prompts, captured agent
  output, or run data. Examples and fixtures must use obvious placeholders only.
- Keep full task prompts and executor output out of normal logs and error
  messages. Errors must remain actionable without revealing secrets.
- Authenticate Notion webhooks from the unmodified raw body with constant-time
  HMAC comparison before accepting an event.
- Bind locally by default. Cloudflare Tunnel provisioning and service management
  remain external to the project.
- Treat users who can edit eligible task bodies as trusted to instruct Codex
  within the configured host permissions.

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

Use fake Notion and executor adapters in automated tests. Fixtures must contain
no live service data or personal identifiers. Cover state transitions and
failure boundaries, including duplicate and out-of-order events, pagination,
ambiguous writes, process locking, sequential execution, timeouts, malformed
results, crash recovery, and unknown launch outcomes.

## Documentation

Keep current behavior separate from target or planned behavior. When a command,
configuration field, module, or guarantee becomes real, update `README.md` in
the same change. Link to current primary documentation for external interfaces
whose behavior can change.

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
