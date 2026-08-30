# agent-handover

Minimal Rust command-line project with reproducible Nix builds and automated
Conventional Commit releases.

## Development

Install Nix with flakes enabled, plus direnv with nix-direnv support. Then enter
the development environment:

```sh
direnv allow
```

Without direnv, run commands explicitly through the flake:

```sh
nix develop
cargo test
```

The main verification command matches CI:

```sh
nix flake check
```

Build either Linux x86-64 release target with:

```sh
nix build .#gnu
nix build .#musl
```

`gnu` produces a normal glibc-linked binary. `musl` produces a statically
linked binary.

## Releases

Pushes to `main` are analyzed using Conventional Commits. `feat` creates a
minor release; `fix`, `perf`, `refactor`, and `revert` create patch releases;
and a breaking change creates a major release. Other accepted commit types do
not publish a release.

Every release updates `CHANGELOG.md`, `VERSION`, `Cargo.toml`, and `Cargo.lock`,
then publishes GNU and musl archives plus `SHA256SUMS` to GitHub Releases.
