#!/usr/bin/env bash

set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "usage: $0 <version>" >&2
  exit 2
fi

version=$1
binary=agent-handover
gnu_archive="${binary}-v${version}-x86_64-unknown-linux-gnu.tar.gz"
musl_archive="${binary}-v${version}-x86_64-unknown-linux-musl.tar.gz"

cargo set-version "$version"
printf '%s\n' "$version" > VERSION

rm -rf dist
mkdir -p dist
nix build .#gnu --out-link dist/result-gnu
nix build .#musl --out-link dist/result-musl

gnu_binary="dist/result-gnu/bin/$binary"
musl_binary="dist/result-musl/bin/$binary"

"$gnu_binary" --version | grep -Fx "${binary} ${version}"
"$musl_binary" --version | grep -Fx "${binary} ${version}"
file "$gnu_binary" | grep -F "x86-64"
file "$gnu_binary" | grep -F "dynamically linked"
[[ "$(patchelf --print-interpreter "$gnu_binary")" == "/lib64/ld-linux-x86-64.so.2" ]]
[[ -z "$(patchelf --print-rpath "$gnu_binary")" ]]
file "$musl_binary" | grep -F "x86-64"
file "$musl_binary" | grep -E "static(-pie|ally) linked"

tar --sort=name --mtime='UTC 1970-01-01' --owner=0 --group=0 --numeric-owner \
  -C "$(dirname "$gnu_binary")" -cf - "$binary" | gzip -n > "dist/$gnu_archive"
tar --sort=name --mtime='UTC 1970-01-01' --owner=0 --group=0 --numeric-owner \
  -C "$(dirname "$musl_binary")" -cf - "$binary" | gzip -n > "dist/$musl_archive"

(
  cd dist
  sha256sum "$gnu_archive" "$musl_archive" > SHA256SUMS
  sha256sum --check SHA256SUMS
)
