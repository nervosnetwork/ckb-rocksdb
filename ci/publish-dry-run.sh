#!/bin/sh
set -eu

cd "$(dirname "$0")/.."
release_dir=$(mktemp -d "${TMPDIR:-/tmp}/ckb-rocksdb-publish.XXXXXX")
trap 'rm -rf "$release_dir"' 0
trap 'exit 1' HUP INT TERM

# Workspace verification unpacks unpublished dependencies into CARGO_HOME too.
# Isolate both directories so shared cache permissions and old sources cannot
# affect verification of the archives produced by this run.
CARGO_HOME="$release_dir/cargo-home" cargo publish \
    --locked --workspace --dry-run --features portable,jemalloc,io-uring \
    --target-dir "$release_dir/target" "$@"
