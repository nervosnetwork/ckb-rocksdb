#!/usr/bin/env bash
set -euo pipefail

# Build metadata identifies the exact libraries tested by this CI invocation.
metadata=$1
native=$(jq -r 'select(.reason == "build-script-executed" and (.package_id | contains("ckb-librocksdb-sys"))) | .out_dir' "$metadata")
jemalloc=$(jq -r 'select(.reason == "build-script-executed" and (.package_id | contains("tikv-jemalloc-sys"))) | .out_dir' "$metadata")
test -n "$native" && test -n "$jemalloc"

c++ -std=c++20 -I librocksdb-sys/rocksdb/include \
  librocksdb-sys/tests/native_features.cc \
  "$native/librocksdb.a" "$native/liburing/src/liburing.a" \
  "$native/libsnappy.a" "$native/liblz4.a" "$native/libzstd.a" \
  "$native/libz.a" "$native/libbz2.a" \
  "$jemalloc/build/lib/libjemalloc_pic.a" -pthread -ldl -lm \
  -o "$native/native-features"

ldd "$native/native-features" > "$native/native-features.ldd"
if grep -Eq 'lib(jemalloc|uring)' "$native/native-features.ldd"; then
  cat "$native/native-features.ldd"
  exit 1
fi
strace -e io_uring_setup,io_uring_enter -o "$native/io-uring.trace" "$native/native-features"
cat "$native/native-features.ldd" "$native/io-uring.trace"
grep -Eq 'io_uring_setup\(.*= [0-9]+' "$native/io-uring.trace"
grep -Eq 'io_uring_enter\(.*= [0-9]+' "$native/io-uring.trace"
strace -e io_uring_setup,io_uring_enter -o "$native/io-uring-denied.trace" "$native/native-features" --deny-io-uring
cat "$native/io-uring-denied.trace"
grep -Eq 'io_uring_setup\(.*= -1 EPERM' "$native/io-uring-denied.trace"
