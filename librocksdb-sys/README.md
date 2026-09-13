# Native RocksDB bindings

`ckb-librocksdb-sys 11.8.1` builds RocksDB 11.8.1 and generates its C API bindings.
It requires Rust 1.95.0, a C++20 compiler, Clang, and LLVM. The crate links the
bundled native code statically. The published package includes the required
sources; Git checkouts must initialize their submodules before building.

Use `ckb-rocksdb` for the Rust wrapper. Native feature flags select compression
codecs and platform integrations; see the [wrapper documentation](https://docs.rs/ckb-rocksdb).
Keep codecs required by existing SSTs enabled when upgrading a database.

## Bundled source licenses

The Rust bindings use Apache-2.0. The package retains the following native
license notices alongside their sources:

| Component | License notice |
|---|---|
| RocksDB | `rocksdb/LICENSE.Apache`, `rocksdb/LICENSE.leveldb` |
| Snappy | `snappy/COPYING` |
| LZ4 library | `lz4/lib/LICENSE` |
| Zstandard | `zstd/LICENSE` |
| zlib | `zlib/LICENSE` |
| bzip2 | `bzip2/LICENSE` |
| liburing | `liburing/LICENSE` |

RocksDB's Apache-2.0 license is used for the bundled engine. BSD-2-Clause,
BSD-3-Clause, MIT, Zlib, and bzip2-1.0.6 notices cover the additional bundled code.
