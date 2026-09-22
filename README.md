ckb-rocksdb
============
[![Build Status](https://github.com/nervosnetwork/ckb-rocksdb/actions/workflows/rust.yml/badge.svg)](https://github.com/nervosnetwork/ckb-rocksdb/actions/workflows/rust.yml?query=branch%3Amaster)
[![crates.io](https://img.shields.io/crates/v/ckb-rocksdb.svg)](https://crates.io/crates/ckb-rocksdb)
[![documentation](https://docs.rs/ckb-rocksdb/badge.svg)](https://docs.rs/ckb-rocksdb)
[![license](https://img.shields.io/crates/l/ckb-rocksdb.svg)](https://github.com/nervosnetwork/ckb-rocksdb/blob/master/LICENSE)
[![Discord](https://img.shields.io/badge/chat-on%20Discord-7289DA.svg)](https://discord.com/invite/nervos)

Rust bindings to RocksDB for Nervos CKB, including owned column families for online storage reclamation.

## Requirements

- Rust 1.95.0 or newer
- A C++20 compiler, Clang, and LLVM

## Contributing

Feedback and pull requests welcome!  If a particular feature of RocksDB is
important to you, please let me know by opening an issue, and I'll
prioritize it.

## Usage

This binding is statically linked with a specific version of RocksDB. If you
want to build it yourself, make sure you've also cloned the RocksDB and
compression submodules:

    git submodule update --init --recursive

## Compression Support

New SSTs use LZ4 by default when available. Existing SSTs remain readable when
their compression codecs are enabled; keep Snappy enabled when upgrading a
database that contains Snappy-compressed SSTs. Explicit compression options
continue to take precedence.

By default, support for the [Snappy](https://github.com/google/snappy),
[LZ4](https://github.com/lz4/lz4), [Zstd](https://github.com/facebook/zstd),
[Zlib](https://zlib.net), and [Bzip2](http://www.bzip.org) compression
is enabled through crate features.  If support for all of these compression
algorithms is not needed, default features can be disabled and specific
compression algorithms can be enabled. For example, to use LZ4 while retaining compatibility with Snappy SSTs:

```toml
[dependencies]
rocksdb = { package = "ckb-rocksdb", version = "1.0", default-features = false, features = ["snappy", "lz4"] }
```

## Platform features

| Feature | Effect |
|---|---|
| `jemalloc` | Linux GNU targets build and statically link `tikv-jemalloc-sys 0.5.4`, sharing CKB's allocator version and unprefixed symbols. Other targets keep their existing allocator. |
| `io-uring` | Linux targets build and statically link the bundled liburing 2.15. Other targets keep their existing I/O implementation. |
| `portable` | Keep the portable CPU baseline. |
| `march-native` | Optimize for the build machine's CPU; do not combine with `portable`. |

Linux feature builds require GNU Make and the target C/C++ toolchain. They do not
require system jemalloc or liburing development packages. Static linking avoids
adding `libjemalloc.so` or `liburing.so` as deployment dependencies. RocksDB's
MultiRead path falls back to ordinary reads when the kernel cannot create an
io-uring queue, including when a container policy denies it. This does not change
ordinary reads into asynchronous I/O or promise a performance improvement.

The jemalloc version is intentionally shared with CKB. Upgrading that optional
dependency to another release series requires coordinating CKB's allocator and
memory-tracking dependencies to avoid multiple native allocator implementations.

## Versioning and release verification

### Migrating transaction and callback code to 1.0

Create optimistic transactions from an `Arc<OptimisticTransactionDB>`. Each
transaction retains the database, so it may outlive the caller's database handle.
Native transaction calls are serialized. Drop transaction iterators before
writing, and drop both iterators and transaction snapshots before commit,
rollback, or rollback to a savepoint. These operations return an error while a
view they would invalidate is alive; exhausting an iterator does not drop it.
Pinned values retain their bytes across later writes and commits.

CF iterators borrow the CF handle as well as the database or transaction. For
independent read ownership, `OwnedColumnFamily::iterator_opt` retains its CF and
database without borrowing the caller's `Arc`.

Use the snapshot's read and iterator methods for safe snapshot access.
`ReadOptions::set_snapshot` is now unsafe: the snapshot must belong to the same
database and outlive every use of the options, its clones and derived iterators.
Implementing `Handle`/`ConstHandle`, constructing a database with `OpenRaw::build`,
and mutating its raw CF map also require explicit unsafe contracts. Raw pointer
fields in transaction options and prefix transforms are private.

Compaction filters and factories must implement `Send`; the binding serializes
their mutable callbacks. Direct filters and comparators stay alive through
database shutdown, including those installed on CFs created after open.
Prefix transforms must accept keys of any lifetime and cannot retain a borrowed
key beyond the callback.

### Release checks

`ckb-rocksdb 1.x` preserves the public Rust API under semantic versioning.
The native crate has its own version, matching the bundled RocksDB engine:
`ckb-librocksdb-sys 11.8.1`. The wrapper pins that native dependency exactly.
Engine upgrades must document any storage-format or behavioral restrictions;
Rust API compatibility alone does not guarantee downgrade compatibility.
The minimum supported Rust version is 1.95.0.

The workspace lockfile fixes the dependencies used by CI. Release checks build
both packages from their packaged sources, including all bundled codecs:

```sh
cargo test --locked --workspace --features portable
make clippy
sh ci/publish-dry-run.sh
```

The script verifies all default codecs, `portable`, and the Linux
`jemalloc`/`io-uring` features using a fresh Cargo cache and target directory,
then removes both on exit. It requires network access to download dependencies.
To check uncommitted changes, pass `--allow-dirty`.

The dry run validates both packaged crates without uploading. Publish the native
crate before the wrapper, or use Cargo's workspace publishing to order them.
The release archives include their source, headers, and license notices;
installation from crates.io does not require Git submodules.
