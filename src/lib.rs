// Copyright 2014 Tyler Neely
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

//! Rust wrapper for RocksDB.
//!
//! # Examples
//!
//! ```
//! use ckb_rocksdb::prelude::*;
//! # use ckb_rocksdb::TemporaryDBPath;
//! // NB: db is automatically closed at end of lifetime
//!
//! let path = "_path_for_rocksdb_storage";
//! # let path = TemporaryDBPath::new();
//! # {
//!
//! let db = DB::open_default(&path).unwrap();
//! db.put(b"my key", b"my value").unwrap();
//! match db.get(b"my key") {
//!     Ok(Some(value)) => println!("retrieved value {}", value.to_utf8().unwrap()),
//!     Ok(None) => println!("value not found"),
//!     Err(e) => println!("operational problem encountered: {}", e),
//! }
//! db.delete(b"my key").unwrap();

//! # }
//! ```
//!
//! Opening a database and a single column family with custom options:
//!
//! ```
//! use ckb_rocksdb::{prelude::*, ColumnFamilyDescriptor};
//! # use ckb_rocksdb::TemporaryDBPath;
//!
//! let path = "_path_for_rocksdb_storage_with_cfs";
//! # let path = TemporaryDBPath::new();
//!
//! let mut cf_opts = Options::default();
//! cf_opts.set_max_write_buffer_number(16);
//! let cf = ColumnFamilyDescriptor::new("cf1", cf_opts);
//!
//! let mut db_opts = Options::default();
//! db_opts.create_missing_column_families(true);
//! db_opts.create_if_missing(true);
//! # {
//! let db = DB::open_cf_descriptors(&db_opts, &path, vec![cf]).unwrap();
//! # }
//! ```
//!

pub extern crate librocksdb_sys as ffi;

#[macro_use]
pub mod ffi_util;
mod util;

pub mod backup;
pub mod checkpoint;
pub mod column_family;
pub mod compaction_filter;
pub mod compaction_filter_factory;
mod comparator;
mod db;
mod db_iterator;
mod db_options;
mod db_pinnable_slice;
mod db_vector;
mod db_with_ttl;
mod handle;
pub mod merge_operator;
mod open_raw;
pub mod ops;
mod optimistic_transaction;
mod optimistic_transaction_db;
mod options;
mod owned_column_family;
mod read_only_db;
mod secondary_db;
mod slice_transform;
mod snapshot;
mod sst_file_writer;
mod transaction;
mod transaction_db;
mod transaction_state;
mod write_batch;

pub mod prelude;

pub use crate::column_family::ColumnFamilyDescriptor;
pub use crate::compaction_filter::Decision as CompactionDecision;
pub use crate::db::DB;
pub use crate::db_iterator::{DBIterator, DBRawIterator, Direction, IteratorMode};
pub use crate::db_options::{
    BlockBasedIndexType, BlockBasedOptions, BottommostLevelCompaction, Cache, CompactOptions,
    CuckooTableOptions, DBCompactionStyle, DBCompressionType, DBPath, DBRecoveryMode,
    DataBlockIndexType, Env, FifoCompactOptions, FlushOptions, IngestExternalFileOptions,
    KeyEncodingType, LogLevel, MemtableFactory, Options, PlainTableFactoryOptions, ReadOptions,
    UniversalCompactOptions, UniversalCompactionStopStyle, WriteOptions,
};
pub use crate::db_pinnable_slice::DBPinnableSlice;
pub use crate::db_vector::DBVector;
pub use crate::db_with_ttl::{DBWithTTL, TTLOpenDescriptor};
pub use crate::handle::{ConstHandle, Handle};
pub use crate::options::FullOptions;
pub use crate::owned_column_family::OwnedColumnFamily;
pub use crate::read_only_db::ReadOnlyDB;
pub use crate::secondary_db::{SecondaryDB, SecondaryOpenDescriptor};
pub use crate::slice_transform::SliceTransform;
pub use crate::snapshot::Snapshot;
pub use crate::sst_file_writer::SstFileWriter;
pub use crate::util::TemporaryDBPath;
pub use crate::write_batch::WriteBatch;

pub use crate::merge_operator::MergeOperands;
use std::error;
use std::fmt;

pub use crate::optimistic_transaction::{OptimisticTransaction, OptimisticTransactionSnapshot};
pub use crate::optimistic_transaction_db::{OptimisticTransactionDB, OptimisticTransactionOptions};
pub use crate::transaction::{Transaction, TransactionSnapshot};
pub use crate::transaction_db::{TransactionDB, TransactionDBOptions, TransactionOptions};

/// A simple wrapper round a string, used for errors reported from
/// ffi calls.
#[derive(Debug, Clone, PartialEq)]
pub struct Error {
    message: String,
}

impl Error {
    pub fn new(message: String) -> Error {
        Error { message }
    }

    pub fn into_string(self) -> String {
        self.into()
    }
}

impl AsRef<str> for Error {
    fn as_ref(&self) -> &str {
        &self.message
    }
}

impl From<Error> for String {
    fn from(e: Error) -> String {
        e.message
    }
}

impl error::Error for Error {
    fn description(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        self.message.fmt(formatter)
    }
}

/// An opaque type used to represent a column family. Returned from some functions, and used
/// in others
pub struct ColumnFamily {
    inner: *mut ffi::rocksdb_column_family_handle_t,
}

unsafe impl Send for ColumnFamily {}
