// Copyright 2019 Tyler Neely
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

use crate::{
    ColumnFamily, DB, DBRawIterator, DBVector, Error, ReadOptions, handle::ConstHandle, ops::*,
};

/// A consistent view of the database at the point of creation.
///
/// ```
/// use ckb_rocksdb::{prelude::*, IteratorMode};
/// # use ckb_rocksdb::TemporaryDBPath;
///
/// let path = "_path_for_rocksdb_storage3";
/// # let path = TemporaryDBPath::new();
/// # {
///
///     let db = DB::open_default(&path).unwrap();
///     let snapshot = db.snapshot(); // Creates a longer-term snapshot of the DB, but closed when goes out of scope
///     let mut iter = snapshot.iterator(IteratorMode::Start); // Make as many iterators as you'd like from one snapshot

/// # }
/// ```
///
pub struct Snapshot<'a> {
    pub(crate) db: &'a DB,
    pub(crate) inner: *const ffi::rocksdb_snapshot_t,
}

impl ConstHandle<ffi::rocksdb_snapshot_t> for Snapshot<'_> {
    fn const_handle(&self) -> *const ffi::rocksdb_snapshot_t {
        self.inner
    }
}

impl Read for Snapshot<'_> {}

impl GetCF<ReadOptions> for Snapshot<'_> {
    fn get_cf_full<K: AsRef<[u8]>>(
        &self,
        cf: Option<&ColumnFamily>,
        key: K,
        readopts: Option<&ReadOptions>,
    ) -> Result<Option<DBVector>, Error> {
        let mut ro = readopts.cloned().unwrap_or_default();
        ro.set_snapshot(self);

        self.db.get_cf_full(cf, key, Some(&ro))
    }
}

impl MultiGet<ReadOptions> for Snapshot<'_> {
    fn multi_get_full<K, I>(
        &self,
        keys: I,
        readopts: Option<&ReadOptions>,
    ) -> Vec<Result<Option<DBVector>, Error>>
    where
        K: AsRef<[u8]>,
        I: IntoIterator<Item = K>,
    {
        let mut ro = readopts.cloned().unwrap_or_default();
        ro.set_snapshot(self);

        self.db.multi_get_full(keys, Some(&ro))
    }
}

impl MultiGetCF<ReadOptions> for Snapshot<'_> {
    fn multi_get_cf_full<'m, K, I>(
        &self,
        keys: I,
        readopts: Option<&ReadOptions>,
    ) -> Vec<Result<Option<DBVector>, Error>>
    where
        K: AsRef<[u8]>,
        I: IntoIterator<Item = (&'m ColumnFamily, K)>,
    {
        let mut ro = readopts.cloned().unwrap_or_default();
        ro.set_snapshot(self);

        self.db.multi_get_cf_full(keys, Some(&ro))
    }
}

impl Drop for Snapshot<'_> {
    fn drop(&mut self) {
        unsafe {
            ffi::rocksdb_release_snapshot(self.db.inner, self.inner);
        }
    }
}

impl Iterate for Snapshot<'_> {
    fn get_raw_iter<'a: 'b, 'b>(&'a self, readopts: &ReadOptions) -> DBRawIterator<'b> {
        let mut ro = readopts.to_owned();
        ro.set_snapshot(self);
        self.db.get_raw_iter(&ro)
    }
}

impl IterateCF for Snapshot<'_> {
    fn get_raw_iter_cf<'a: 'b, 'b>(
        &'a self,
        cf_handle: &ColumnFamily,
        readopts: &ReadOptions,
    ) -> Result<DBRawIterator<'b>, Error> {
        let mut ro = readopts.to_owned();
        ro.set_snapshot(self);
        self.db.get_raw_iter_cf(cf_handle, &ro)
    }
}
