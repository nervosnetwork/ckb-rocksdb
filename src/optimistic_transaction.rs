use crate::ffi;
use crate::transaction_state::TransactionState;
use crate::{
    ColumnFamily, DBPinnableSlice, DBRawIterator, DBVector, Error, ReadOptions,
    handle::{ConstHandle, Handle},
    ops::*,
};
use libc::c_void;
use std::sync::Arc;

/// An owned transaction that keeps its database alive and serializes native access.
///
/// Writes return an error while a transaction iterator is alive. Commit, rollback
/// and rollback to a savepoint also require transaction snapshots to be dropped.
pub struct OptimisticTransaction {
    inner: Arc<TransactionState>,
    _db: Arc<crate::OptimisticTransactionDB>,
}

impl OptimisticTransaction {
    pub(crate) fn new(
        inner: *mut ffi::rocksdb_transaction_t,
        db: Arc<crate::OptimisticTransactionDB>,
    ) -> OptimisticTransaction {
        OptimisticTransaction {
            inner: TransactionState::new(inner),
            _db: db,
        }
    }

    /// commits a transaction
    pub fn commit(&self) -> Result<(), Error> {
        self.inner.commit()
    }

    /// Transaction rollback
    pub fn rollback(&self) -> Result<(), Error> {
        self.inner.rollback()
    }

    /// Transaction rollback to savepoint
    pub fn rollback_to_savepoint(&self) -> Result<(), Error> {
        self.inner.rollback_to_savepoint()
    }

    /// Set savepoint for transaction
    pub fn set_savepoint(&self) {
        self.inner.set_savepoint()
    }

    /// Get Snapshot
    pub fn snapshot(&self) -> OptimisticTransactionSnapshot<'_> {
        let mut _state = self.inner.lock();
        _state.snapshots += 1;
        unsafe {
            let snapshot = ffi::rocksdb_transaction_get_snapshot(self.inner.raw);
            OptimisticTransactionSnapshot {
                txn: self,
                inner: snapshot,
            }
        }
    }

    /// Get For Update
    /// ReadOptions: Default
    /// exclusive: true
    pub fn get_for_update<K: AsRef<[u8]>>(&self, key: K) -> Result<Option<DBVector>, Error> {
        let opt = ReadOptions::default();
        self.get_for_update_opt(key, &opt, true)
    }

    /// Get For Update with custom ReadOptions and exclusive
    pub fn get_for_update_opt<K: AsRef<[u8]>>(
        &self,
        key: K,
        readopts: &ReadOptions,
        exclusive: bool,
    ) -> Result<Option<DBVector>, Error> {
        self.inner.get_for_update(None, &key, readopts, exclusive)
    }

    pub fn get_for_update_cf<K: AsRef<[u8]>>(
        &self,
        cf: &ColumnFamily,
        key: K,
    ) -> Result<Option<DBVector>, Error> {
        let opt = ReadOptions::default();
        self.get_for_update_cf_opt(cf, key, &opt, true)
    }

    pub fn get_for_update_cf_opt<K: AsRef<[u8]>>(
        &self,
        cf: &ColumnFamily,
        key: K,
        readopts: &ReadOptions,
        exclusive: bool,
    ) -> Result<Option<DBVector>, Error> {
        self.inner
            .get_for_update(Some(cf), &key, readopts, exclusive)
    }
}

unsafe impl Handle<ffi::rocksdb_transaction_t> for OptimisticTransaction {
    fn handle(&self) -> *mut ffi::rocksdb_transaction_t {
        self.inner.raw
    }
}

impl Read for OptimisticTransaction {}

impl GetCF<ReadOptions> for OptimisticTransaction {
    fn get_cf_full<K: AsRef<[u8]>>(
        &self,
        cf: Option<&ColumnFamily>,
        key: K,
        readopts: Option<&ReadOptions>,
    ) -> Result<Option<DBVector>, Error> {
        self.inner.get(cf, &key, readopts)
    }
}

impl MultiGet<ReadOptions> for OptimisticTransaction {
    fn multi_get_full<K, I>(
        &self,
        keys: I,
        readopts: Option<&ReadOptions>,
    ) -> Vec<Result<Option<DBVector>, Error>>
    where
        K: AsRef<[u8]>,
        I: IntoIterator<Item = K>,
    {
        self.inner.multi_get(keys, readopts)
    }
}

impl MultiGetCF<ReadOptions> for OptimisticTransaction {
    fn multi_get_cf_full<'a, K, I>(
        &self,
        keys: I,
        readopts: Option<&ReadOptions>,
    ) -> Vec<Result<Option<DBVector>, Error>>
    where
        K: AsRef<[u8]>,
        I: IntoIterator<Item = (&'a ColumnFamily, K)>,
    {
        self.inner.multi_get_cf(keys, readopts)
    }
}

impl Iterate for OptimisticTransaction {
    fn get_raw_iter<'a: 'b, 'b>(&'a self, readopts: &ReadOptions) -> DBRawIterator<'b> {
        unsafe {
            self.inner.iterator(readopts, |readopts| {
                ffi::rocksdb_transaction_create_iterator(self.inner.raw, readopts.handle())
            })
        }
    }
}

impl IterateCF for OptimisticTransaction {
    fn get_raw_iter_cf<'a: 'b, 'b>(
        &'a self,
        cf_handle: &'b ColumnFamily,
        readopts: &ReadOptions,
    ) -> Result<DBRawIterator<'b>, Error> {
        unsafe {
            Ok(self.inner.iterator(readopts, |readopts| {
                ffi::rocksdb_transaction_create_iterator_cf(
                    self.inner.raw,
                    readopts.handle(),
                    cf_handle.inner,
                )
            }))
        }
    }
}

impl PutCF<()> for OptimisticTransaction {
    fn put_cf_full<K, V>(
        &self,
        cf: Option<&ColumnFamily>,
        key: K,
        value: V,
        _: Option<&()>,
    ) -> Result<(), Error>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        self.inner.put(cf, &key, &value)
    }
}

impl MergeCF<()> for OptimisticTransaction {
    fn merge_cf_full<K, V>(
        &self,
        cf: Option<&ColumnFamily>,
        key: K,
        value: V,
        _: Option<&()>,
    ) -> Result<(), Error>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        self.inner.merge(cf, &key, &value)
    }
}

impl DeleteCF<()> for OptimisticTransaction {
    fn delete_cf_full<K>(
        &self,
        cf: Option<&ColumnFamily>,
        key: K,
        _: Option<&()>,
    ) -> Result<(), Error>
    where
        K: AsRef<[u8]>,
    {
        self.inner.delete(cf, &key)
    }
}

pub struct OptimisticTransactionSnapshot<'a> {
    txn: &'a OptimisticTransaction,
    inner: *const ffi::rocksdb_snapshot_t,
}

unsafe impl Send for OptimisticTransactionSnapshot<'_> {}
unsafe impl Sync for OptimisticTransactionSnapshot<'_> {}

unsafe impl ConstHandle<ffi::rocksdb_snapshot_t> for OptimisticTransactionSnapshot<'_> {
    fn const_handle(&self) -> *const ffi::rocksdb_snapshot_t {
        self.inner
    }
}

impl Read for OptimisticTransactionSnapshot<'_> {}

impl GetCF<ReadOptions> for OptimisticTransactionSnapshot<'_> {
    fn get_cf_full<K: AsRef<[u8]>>(
        &self,
        cf: Option<&ColumnFamily>,
        key: K,
        readopts: Option<&ReadOptions>,
    ) -> Result<Option<DBVector>, Error> {
        let mut ro = readopts.cloned().unwrap_or_default();
        // The adapter and its returned reads borrow this same snapshot.
        unsafe { ro.set_snapshot(self) };
        self.txn.get_cf_full(cf, key, Some(&ro))
    }
}

impl MultiGet<ReadOptions> for OptimisticTransactionSnapshot<'_> {
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
        // The adapter and its returned reads borrow this same snapshot.
        unsafe { ro.set_snapshot(self) };
        self.txn.multi_get_full(keys, Some(&ro))
    }
}

impl MultiGetCF<ReadOptions> for OptimisticTransactionSnapshot<'_> {
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
        // The adapter and its returned reads borrow this same snapshot.
        unsafe { ro.set_snapshot(self) };
        self.txn.multi_get_cf_full(keys, Some(&ro))
    }
}

impl Drop for OptimisticTransactionSnapshot<'_> {
    fn drop(&mut self) {
        unsafe {
            let mut views = self.txn.inner.lock();
            ffi::rocksdb_free(self.inner as *mut c_void);
            views.snapshots -= 1;
        }
    }
}

impl Iterate for OptimisticTransactionSnapshot<'_> {
    fn get_raw_iter<'a: 'b, 'b>(&'a self, readopts: &ReadOptions) -> DBRawIterator<'b> {
        let mut readopts = readopts.to_owned();
        // The adapter and its returned reads borrow this same snapshot.
        unsafe { readopts.set_snapshot(self) };
        self.txn.get_raw_iter(&readopts)
    }
}

impl IterateCF for OptimisticTransactionSnapshot<'_> {
    fn get_raw_iter_cf<'a: 'b, 'b>(
        &'a self,
        cf_handle: &'b ColumnFamily,
        readopts: &ReadOptions,
    ) -> Result<DBRawIterator<'b>, Error> {
        let mut readopts = readopts.to_owned();
        // The adapter and its returned reads borrow this same snapshot.
        unsafe { readopts.set_snapshot(self) };
        self.txn.get_raw_iter_cf(cf_handle, &readopts)
    }
}

impl<'a> GetPinnedCF<'a> for OptimisticTransaction {
    type ColumnFamily = &'a ColumnFamily;
    type ReadOptions = &'a ReadOptions;

    fn get_pinned_cf_full<K: AsRef<[u8]>>(
        &'a self,
        cf: Option<Self::ColumnFamily>,
        key: K,
        readopts: Option<Self::ReadOptions>,
    ) -> Result<Option<DBPinnableSlice<'a>>, Error> {
        self.inner.get_pinned(cf, &key, readopts)
    }
}

impl<'a> GetPinnedCF<'a> for OptimisticTransactionSnapshot<'a> {
    type ColumnFamily = &'a ColumnFamily;
    type ReadOptions = &'a ReadOptions;

    fn get_pinned_cf_full<K: AsRef<[u8]>>(
        &'a self,
        cf: Option<Self::ColumnFamily>,
        key: K,
        readopts: Option<Self::ReadOptions>,
    ) -> ::std::result::Result<Option<DBPinnableSlice<'a>>, Error> {
        let mut ro = readopts.cloned().unwrap_or_default();
        // The adapter and its returned reads borrow this same snapshot.
        unsafe { ro.set_snapshot(self) };

        self.txn.inner.get_pinned(cf, &key, Some(&ro))
    }
}
