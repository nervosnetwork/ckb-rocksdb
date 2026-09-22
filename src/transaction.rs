use crate::ffi;
use crate::transaction_state::TransactionState;
use crate::{
    ColumnFamily, DBPinnableSlice, DBRawIterator, DBVector, Error, ReadOptions,
    handle::{ConstHandle, Handle},
    ops::*,
};
use libc::c_void;
use std::marker::PhantomData;
use std::sync::Arc;

/// A transaction borrowing its database, with serialized native access.
///
/// Drop iterators before writes and drop both iterators and transaction snapshots
/// before commit, rollback or rollback to a savepoint. Invalidating an active view
/// returns an error without changing the transaction.
pub struct Transaction<'a, T> {
    inner: Arc<TransactionState>,
    db: PhantomData<&'a T>,
}

impl<'a, T> Transaction<'a, T> {
    pub(crate) fn new(inner: *mut ffi::rocksdb_transaction_t) -> Transaction<'a, T> {
        Transaction {
            inner: TransactionState::new(inner),
            db: PhantomData,
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
    pub fn snapshot(&'a self) -> TransactionSnapshot<'a, T> {
        let mut _state = self.inner.lock();
        _state.snapshots += 1;
        unsafe {
            let snapshot = ffi::rocksdb_transaction_get_snapshot(self.inner.raw);
            TransactionSnapshot {
                inner: snapshot,
                db: self,
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

unsafe impl<T> Handle<ffi::rocksdb_transaction_t> for Transaction<'_, T> {
    fn handle(&self) -> *mut ffi::rocksdb_transaction_t {
        self.inner.raw
    }
}

impl<T> Read for Transaction<'_, T> {}

impl<'a, T> GetCF<ReadOptions> for Transaction<'a, T>
where
    Transaction<'a, T>: Handle<ffi::rocksdb_transaction_t> + Read,
{
    fn get_cf_full<K: AsRef<[u8]>>(
        &self,
        cf: Option<&ColumnFamily>,
        key: K,
        readopts: Option<&ReadOptions>,
    ) -> Result<Option<DBVector>, Error> {
        self.inner.get(cf, &key, readopts)
    }
}

impl<'a, T> MultiGet<ReadOptions> for Transaction<'a, T>
where
    Transaction<'a, T>: Handle<ffi::rocksdb_transaction_t> + Read,
{
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

impl<'a, T> MultiGetCF<ReadOptions> for Transaction<'a, T>
where
    Transaction<'a, T>: Handle<ffi::rocksdb_transaction_t> + Read,
{
    fn multi_get_cf_full<'m, K, I>(
        &self,
        keys: I,
        readopts: Option<&ReadOptions>,
    ) -> Vec<Result<Option<DBVector>, Error>>
    where
        K: AsRef<[u8]>,
        I: IntoIterator<Item = (&'m ColumnFamily, K)>,
    {
        self.inner.multi_get_cf(keys, readopts)
    }
}

impl<T> Iterate for Transaction<'_, T> {
    fn get_raw_iter<'a: 'b, 'b>(&'a self, readopts: &ReadOptions) -> DBRawIterator<'b> {
        unsafe {
            self.inner.iterator(readopts, |readopts| {
                ffi::rocksdb_transaction_create_iterator(self.inner.raw, readopts.handle())
            })
        }
    }
}

impl<T> IterateCF for Transaction<'_, T> {
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

pub struct TransactionSnapshot<'a, T> {
    db: &'a Transaction<'a, T>,
    inner: *const ffi::rocksdb_snapshot_t,
}

unsafe impl<T> ConstHandle<ffi::rocksdb_snapshot_t> for TransactionSnapshot<'_, T> {
    fn const_handle(&self) -> *const ffi::rocksdb_snapshot_t {
        self.inner
    }
}

impl<T> Read for TransactionSnapshot<'_, T> {}

impl<'a, T> GetCF<ReadOptions> for TransactionSnapshot<'a, T>
where
    Transaction<'a, T>: GetCF<ReadOptions>,
{
    fn get_cf_full<K: AsRef<[u8]>>(
        &self,
        cf: Option<&ColumnFamily>,
        key: K,
        readopts: Option<&ReadOptions>,
    ) -> Result<Option<DBVector>, Error> {
        let mut ro = readopts.cloned().unwrap_or_default();
        // The adapter and its returned reads borrow this same snapshot.
        unsafe { ro.set_snapshot(self) };
        self.db.get_cf_full(cf, key, Some(&ro))
    }
}

impl<'a, T> MultiGet<ReadOptions> for TransactionSnapshot<'a, T>
where
    Transaction<'a, T>: MultiGet<ReadOptions>,
{
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
        self.db.multi_get_full(keys, Some(&ro))
    }
}

impl<'a, T> MultiGetCF<ReadOptions> for TransactionSnapshot<'a, T>
where
    Transaction<'a, T>: MultiGet<ReadOptions>,
{
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
        self.db.multi_get_cf_full(keys, Some(&ro))
    }
}

impl<T> PutCF<()> for Transaction<'_, T> {
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

impl<T> MergeCF<()> for Transaction<'_, T> {
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

impl<T> DeleteCF<()> for Transaction<'_, T> {
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

impl<T> Drop for TransactionSnapshot<'_, T> {
    fn drop(&mut self) {
        unsafe {
            let mut views = self.db.inner.lock();
            ffi::rocksdb_free(self.inner as *mut c_void);
            views.snapshots -= 1;
        }
    }
}

impl<T: Iterate> Iterate for TransactionSnapshot<'_, T> {
    fn get_raw_iter<'a: 'b, 'b>(&'a self, readopts: &ReadOptions) -> DBRawIterator<'b> {
        let mut readopts = readopts.to_owned();
        // The adapter and its returned reads borrow this same snapshot.
        unsafe { readopts.set_snapshot(self) };
        self.db.get_raw_iter(&readopts)
    }
}

impl<T: IterateCF> IterateCF for TransactionSnapshot<'_, T> {
    fn get_raw_iter_cf<'a: 'b, 'b>(
        &'a self,
        cf_handle: &'b ColumnFamily,
        readopts: &ReadOptions,
    ) -> Result<DBRawIterator<'b>, Error> {
        let mut readopts = readopts.to_owned();
        // The adapter and its returned reads borrow this same snapshot.
        unsafe { readopts.set_snapshot(self) };
        self.db.get_raw_iter_cf(cf_handle, &readopts)
    }
}

impl<'a, T> GetPinnedCF<'a> for Transaction<'a, T> {
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

impl<'a, T> GetPinnedCF<'a> for TransactionSnapshot<'a, T> {
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

        self.db.inner.get_pinned(cf, &key, Some(&ro))
    }
}
