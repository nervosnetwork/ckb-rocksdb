use crate::ffi;
use crate::{
    ColumnFamily, DBPinnableSlice, DBRawIterator, DBVector, Error, ReadOptions, ffi_util,
    handle::{ConstHandle, Handle},
    ops::*,
};
use libc::{c_char, c_uchar, c_void, size_t};
use std::marker::PhantomData;
use std::ptr;

pub struct OptimisticTransaction {
    inner: *mut ffi::rocksdb_transaction_t,
}

unsafe impl Send for OptimisticTransaction {}
unsafe impl Sync for OptimisticTransaction {}

impl OptimisticTransaction {
    pub(crate) fn new(inner: *mut ffi::rocksdb_transaction_t) -> OptimisticTransaction {
        OptimisticTransaction { inner }
    }

    /// commits a transaction
    pub fn commit(&self) -> Result<(), Error> {
        unsafe {
            ffi_try!(ffi::rocksdb_transaction_commit(self.inner,));
        }
        Ok(())
    }

    /// Transaction rollback
    pub fn rollback(&self) -> Result<(), Error> {
        unsafe { ffi_try!(ffi::rocksdb_transaction_rollback(self.inner,)) }
        Ok(())
    }

    /// Transaction rollback to savepoint
    pub fn rollback_to_savepoint(&self) -> Result<(), Error> {
        unsafe { ffi_try!(ffi::rocksdb_transaction_rollback_to_savepoint(self.inner,)) }
        Ok(())
    }

    /// Set savepoint for transaction
    pub fn set_savepoint(&self) {
        unsafe { ffi::rocksdb_transaction_set_savepoint(self.inner) }
    }

    /// Get Snapshot
    pub fn snapshot(&self) -> OptimisticTransactionSnapshot<'_> {
        unsafe {
            let snapshot = ffi::rocksdb_transaction_get_snapshot(self.inner);
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
        let key = key.as_ref();
        let key_ptr = key.as_ptr() as *const c_char;
        let key_len = key.len() as size_t;
        unsafe {
            let mut val_len: size_t = 0;
            let val = ffi_try!(ffi::rocksdb_transaction_get_for_update(
                self.handle(),
                readopts.handle(),
                key_ptr,
                key_len,
                &mut val_len,
                exclusive as c_uchar,
            )) as *mut u8;

            if val.is_null() {
                Ok(None)
            } else {
                Ok(Some(DBVector::from_c(val, val_len)))
            }
        }
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
        let key = key.as_ref();
        let key_ptr = key.as_ptr() as *const c_char;
        let key_len = key.len() as size_t;
        unsafe {
            let mut val_len: size_t = 0;
            let val = ffi_try!(ffi::rocksdb_transaction_get_for_update_cf(
                self.handle(),
                readopts.handle(),
                cf.handle(),
                key_ptr,
                key_len,
                &mut val_len,
                exclusive as c_uchar,
            )) as *mut u8;

            if val.is_null() {
                Ok(None)
            } else {
                Ok(Some(DBVector::from_c(val, val_len)))
            }
        }
    }
}

impl Drop for OptimisticTransaction {
    fn drop(&mut self) {
        unsafe {
            ffi::rocksdb_transaction_destroy(self.inner);
        }
    }
}

impl Handle<ffi::rocksdb_transaction_t> for OptimisticTransaction {
    fn handle(&self) -> *mut ffi::rocksdb_transaction_t {
        self.inner
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
        let mut default_readopts = None;

        let ro_handle = ReadOptions::input_or_default(readopts, &mut default_readopts)?;

        let key = key.as_ref();
        let key_ptr = key.as_ptr() as *const c_char;
        let key_len = key.len() as size_t;

        unsafe {
            let mut val_len: size_t = 0;

            let val = match cf {
                Some(cf) => ffi_try!(ffi::rocksdb_transaction_get_cf(
                    self.handle(),
                    ro_handle,
                    cf.inner,
                    key_ptr,
                    key_len,
                    &mut val_len,
                )),
                None => ffi_try!(ffi::rocksdb_transaction_get(
                    self.handle(),
                    ro_handle,
                    key_ptr,
                    key_len,
                    &mut val_len,
                )),
            } as *mut u8;

            if val.is_null() {
                Ok(None)
            } else {
                Ok(Some(DBVector::from_c(val, val_len)))
            }
        }
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
        let mut default_readopts = None;
        let ro_handle = match ReadOptions::input_or_default(readopts, &mut default_readopts) {
            Ok(ro) => ro,
            Err(e) => {
                let key_count = keys.into_iter().count();

                return vec![e; key_count]
                    .iter()
                    .map(|e| Err(e.to_owned()))
                    .collect();
            }
        };

        let (keys, keys_sizes): (Vec<Box<[u8]>>, Vec<_>) = keys
            .into_iter()
            .map(|k| (Box::from(k.as_ref()), k.as_ref().len()))
            .unzip();
        let ptr_keys: Vec<_> = keys.iter().map(|k| k.as_ptr() as *const c_char).collect();

        let mut values = vec![ptr::null_mut(); keys.len()];
        let mut values_sizes = vec![0_usize; keys.len()];
        let mut errors = vec![ptr::null_mut(); keys.len()];
        unsafe {
            ffi::rocksdb_transaction_multi_get(
                self.inner,
                ro_handle,
                ptr_keys.len(),
                ptr_keys.as_ptr(),
                keys_sizes.as_ptr(),
                values.as_mut_ptr(),
                values_sizes.as_mut_ptr(),
                errors.as_mut_ptr(),
            );
        }

        convert_values(values, values_sizes, errors)
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
        let mut default_readopts = None;
        let ro_handle = match ReadOptions::input_or_default(readopts, &mut default_readopts) {
            Ok(ro) => ro,
            Err(e) => {
                let key_count = keys.into_iter().count();

                return vec![e; key_count]
                    .iter()
                    .map(|e| Err(e.to_owned()))
                    .collect();
            }
        };

        let (cfs_and_keys, keys_sizes): (Vec<CFAndKey>, Vec<_>) = keys
            .into_iter()
            .map(|(cf, key)| ((cf, Box::from(key.as_ref())), key.as_ref().len()))
            .unzip();
        let ptr_keys: Vec<_> = cfs_and_keys
            .iter()
            .map(|(_, k)| k.as_ptr() as *const c_char)
            .collect();
        let ptr_cfs: Vec<_> = cfs_and_keys
            .iter()
            .map(|(c, _)| c.inner as *const _)
            .collect();

        let mut values = vec![ptr::null_mut(); ptr_keys.len()];
        let mut values_sizes = vec![0_usize; ptr_keys.len()];
        let mut errors = vec![ptr::null_mut(); ptr_keys.len()];
        unsafe {
            ffi::rocksdb_transaction_multi_get_cf(
                self.inner,
                ro_handle,
                ptr_cfs.as_ptr(),
                ptr_keys.len(),
                ptr_keys.as_ptr(),
                keys_sizes.as_ptr(),
                values.as_mut_ptr(),
                values_sizes.as_mut_ptr(),
                errors.as_mut_ptr(),
            );
        }

        convert_values(values, values_sizes, errors)
    }
}

impl Iterate for OptimisticTransaction {
    fn get_raw_iter<'a: 'b, 'b>(&'a self, readopts: &ReadOptions) -> DBRawIterator<'b> {
        unsafe {
            DBRawIterator {
                inner: ffi::rocksdb_transaction_create_iterator(self.inner, readopts.handle()),
                db: PhantomData,
            }
        }
    }
}

impl IterateCF for OptimisticTransaction {
    fn get_raw_iter_cf<'a: 'b, 'b>(
        &'a self,
        cf_handle: &ColumnFamily,
        readopts: &ReadOptions,
    ) -> Result<DBRawIterator<'b>, Error> {
        unsafe {
            Ok(DBRawIterator {
                inner: ffi::rocksdb_transaction_create_iterator_cf(
                    self.inner,
                    readopts.handle(),
                    cf_handle.inner,
                ),
                db: PhantomData,
            })
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
        let key = key.as_ref();
        let value = value.as_ref();
        let key_ptr = key.as_ptr() as *const c_char;
        let key_len = key.len() as size_t;
        let val_ptr = value.as_ptr() as *const c_char;
        let val_len = value.len() as size_t;

        unsafe {
            match cf {
                Some(cf) => ffi_try!(ffi::rocksdb_transaction_put_cf(
                    self.handle(),
                    cf.handle(),
                    key_ptr,
                    key_len,
                    val_ptr,
                    val_len,
                )),
                None => ffi_try!(ffi::rocksdb_transaction_put(
                    self.handle(),
                    key_ptr,
                    key_len,
                    val_ptr,
                    val_len,
                )),
            }

            Ok(())
        }
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
        let key = key.as_ref();
        let value = value.as_ref();
        let key_ptr = key.as_ptr() as *const c_char;
        let key_len = key.len() as size_t;
        let val_ptr = value.as_ptr() as *const c_char;
        let val_len = value.len() as size_t;

        unsafe {
            match cf {
                Some(cf) => ffi_try!(ffi::rocksdb_transaction_merge_cf(
                    self.handle(),
                    cf.handle(),
                    key_ptr,
                    key_len,
                    val_ptr,
                    val_len,
                )),
                None => ffi_try!(ffi::rocksdb_transaction_merge(
                    self.handle(),
                    key_ptr,
                    key_len,
                    val_ptr,
                    val_len,
                )),
            }

            Ok(())
        }
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
        let key = key.as_ref();
        let key_ptr = key.as_ptr() as *const c_char;
        let key_len = key.len() as size_t;

        unsafe {
            match cf {
                Some(cf) => ffi_try!(ffi::rocksdb_transaction_delete_cf(
                    self.handle(),
                    cf.inner,
                    key_ptr,
                    key_len,
                )),
                None => ffi_try!(ffi::rocksdb_transaction_delete(
                    self.handle(),
                    key_ptr,
                    key_len,
                )),
            }

            Ok(())
        }
    }
}

pub struct OptimisticTransactionSnapshot<'a> {
    txn: &'a OptimisticTransaction,
    inner: *const ffi::rocksdb_snapshot_t,
}

unsafe impl Send for OptimisticTransactionSnapshot<'_> {}
unsafe impl Sync for OptimisticTransactionSnapshot<'_> {}

impl ConstHandle<ffi::rocksdb_snapshot_t> for OptimisticTransactionSnapshot<'_> {
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
        ro.set_snapshot(self);
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
        ro.set_snapshot(self);
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
        ro.set_snapshot(self);
        self.txn.multi_get_cf_full(keys, Some(&ro))
    }
}

impl Drop for OptimisticTransactionSnapshot<'_> {
    fn drop(&mut self) {
        unsafe {
            ffi::rocksdb_free(self.inner as *mut c_void);
        }
    }
}

impl Iterate for OptimisticTransactionSnapshot<'_> {
    fn get_raw_iter<'a: 'b, 'b>(&'a self, readopts: &ReadOptions) -> DBRawIterator<'b> {
        let mut readopts = readopts.to_owned();
        readopts.set_snapshot(self);
        self.txn.get_raw_iter(&readopts)
    }
}

impl IterateCF for OptimisticTransactionSnapshot<'_> {
    fn get_raw_iter_cf<'a: 'b, 'b>(
        &'a self,
        cf_handle: &ColumnFamily,
        readopts: &ReadOptions,
    ) -> Result<DBRawIterator<'b>, Error> {
        let mut readopts = readopts.to_owned();
        readopts.set_snapshot(self);
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
        let mut default_readopts = None;

        let ro_handle = ReadOptions::input_or_default(readopts, &mut default_readopts)?;

        let key = key.as_ref();
        let key_ptr = key.as_ptr() as *const c_char;
        let key_len = key.len() as size_t;

        unsafe {
            let mut err: *mut ::libc::c_char = ::std::ptr::null_mut();
            let val = match cf {
                Some(cf) => ffi::rocksdb_transaction_get_pinned_cf(
                    self.handle(),
                    ro_handle,
                    cf.handle(),
                    key_ptr,
                    key_len,
                    &mut err,
                ),
                None => ffi::rocksdb_transaction_get_pinned(
                    self.handle(),
                    ro_handle,
                    key_ptr,
                    key_len,
                    &mut err,
                ),
            };

            if !err.is_null() {
                return Err(Error::new(ffi_util::error_message(err)));
            }

            if val.is_null() {
                Ok(None)
            } else {
                Ok(Some(DBPinnableSlice::from_c(val)))
            }
        }
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
        ro.set_snapshot(self);

        let key = key.as_ref();
        let key_ptr = key.as_ptr() as *const c_char;
        let key_len = key.len() as size_t;

        unsafe {
            let mut err: *mut ::libc::c_char = ::std::ptr::null_mut();
            let val = match cf {
                Some(cf) => ffi::rocksdb_transaction_get_pinned_cf(
                    self.txn.handle(),
                    ro.handle(),
                    cf.handle(),
                    key_ptr,
                    key_len,
                    &mut err,
                ),
                None => ffi::rocksdb_transaction_get_pinned(
                    self.txn.handle(),
                    ro.handle(),
                    key_ptr,
                    key_len,
                    &mut err,
                ),
            };

            if !err.is_null() {
                return Err(Error::new(ffi_util::error_message(err)));
            }

            if val.is_null() {
                Ok(None)
            } else {
                Ok(Some(DBPinnableSlice::from_c(val)))
            }
        }
    }
}
