//! Native operations shared by owned and borrowed transactions.
//!
//! The public wrappers own or borrow their DB. This boundary owns serialization,
//! view invalidation checks and native result ownership. Key conversions and
//! iterator materialization finish before taking the transaction lock. Single
//! keys and values stay borrowed from their public caller until the operation ends.
use crate::{
    ColumnFamily, DBPinnableSlice, DBRawIterator, DBVector, Error, ReadOptions, ffi, ffi_util,
    handle::Handle,
    ops::{MultiGetKeys, MultiGetResults},
};
use libc::{c_char, c_uchar, size_t};
use std::sync::{Arc, Mutex, MutexGuard};

#[derive(Default)]
pub(crate) struct Views {
    pub iterators: usize,
    pub snapshots: usize,
}

pub(crate) struct TransactionState {
    pub raw: *mut ffi::rocksdb_transaction_t,
    views: Mutex<Views>,
}

// Every native transaction/iterator call uses the same mutex. Views prevent
// writes or resets from invalidating bytes exposed after the lock is released.
unsafe impl Send for TransactionState {}
unsafe impl Sync for TransactionState {}

impl TransactionState {
    pub fn new(raw: *mut ffi::rocksdb_transaction_t) -> Arc<Self> {
        Arc::new(Self {
            raw,
            views: Mutex::new(Views::default()),
        })
    }

    pub fn lock(&self) -> MutexGuard<'_, Views> {
        // Convert caller-provided keys/iterators before locking: their code may
        // read this transaction or advance one of its iterators.
        self.views.lock().expect("transaction lock poisoned")
    }

    fn write(&self) -> Result<MutexGuard<'_, Views>, Error> {
        let views = self.lock();
        if views.iterators != 0 {
            return Err(Error::new("transaction has active iterators".to_owned()));
        }
        Ok(views)
    }

    fn reset(&self) -> Result<MutexGuard<'_, Views>, Error> {
        let views = self.write()?;
        if views.snapshots != 0 {
            return Err(Error::new("transaction has active snapshots".to_owned()));
        }
        Ok(views)
    }

    pub fn iterator<'a>(
        self: &'a Arc<Self>,
        options: &ReadOptions,
        create: impl FnOnce(&ReadOptions) -> *mut ffi::rocksdb_iterator_t,
    ) -> DBRawIterator<'a> {
        let mut views = self.lock();
        let mut iterator = DBRawIterator::new(options, create);
        views.iterators += 1;
        iterator.transaction = Some(Arc::clone(self));
        iterator
    }

    pub fn commit(&self) -> Result<(), Error> {
        let _state = self.reset()?;
        unsafe {
            ffi_try!(ffi::rocksdb_transaction_commit(self.raw,));
        }
        Ok(())
    }

    pub fn rollback(&self) -> Result<(), Error> {
        let _state = self.reset()?;
        unsafe { ffi_try!(ffi::rocksdb_transaction_rollback(self.raw,)) }
        Ok(())
    }

    pub fn rollback_to_savepoint(&self) -> Result<(), Error> {
        let _state = self.reset()?;
        unsafe { ffi_try!(ffi::rocksdb_transaction_rollback_to_savepoint(self.raw,)) }
        Ok(())
    }

    pub fn set_savepoint(&self) {
        let _state = self.lock();
        unsafe { ffi::rocksdb_transaction_set_savepoint(self.raw) }
    }

    pub fn get_for_update<K: AsRef<[u8]>>(
        &self,
        cf: Option<&ColumnFamily>,
        key: &K,
        readopts: &ReadOptions,
        exclusive: bool,
    ) -> Result<Option<DBVector>, Error> {
        let key = key.as_ref();
        let key_ptr = key.as_ptr() as *const c_char;
        let key_len = key.len() as size_t;
        let _state = self.write()?;
        unsafe {
            let mut val_len: size_t = 0;
            let val = match cf {
                Some(cf) => ffi_try!(ffi::rocksdb_transaction_get_for_update_cf(
                    self.raw,
                    readopts.handle(),
                    cf.handle(),
                    key_ptr,
                    key_len,
                    &mut val_len,
                    exclusive as c_uchar,
                )),
                None => ffi_try!(ffi::rocksdb_transaction_get_for_update(
                    self.raw,
                    readopts.handle(),
                    key_ptr,
                    key_len,
                    &mut val_len,
                    exclusive as c_uchar,
                )),
            } as *mut u8;
            if val.is_null() {
                Ok(None)
            } else {
                Ok(Some(DBVector::from_c(val, val_len)))
            }
        }
    }

    pub fn get<K: AsRef<[u8]>>(
        &self,
        cf: Option<&ColumnFamily>,
        key: &K,
        readopts: Option<&ReadOptions>,
    ) -> Result<Option<DBVector>, Error> {
        let mut default_readopts = None;

        let ro_handle = ReadOptions::input_or_default(readopts, &mut default_readopts)?;

        let key = key.as_ref();
        let key_ptr = key.as_ptr() as *const c_char;
        let key_len = key.len() as size_t;

        let _state = self.lock();
        unsafe {
            let mut val_len: size_t = 0;

            let val = match cf {
                Some(cf) => ffi_try!(ffi::rocksdb_transaction_get_cf(
                    self.raw,
                    ro_handle,
                    cf.inner,
                    key_ptr,
                    key_len,
                    &mut val_len,
                )),
                None => ffi_try!(ffi::rocksdb_transaction_get(
                    self.raw,
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

    pub fn multi_get<K, I>(
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
                return keys.into_iter().map(|_| Err(e.clone())).collect();
            }
        };

        let keys = MultiGetKeys::new(keys);
        let mut results = MultiGetResults::new(keys.len());
        let _state = self.lock();
        unsafe {
            ffi::rocksdb_transaction_multi_get(
                self.raw,
                ro_handle,
                keys.len(),
                keys.pointers.as_ptr(),
                keys.sizes.as_ptr(),
                results.values.as_mut_ptr(),
                results.sizes.as_mut_ptr(),
                results.errors.as_mut_ptr(),
            );
        }

        results.into_values()
    }

    pub fn multi_get_cf<'a, K, I>(
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
                return keys.into_iter().map(|_| Err(e.clone())).collect();
            }
        };

        let (columns, keys) = MultiGetKeys::with_column_families(keys);
        let mut results = MultiGetResults::new(keys.len());
        let _state = self.lock();
        unsafe {
            ffi::rocksdb_transaction_multi_get_cf(
                self.raw,
                ro_handle,
                columns.as_ptr(),
                keys.len(),
                keys.pointers.as_ptr(),
                keys.sizes.as_ptr(),
                results.values.as_mut_ptr(),
                results.sizes.as_mut_ptr(),
                results.errors.as_mut_ptr(),
            );
        }

        results.into_values()
    }

    pub fn put<K, V>(&self, cf: Option<&ColumnFamily>, key: &K, value: &V) -> Result<(), Error>
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

        let _state = self.write()?;
        unsafe {
            match cf {
                Some(cf) => ffi_try!(ffi::rocksdb_transaction_put_cf(
                    self.raw,
                    cf.handle(),
                    key_ptr,
                    key_len,
                    val_ptr,
                    val_len,
                )),
                None => ffi_try!(ffi::rocksdb_transaction_put(
                    self.raw, key_ptr, key_len, val_ptr, val_len,
                )),
            }

            Ok(())
        }
    }

    pub fn merge<K, V>(&self, cf: Option<&ColumnFamily>, key: &K, value: &V) -> Result<(), Error>
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

        let _state = self.write()?;
        unsafe {
            match cf {
                Some(cf) => ffi_try!(ffi::rocksdb_transaction_merge_cf(
                    self.raw,
                    cf.handle(),
                    key_ptr,
                    key_len,
                    val_ptr,
                    val_len,
                )),
                None => ffi_try!(ffi::rocksdb_transaction_merge(
                    self.raw, key_ptr, key_len, val_ptr, val_len,
                )),
            }

            Ok(())
        }
    }

    pub fn delete<K>(&self, cf: Option<&ColumnFamily>, key: &K) -> Result<(), Error>
    where
        K: AsRef<[u8]>,
    {
        let key = key.as_ref();
        let key_ptr = key.as_ptr() as *const c_char;
        let key_len = key.len() as size_t;

        let _state = self.write()?;
        unsafe {
            match cf {
                Some(cf) => ffi_try!(ffi::rocksdb_transaction_delete_cf(
                    self.raw, cf.inner, key_ptr, key_len,
                )),
                None => ffi_try!(ffi::rocksdb_transaction_delete(self.raw, key_ptr, key_len,)),
            }

            Ok(())
        }
    }

    pub fn get_pinned<'a, K: AsRef<[u8]>>(
        &'a self,
        cf: Option<&ColumnFamily>,
        key: &K,
        readopts: Option<&ReadOptions>,
    ) -> Result<Option<DBPinnableSlice<'a>>, Error> {
        let mut default_readopts = None;

        let ro_handle = ReadOptions::input_or_default(readopts, &mut default_readopts)?;

        let key = key.as_ref();
        let key_ptr = key.as_ptr() as *const c_char;
        let key_len = key.len() as size_t;

        let _state = self.lock();
        unsafe {
            let mut err: *mut ::libc::c_char = ::std::ptr::null_mut();
            let val = match cf {
                Some(cf) => ffi::rocksdb_transaction_get_pinned_cf(
                    self.raw,
                    ro_handle,
                    cf.handle(),
                    key_ptr,
                    key_len,
                    &mut err,
                ),
                None => ffi::rocksdb_transaction_get_pinned(
                    self.raw, ro_handle, key_ptr, key_len, &mut err,
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

impl Drop for TransactionState {
    fn drop(&mut self) {
        // All iterator owners and transaction snapshots have already released.
        unsafe { ffi::rocksdb_transaction_destroy(self.raw) };
    }
}
