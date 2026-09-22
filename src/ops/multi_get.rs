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

use crate::{ColumnFamily, DBPinnableSlice, DBVector, ffi};
use libc::c_char;
use std::ptr;

use crate::{Error, ReadOptions, handle::Handle};

pub type CFAndKey<'a> = (&'a ColumnFamily, Box<[u8]>);

/// Owns the key bytes backing the parallel arrays passed to the C API.
pub(crate) struct MultiGetKeys {
    _keys: Vec<Box<[u8]>>,
    pub(crate) pointers: Vec<*const c_char>,
    pub(crate) sizes: Vec<usize>,
}

impl MultiGetKeys {
    pub(crate) fn new<K: AsRef<[u8]>>(keys: impl IntoIterator<Item = K>) -> Self {
        let (keys, sizes): (Vec<Box<[u8]>>, Vec<_>) = keys
            .into_iter()
            .map(|key| {
                let bytes = Box::<[u8]>::from(key.as_ref());
                let len = bytes.len();
                (bytes, len)
            })
            .unzip();
        let pointers = keys.iter().map(|key| key.as_ptr().cast()).collect();
        Self {
            _keys: keys,
            pointers,
            sizes,
        }
    }

    pub(crate) fn with_column_families<'a, K: AsRef<[u8]>>(
        keys: impl IntoIterator<Item = (&'a ColumnFamily, K)>,
    ) -> (Vec<*const ffi::rocksdb_column_family_handle_t>, Self) {
        let keys = keys.into_iter();
        let mut columns = Vec::with_capacity(keys.size_hint().0);
        let keys = Self::new(keys.map(|(column, key)| {
            columns.push(column.inner as *const _);
            key
        }));
        (columns, keys)
    }

    pub(crate) fn len(&self) -> usize {
        self.pointers.len()
    }
}

/// Each input key has one value, length and error slot, in input order.
pub(crate) struct MultiGetResults {
    pub(crate) values: Vec<*mut c_char>,
    pub(crate) sizes: Vec<usize>,
    pub(crate) errors: Vec<*mut c_char>,
}

impl MultiGetResults {
    pub(crate) fn new(len: usize) -> Self {
        Self {
            values: vec![ptr::null_mut(); len],
            sizes: vec![0; len],
            errors: vec![ptr::null_mut(); len],
        }
    }

    pub(crate) fn into_values(self) -> Vec<Result<Option<DBVector>, Error>> {
        // These equally sized output slots were filled by the native MultiGet.
        unsafe { convert_values(self.values, self.sizes, self.errors) }
    }
}

pub trait MultiGet<R> {
    fn multi_get_full<K, I>(
        &self,
        keys: I,
        readopts: Option<&R>,
    ) -> Vec<Result<Option<DBVector>, Error>>
    where
        K: AsRef<[u8]>,
        I: IntoIterator<Item = K>;

    fn multi_get<K, I>(&self, keys: I) -> Vec<Result<Option<DBVector>, Error>>
    where
        K: AsRef<[u8]>,
        I: IntoIterator<Item = K>,
    {
        self.multi_get_full(keys, None)
    }

    fn multi_get_opt<K, I>(&self, keys: I, readopts: &R) -> Vec<Result<Option<DBVector>, Error>>
    where
        K: AsRef<[u8]>,
        I: IntoIterator<Item = K>,
    {
        self.multi_get_full(keys, Some(readopts))
    }
}

pub trait MultiGetCF<R> {
    fn multi_get_cf_full<'a, K, I>(
        &self,
        keys_cf: I,
        readopts: Option<&R>,
    ) -> Vec<Result<Option<DBVector>, Error>>
    where
        K: AsRef<[u8]>,
        I: IntoIterator<Item = (&'a ColumnFamily, K)>;

    fn multi_get_cf<'a, K, I>(&self, keys_cf: I) -> Vec<Result<Option<DBVector>, Error>>
    where
        K: AsRef<[u8]>,
        I: IntoIterator<Item = (&'a ColumnFamily, K)>,
    {
        self.multi_get_cf_full(keys_cf, None)
    }

    fn multi_get_cf_opt<'a, K, I>(
        &self,
        keys_cf: I,
        readopts: &R,
    ) -> Vec<Result<Option<DBVector>, Error>>
    where
        K: AsRef<[u8]>,
        I: IntoIterator<Item = (&'a ColumnFamily, K)>,
    {
        self.multi_get_cf_full(keys_cf, Some(readopts))
    }
}

impl<T> MultiGet<ReadOptions> for T
where
    T: Handle<ffi::rocksdb_t> + super::Read,
{
    /// Return the values associated with the given keys using read options.
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
                return keys.into_iter().map(|_| Err(e.clone())).collect();
            }
        };

        let keys = MultiGetKeys::new(keys);
        let mut results = MultiGetResults::new(keys.len());
        unsafe {
            ffi::rocksdb_multi_get(
                self.handle(),
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
}

impl<T> MultiGetCF<ReadOptions> for T
where
    T: Handle<ffi::rocksdb_t> + super::Read,
{
    /// Return the values associated with the given keys and column families using read options.
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
                return keys.into_iter().map(|_| Err(e.clone())).collect();
            }
        };

        let (columns, keys) = MultiGetKeys::with_column_families(keys);
        let mut results = MultiGetResults::new(keys.len());
        unsafe {
            ffi::rocksdb_multi_get_cf(
                self.handle(),
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
}

pub trait BatchedMultiGetCF<R> {
    fn batched_multi_get_cf_full<'a, K, I>(
        &'a self,
        cf: &ColumnFamily,
        keys: I,
        sorted_input: bool,
        readopts: Option<&R>,
    ) -> Vec<Result<Option<DBPinnableSlice<'a>>, Error>>
    where
        K: AsRef<[u8]> + 'a + ?Sized,
        I: IntoIterator<Item = &'a K>;

    fn batched_multi_get_cf<'a, K, I>(
        &'a self,
        cf: &ColumnFamily,
        keys: I,
        sorted_input: bool,
    ) -> Vec<Result<Option<DBPinnableSlice<'a>>, Error>>
    where
        K: AsRef<[u8]> + 'a + ?Sized,
        I: IntoIterator<Item = &'a K>,
    {
        self.batched_multi_get_cf_full(cf, keys, sorted_input, None)
    }

    fn batched_multi_get_cf_opt<'a, K, I>(
        &'a self,
        cf: &ColumnFamily,
        keys: I,
        sorted_input: bool,
        readopts: &R,
    ) -> Vec<Result<Option<DBPinnableSlice<'a>>, Error>>
    where
        K: AsRef<[u8]> + 'a + ?Sized,
        I: IntoIterator<Item = &'a K>,
    {
        self.batched_multi_get_cf_full(cf, keys, sorted_input, Some(readopts))
    }
}

impl<T> BatchedMultiGetCF<ReadOptions> for T
where
    T: Handle<ffi::rocksdb_t> + super::Read,
{
    fn batched_multi_get_cf_full<'a, K, I>(
        &'a self,
        cf: &ColumnFamily,
        keys: I,
        sorted_input: bool,
        readopts: Option<&ReadOptions>,
    ) -> Vec<Result<Option<DBPinnableSlice<'a>>, Error>>
    where
        K: AsRef<[u8]> + 'a + ?Sized,
        I: IntoIterator<Item = &'a K>,
    {
        let mut default_readopts = None;
        let ro_handle = match ReadOptions::input_or_default(readopts, &mut default_readopts) {
            Ok(ro) => ro,
            Err(e) => {
                return keys.into_iter().map(|_| Err(e.clone())).collect();
            }
        };

        let (ptr_keys, keys_sizes): (Vec<_>, Vec<_>) = keys
            .into_iter()
            .map(|k| {
                let k = k.as_ref();
                (k.as_ptr() as *const c_char, k.len())
            })
            .unzip();

        let mut pinned_values = vec![ptr::null_mut(); ptr_keys.len()];
        let mut errors = vec![ptr::null_mut(); ptr_keys.len()];

        unsafe {
            ffi::rocksdb_batched_multi_get_cf(
                self.handle(),
                ro_handle,
                cf.inner,
                ptr_keys.len(),
                ptr_keys.as_ptr(),
                keys_sizes.as_ptr(),
                pinned_values.as_mut_ptr(),
                errors.as_mut_ptr(),
                sorted_input,
            );
            pinned_values
                .into_iter()
                .zip(errors)
                .map(|(v, e)| {
                    if e.is_null() {
                        if v.is_null() {
                            Ok(None)
                        } else {
                            Ok(Some(DBPinnableSlice::from_c(v)))
                        }
                    } else {
                        Err(Error::new(crate::ffi_util::error_message(e)))
                    }
                })
                .collect()
        }
    }
}

/// Takes ownership of native MultiGet outputs.
///
/// # Safety
/// All vectors must have equal lengths. Non-null values and errors must be
/// distinct, exclusively owned RocksDB allocations; each length must describe
/// the corresponding value allocation and each error must be NUL-terminated.
pub unsafe fn convert_values(
    values: Vec<*mut c_char>,
    values_sizes: Vec<usize>,
    errors: Vec<*mut c_char>,
) -> Vec<Result<Option<DBVector>, Error>> {
    values
        .into_iter()
        .zip(values_sizes)
        .zip(errors)
        .map(|((v, s), e)| {
            if e.is_null() {
                if v.is_null() {
                    return Ok(None);
                }
                unsafe { Ok(Some(DBVector::from_c(v as *mut u8, s))) }
            } else {
                Err(Error::new(unsafe { crate::ffi_util::error_message(e) }))
            }
        })
        .collect()
}
