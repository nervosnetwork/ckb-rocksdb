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
            ffi::rocksdb_multi_get(
                self.handle(),
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
            ffi::rocksdb_multi_get_cf(
                self.handle(),
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
                let key_count = keys.into_iter().count();

                return vec![e; key_count]
                    .iter()
                    .map(|e| Err(e.to_owned()))
                    .collect();
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

pub fn convert_values(
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
                Err(Error::new(crate::ffi_util::error_message(e)))
            }
        })
        .collect()
}
