// Copyright 2020 Nervos Core Dev
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

use std::{ffi::CStr, path, ptr::NonNull};

use crate::{
    ColumnFamilyDescriptor, Env, Error, Options,
    db_options::{Cache, CacheWrapper, OptionsMustOutliveDB},
    ffi, ffi_util,
};

#[derive(Clone)]
pub struct FullOptions {
    pub db_opts: Options,
    pub cf_descriptors: Vec<ColumnFamilyDescriptor>,
}

struct ColumnFamilyDescriptors(*mut ffi::rocksdb_column_family_descriptors_t);

impl Drop for ColumnFamilyDescriptors {
    fn drop(&mut self) {
        // The loader transfers the collection, including every name, to us.
        unsafe { ffi::rocksdb_column_family_descriptors_destroy(self.0) };
    }
}

impl FullOptions {
    pub fn load_from_file<P>(
        file: P,
        cache_size: Option<usize>,
        ignore_unknown_options: bool,
    ) -> Result<Self, Error>
    where
        P: AsRef<path::Path>,
    {
        Self::load_from_file_with_cache(
            file,
            cache_size.map(Cache::try_new_lru_cache).transpose()?,
            ignore_unknown_options,
        )
    }

    pub fn load_from_file_with_cache<P>(
        file: P,
        cache: Option<Cache>,
        ignore_unknown_options: bool,
    ) -> Result<Self, Error>
    where
        P: AsRef<path::Path>,
    {
        let cpath = ffi_util::to_cpath(
            file,
            "Failed to convert path to CString when load config file.",
        )?;

        let env = Env::default_env()?;
        unsafe {
            // The C loader expects a cache wrapper even when its shared cache
            // is null. Give that temporary wrapper an owner on both exit paths.
            let null_cache;
            let cache_ptr = match &cache {
                Some(cache) => cache.0.inner.as_ptr(),
                None => {
                    null_cache = CacheWrapper {
                        inner: NonNull::new(ffi::rocksdb_null_cache()).ok_or_else(|| {
                            Error::new("Could not create RocksDB null cache.".to_owned())
                        })?,
                    };
                    null_cache.inner.as_ptr()
                }
            };
            let result = ffi_try!(ffi::rocksdb_options_load_from_file(
                cpath.as_ptr(),
                env.handle(),
                ignore_unknown_options,
                cache_ptr,
            ));
            let descriptors = ColumnFamilyDescriptors(result.cf_descs);
            let db_opts = Options {
                inner: NonNull::new(result.db_opts)
                    .ok_or_else(|| Error::new("Could not load RocksDB options.".to_owned()))?
                    .as_ptr(),
                outlive: OptionsMustOutliveDB {
                    row_cache: cache.clone(),
                    ..Default::default()
                },
            };
            let cf_descs = NonNull::new(descriptors.0)
                .ok_or_else(|| Error::new("Could not load column family descriptors.".to_owned()))?
                .as_ptr();
            let cf_descs_size = ffi::rocksdb_column_family_descriptors_count(cf_descs);
            let mut cf_descriptors = Vec::new();
            for index in 0..cf_descs_size {
                let name_raw = ffi::rocksdb_column_family_descriptors_name(cf_descs, index);
                if name_raw.is_null() {
                    return Err(Error::new(
                        "Could not load a column family name.".to_owned(),
                    ));
                }
                let name_cstr = CStr::from_ptr(name_raw as *const _);
                let name = String::from_utf8_lossy(name_cstr.to_bytes());
                let cf_opts_inner = NonNull::new(ffi::rocksdb_column_family_descriptors_options(
                    cf_descs, index,
                ))
                .ok_or_else(|| Error::new("Could not copy column family options.".to_owned()))?
                .as_ptr();
                let outlive = OptionsMustOutliveDB {
                    row_cache: cache.clone(),
                    ..Default::default()
                };
                let cf_opts = Options {
                    inner: cf_opts_inner,
                    outlive,
                };
                cf_descriptors.push(ColumnFamilyDescriptor::new(name, cf_opts));
            }
            Ok(Self {
                db_opts,
                cf_descriptors,
            })
        }
    }

    /// Add missing families using the options of the file's `default` family,
    /// creating that family with [`Options::default`] if it is absent.
    ///
    /// `cf_names` must not include `default`. Existing families omitted from
    /// `cf_names` cause an error unless `ignore_unknown_column_families` is true;
    /// they are retained in either case. An error does not roll back additions.
    pub fn complete_column_families(
        &mut self,
        cf_names: &[&str],
        ignore_unknown_column_families: bool,
    ) -> Result<(), Error> {
        let mut default_options = None;
        for cfd in &self.cf_descriptors {
            if cfd.name == "default" {
                default_options = Some(cfd.options.clone());
            } else if !ignore_unknown_column_families && !cf_names.contains(&cfd.name.as_str()) {
                return Err(Error::new(format!(
                    "an unknown column family named \"{}\"",
                    cfd.name
                )));
            }
        }
        let default_options = default_options.unwrap_or_else(|| {
            let cf = ColumnFamilyDescriptor::new("default", Options::default());
            let options = cf.options.clone();
            self.cf_descriptors.insert(0, cf);
            options
        });
        for &cf_name in cf_names {
            if cf_name == "default" {
                return Err(Error::new(format!(
                    "don't name a user-defined column family as \"{}\"",
                    cf_name
                )));
            }
            if !self.cf_descriptors.iter().any(|cfd| cfd.name == cf_name) {
                self.cf_descriptors.push(ColumnFamilyDescriptor::new(
                    cf_name.to_owned(),
                    default_options.clone(),
                ));
            }
        }
        Ok(())
    }
}
