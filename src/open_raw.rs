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

use libc::{c_char, c_int};

use crate::ffi_util;

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::ptr;

use crate::{ColumnFamilyDescriptor, Error, Options, db_options::OptionsMustOutliveDB};

pub struct OpenRawInput<'a, T> {
    pub(crate) options: &'a Options,
    pub(crate) path: &'a Path,
    pub(crate) column_families: Vec<ColumnFamilyDescriptor>,
    pub(crate) open_descriptor: T,
    pub(crate) outlive: Vec<OptionsMustOutliveDB>,
}

pub struct OpenRawFFI<'a, T> {
    pub(crate) options: *const ffi::rocksdb_options_t,
    pub(crate) path: *const c_char,
    pub(crate) num_column_families: c_int,
    pub(crate) column_family_names: *mut *const c_char,
    pub(crate) column_family_options: *mut *const ffi::rocksdb_options_t,
    pub(crate) column_family_handles: *mut *mut ffi::rocksdb_column_family_handle_t,
    pub(crate) open_descriptor: &'a T,
}

pub trait OpenRaw: Sized {
    type Descriptor: Default;
    type Pointer;

    fn open_ffi(input: OpenRawFFI<'_, Self::Descriptor>) -> Result<*mut Self::Pointer, Error>;

    /// # Safety
    /// All pointers must come from a successful native open of this database
    /// kind. Their ownership transfers here, together with their option owners.
    unsafe fn build<I>(
        path: PathBuf,
        open_descriptor: Self::Descriptor,
        pointer: *mut Self::Pointer,
        column_families: I,
        outlive: Vec<OptionsMustOutliveDB>,
    ) -> Result<Self, Error>
    where
        I: IntoIterator<Item = (String, *mut ffi::rocksdb_column_family_handle_t)>;

    fn open_raw(input: OpenRawInput<'_, Self::Descriptor>) -> Result<Self, Error> {
        let cpath = ffi_util::to_cpath(
            input.path,
            "Failed to convert path to CString when opening database.",
        )?;

        let mut cfs = input.column_families;

        if !cfs.is_empty() && !cfs.iter().any(|cf| cf.name == "default") {
            cfs.push(ColumnFamilyDescriptor {
                name: "default".to_string(),
                options: Options::default(),
            });
        }

        let mut names = BTreeSet::new();
        for cf in &cfs {
            if !names.insert(&cf.name) {
                return Err(Error::new(format!("Duplicate column family: {}", cf.name)));
            }
        }
        let num_column_families = c_int::try_from(cfs.len())
            .map_err(|_| Error::new("too many column families".to_owned()))?;
        let cf_names: Vec<_> = cfs
            .iter()
            .map(|cf| ffi_util::to_cstring(&cf.name, "column family name contains a NUL byte"))
            .collect::<Result<_, _>>()?;

        let mut cf_name_ptrs: Vec<_> = cf_names.iter().map(|cf| cf.as_ptr()).collect();
        let mut cf_options: Vec<_> = cfs.iter().map(|cf| cf.options.inner as *const _).collect();

        let mut cf_handles = vec![ptr::null_mut(); cfs.len()];

        let pointer = Self::open_ffi(OpenRawFFI {
            options: input.options.inner,
            path: cpath.as_ptr(),
            num_column_families,
            column_family_names: cf_name_ptrs.as_mut_ptr(),
            column_family_options: cf_options.as_mut_ptr(),
            column_family_handles: cf_handles.as_mut_ptr(),
            open_descriptor: &input.open_descriptor,
        })?;

        for handle in &cf_handles {
            if handle.is_null() {
                return Err(Error::new(
                    "Received null column family handle from database.".to_owned(),
                ));
            }
        }

        let column_families = cfs.into_iter().map(|cf| cf.name).zip(cf_handles);

        // All pointers were produced by the successful native open above.
        unsafe {
            Self::build(
                input.path.to_path_buf(),
                input.open_descriptor,
                pointer,
                column_families,
                input.outlive,
            )
        }
    }
}
