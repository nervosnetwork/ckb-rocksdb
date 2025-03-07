use crate::ffi;
use crate::ffi_util::to_cstring;
use crate::ops::GetColumnFamilys;
use crate::{
    ColumnFamily, Error, Options,
    db_iterator::DBRawIterator,
    db_options::{OptionsMustOutliveDB, ReadOptions},
    handle::Handle,
    open_raw::{OpenRaw, OpenRawFFI},
    ops,
};
use std::collections::BTreeMap;
use std::fmt;
use std::marker::PhantomData;
use std::path::{Path, PathBuf};

pub struct DBWithTTL {
    pub(crate) inner: *mut ffi::rocksdb_t,
    cfs: BTreeMap<String, ColumnFamily>,
    path: PathBuf,
    _outlive: Vec<OptionsMustOutliveDB>,
}

impl DBWithTTL {
    pub fn path(&self) -> &Path {
        self.path.as_path()
    }

    pub fn create_cf_with_ttl<N: AsRef<str>>(
        &mut self,
        name: N,
        opts: &Options,
        ttl: i32,
    ) -> Result<(), Error> {
        let cname = to_cstring(
            name.as_ref(),
            "Failed to convert path to CString when opening rocksdb",
        )?;
        unsafe {
            let cf_handle = ffi_try!(ffi::rocksdb_create_column_family_with_ttl(
                self.handle(),
                opts.inner,
                cname.as_ptr(),
                ttl as libc::c_int,
            ));

            self.get_mut_cfs()
                .insert(name.as_ref().to_string(), ColumnFamily::new(cf_handle));
        };
        Ok(())
    }
}

impl Default for TTLOpenDescriptor {
    fn default() -> Self {
        TTLOpenDescriptor {
            ttls: TTLs::Default(-1),
        }
    }
}

pub enum TTLs {
    Default(i32),
    Columns(Vec<i32>),
}

// TTL is accepted in seconds
// If TTL is non positive or not provided, the behaviour is TTL = infinity
// (int32_t)Timestamp(creation) is suffixed to values in Put internally
// Expired TTL values are deleted in compaction only:(Timestamp+ttl<time_now)
// Get/Iterator may return expired entries(compaction not run on them yet)
// Different TTL may be used during different Opens
// Example: Open1 at t=0 with ttl=4 and insert k1,k2, close at t=2. Open2 at t=3 with ttl=5. Now k1,k2 should be deleted at t>=5
// read_only=true opens in the usual read-only mode. Compactions will not be triggered(neither manual nor automatic), so no expired entries removed
pub struct TTLOpenDescriptor {
    ttls: TTLs,
}

impl TTLOpenDescriptor {
    pub fn by_columns(ttls: Vec<i32>) -> Self {
        TTLOpenDescriptor {
            ttls: TTLs::Columns(ttls),
        }
    }

    pub fn by_default(ttl: i32) -> Self {
        TTLOpenDescriptor {
            ttls: TTLs::Default(ttl),
        }
    }
}

impl ops::Open for DBWithTTL {}
impl ops::OpenCF for DBWithTTL {}

impl OpenRaw for DBWithTTL {
    type Pointer = ffi::rocksdb_t;
    type Descriptor = TTLOpenDescriptor;

    fn open_ffi(input: OpenRawFFI<'_, Self::Descriptor>) -> Result<*mut Self::Pointer, Error> {
        let pointer = unsafe {
            if input.num_column_families <= 0 {
                let ttl = match input.open_descriptor.ttls {
                    TTLs::Default(ttl) => ttl as libc::c_int,
                    TTLs::Columns(_) => {
                        return Err(Error::new(
                            "Ttls size has to be the same as number of column families".to_owned(),
                        ));
                    }
                };

                ffi_try!(ffi::rocksdb_open_with_ttl(input.options, input.path, ttl,))
            } else {
                let ttls = match input.open_descriptor.ttls {
                    TTLs::Default(ttl) => (0..input.num_column_families)
                        .map(|_| ttl as libc::c_int)
                        .collect::<Vec<_>>(),
                    TTLs::Columns(ref ttls) => {
                        let ttls: Vec<_> = ttls.iter().map(|t| *t as libc::c_int).collect();

                        let is_ttls_match = if input.num_column_families <= 0 {
                            ttls.len() as i32 == 1
                        } else {
                            ttls.len() as i32 == input.num_column_families
                        };

                        if !is_ttls_match {
                            return Err(Error::new(
                                "Ttls size has to be the same as number of column families"
                                    .to_owned(),
                            ));
                        }

                        ttls
                    }
                };

                ffi_try!(ffi::rocksdb_open_column_families_with_ttl(
                    input.options,
                    input.path,
                    input.num_column_families,
                    input.column_family_names,
                    input.column_family_options,
                    input.column_family_handles,
                    ttls.as_ptr(),
                ))
            }
        };

        Ok(pointer)
    }

    fn build<I>(
        path: PathBuf,
        _open_descriptor: Self::Descriptor,
        pointer: *mut Self::Pointer,
        column_families: I,
        outlive: Vec<OptionsMustOutliveDB>,
    ) -> Result<Self, Error>
    where
        I: IntoIterator<Item = (String, *mut ffi::rocksdb_column_family_handle_t)>,
    {
        let cfs: BTreeMap<_, _> = column_families
            .into_iter()
            .map(|(k, h)| (k, ColumnFamily::new(h)))
            .collect();
        Ok(DBWithTTL {
            inner: pointer,
            cfs,
            path,
            _outlive: outlive,
        })
    }
}

impl Handle<ffi::rocksdb_t> for DBWithTTL {
    fn handle(&self) -> *mut ffi::rocksdb_t {
        self.inner
    }
}

impl ops::Iterate for DBWithTTL {
    fn get_raw_iter<'a: 'b, 'b>(&'a self, readopts: &ReadOptions) -> DBRawIterator<'b> {
        unsafe {
            DBRawIterator {
                inner: ffi::rocksdb_create_iterator(self.inner, readopts.handle()),
                db: PhantomData,
            }
        }
    }
}

impl ops::IterateCF for DBWithTTL {
    fn get_raw_iter_cf<'a: 'b, 'b>(
        &'a self,
        cf_handle: &ColumnFamily,
        readopts: &ReadOptions,
    ) -> Result<DBRawIterator<'b>, Error> {
        unsafe {
            Ok(DBRawIterator {
                inner: ffi::rocksdb_create_iterator_cf(
                    self.inner,
                    readopts.handle(),
                    cf_handle.inner,
                ),
                db: PhantomData,
            })
        }
    }
}

impl ops::GetColumnFamilys for DBWithTTL {
    fn get_cfs(&self) -> &BTreeMap<String, ColumnFamily> {
        &self.cfs
    }
    fn get_mut_cfs(&mut self) -> &mut BTreeMap<String, ColumnFamily> {
        &mut self.cfs
    }
}

impl ops::Read for DBWithTTL {}
impl ops::Write for DBWithTTL {}

unsafe impl Send for DBWithTTL {}
unsafe impl Sync for DBWithTTL {}

impl Drop for DBWithTTL {
    fn drop(&mut self) {
        unsafe {
            for cf in self.cfs.values() {
                ffi::rocksdb_column_family_handle_destroy(cf.inner);
            }
            ffi::rocksdb_close(self.inner);
        }
    }
}

impl fmt::Debug for DBWithTTL {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Read-only RocksDB {{ path: {:?} }}", self.path())
    }
}
