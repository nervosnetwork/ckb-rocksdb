use crate::{
    ColumnFamily, Error, OptimisticTransaction, Options, WriteOptions,
    db_iterator::DBRawIterator,
    db_options::{OptionsMustOutliveDB, ReadOptions},
    db_vector::DBVector,
    handle::{ConstHandle, Handle},
    open_raw::{OpenRaw, OpenRawFFI},
    ops::*,
};

use crate::ffi;
use crate::ffi_util::to_cpath;
use libc::c_uchar;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::ptr;

/// A optimistic transaction database.
pub struct OptimisticTransactionDB {
    inner: *mut ffi::rocksdb_optimistictransactiondb_t,
    path: PathBuf,
    cfs: BTreeMap<String, ColumnFamily>,
    base_db: *mut ffi::rocksdb_t,
    outlive: std::sync::Mutex<Vec<OptionsMustOutliveDB>>,
}

unsafe impl Handle<ffi::rocksdb_optimistictransactiondb_t> for OptimisticTransactionDB {
    fn handle(&self) -> *mut ffi::rocksdb_optimistictransactiondb_t {
        self.inner
    }
}

impl Open for OptimisticTransactionDB {}
impl OpenCF for OptimisticTransactionDB {}

impl OpenRaw for OptimisticTransactionDB {
    type Pointer = ffi::rocksdb_optimistictransactiondb_t;
    type Descriptor = ();

    fn open_ffi(input: OpenRawFFI<'_, Self::Descriptor>) -> Result<*mut Self::Pointer, Error> {
        let pointer = unsafe {
            if input.num_column_families <= 0 {
                ffi_try!(ffi::rocksdb_optimistictransactiondb_open(
                    input.options,
                    input.path,
                ))
            } else {
                ffi_try!(ffi::rocksdb_optimistictransactiondb_open_column_families(
                    input.options,
                    input.path,
                    input.num_column_families,
                    input.column_family_names,
                    input.column_family_options,
                    input.column_family_handles,
                ))
            }
        };

        Ok(pointer)
    }

    unsafe fn build<I>(
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
        let base_db = unsafe { ffi::rocksdb_optimistictransactiondb_get_base_db(pointer) };
        Ok(OptimisticTransactionDB {
            inner: pointer,
            cfs,
            path,
            base_db,
            outlive: std::sync::Mutex::new(outlive),
        })
    }
}

impl Read for OptimisticTransactionDB {}
impl Write for OptimisticTransactionDB {}

unsafe impl Send for OptimisticTransactionDB {}
unsafe impl Sync for OptimisticTransactionDB {}

impl GetColumnFamilys for OptimisticTransactionDB {
    fn get_cfs(&self) -> &BTreeMap<String, ColumnFamily> {
        &self.cfs
    }
    unsafe fn get_mut_cfs(&mut self) -> &mut BTreeMap<String, ColumnFamily> {
        &mut self.cfs
    }
}

impl OptimisticTransactionDB {
    pub(crate) fn retain_options(&self, options: &Options) {
        let mut retained = self
            .outlive
            .lock()
            .expect("database resources lock poisoned");
        // Repeated CF generations normally share the same cache and environment.
        // Retain each resource combination once, including after a handle closes.
        options.outlive.retain_in(&mut retained);
    }

    pub fn path(&self) -> &Path {
        self.path.as_path()
    }

    pub fn base_db_ptr(&self) -> *mut ffi::rocksdb_t {
        self.base_db
    }

    pub fn repair<P: AsRef<Path>>(opts: Options, path: P) -> Result<(), Error> {
        let cpath = to_cpath(
            path,
            "Failed to convert path to CString when opening database.",
        )?;
        unsafe {
            ffi_try!(ffi::rocksdb_repair_db(opts.inner, cpath.as_ptr(),));
        }
        Ok(())
    }

    /// Begin a transaction that retains this database through its `Arc` owner.
    pub fn transaction(
        self: &std::sync::Arc<Self>,
        write_options: &WriteOptions,
        tx_options: &OptimisticTransactionOptions,
    ) -> OptimisticTransaction {
        unsafe {
            let inner = ffi::rocksdb_optimistictransaction_begin(
                self.inner,
                write_options.handle(),
                tx_options.inner,
                ptr::null_mut(),
            );
            OptimisticTransaction::new(inner, std::sync::Arc::clone(self))
        }
    }

    pub fn transaction_default(self: &std::sync::Arc<Self>) -> OptimisticTransaction {
        let write_options = WriteOptions::default();
        let transaction_options = OptimisticTransactionOptions::default();
        self.transaction(&write_options, &transaction_options)
    }
}

impl Drop for OptimisticTransactionDB {
    fn drop(&mut self) {
        unsafe {
            for cf in self.cfs.values() {
                ffi::rocksdb_column_family_handle_destroy(cf.inner);
            }
            ffi::rocksdb_optimistictransactiondb_close_base_db(self.base_db);
            ffi::rocksdb_optimistictransactiondb_close(self.inner);
        }
    }
}

pub struct OptimisticTransactionOptions {
    pub(crate) inner: *mut ffi::rocksdb_optimistictransaction_options_t,
}

impl OptimisticTransactionOptions {
    /// Create new optimistic transaction options
    pub fn new() -> OptimisticTransactionOptions {
        unsafe {
            let inner = ffi::rocksdb_optimistictransaction_options_create();
            OptimisticTransactionOptions { inner }
        }
    }

    /// Set a snapshot at start of transaction by setting set_snapshot=true
    /// Default: false
    pub fn set_snapshot(&mut self, set_snapshot: bool) {
        unsafe {
            ffi::rocksdb_optimistictransaction_options_set_set_snapshot(
                self.inner,
                set_snapshot as c_uchar,
            );
        }
    }
}

impl Drop for OptimisticTransactionOptions {
    fn drop(&mut self) {
        unsafe {
            ffi::rocksdb_optimistictransaction_options_destroy(self.inner);
        }
    }
}

impl Default for OptimisticTransactionOptions {
    fn default() -> OptimisticTransactionOptions {
        OptimisticTransactionOptions::new()
    }
}

unsafe impl Handle<ffi::rocksdb_t> for OptimisticTransactionDB {
    fn handle(&self) -> *mut ffi::rocksdb_t {
        self.base_db
    }
}

impl Iterate for OptimisticTransactionDB {
    fn get_raw_iter<'a: 'b, 'b>(&'a self, readopts: &ReadOptions) -> DBRawIterator<'b> {
        unsafe {
            DBRawIterator::new(readopts, |readopts| {
                ffi::rocksdb_create_iterator(self.base_db, readopts.handle())
            })
        }
    }
}

impl IterateCF for OptimisticTransactionDB {
    fn get_raw_iter_cf<'a: 'b, 'b>(
        &'a self,
        cf_handle: &'b ColumnFamily,
        readopts: &ReadOptions,
    ) -> Result<DBRawIterator<'b>, Error> {
        unsafe {
            Ok(DBRawIterator::new(readopts, |readopts| {
                ffi::rocksdb_create_iterator_cf(self.base_db, readopts.handle(), cf_handle.inner)
            }))
        }
    }
}

impl OptimisticTransactionDB {
    pub fn snapshot(&self) -> Snapshot<'_> {
        let snapshot = unsafe { ffi::rocksdb_create_snapshot(self.base_db) };
        Snapshot {
            db: self,
            inner: snapshot,
        }
    }
}

pub struct Snapshot<'a> {
    db: &'a OptimisticTransactionDB,
    inner: *const ffi::rocksdb_snapshot_t,
}

unsafe impl ConstHandle<ffi::rocksdb_snapshot_t> for Snapshot<'_> {
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
        // The adapter and its returned reads borrow this same snapshot.
        unsafe { ro.set_snapshot(self) };

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
        // The adapter and its returned reads borrow this same snapshot.
        unsafe { ro.set_snapshot(self) };

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
        // The adapter and its returned reads borrow this same snapshot.
        unsafe { ro.set_snapshot(self) };

        self.db.multi_get_cf_full(keys, Some(&ro))
    }
}

impl Drop for Snapshot<'_> {
    fn drop(&mut self) {
        unsafe {
            ffi::rocksdb_release_snapshot(self.db.base_db, self.inner);
        }
    }
}

impl Iterate for Snapshot<'_> {
    fn get_raw_iter<'a: 'b, 'b>(&'a self, readopts: &ReadOptions) -> DBRawIterator<'b> {
        let mut ro = readopts.to_owned();
        // The adapter and its returned reads borrow this same snapshot.
        unsafe { ro.set_snapshot(self) };
        self.db.get_raw_iter(&ro)
    }
}

impl IterateCF for Snapshot<'_> {
    fn get_raw_iter_cf<'a: 'b, 'b>(
        &'a self,
        cf_handle: &'b ColumnFamily,
        readopts: &ReadOptions,
    ) -> Result<DBRawIterator<'b>, Error> {
        let mut ro = readopts.to_owned();
        // The adapter and its returned reads borrow this same snapshot.
        unsafe { ro.set_snapshot(self) };
        self.db.get_raw_iter_cf(cf_handle, &ro)
    }
}

impl crate::db_options::RetainOptions for OptimisticTransactionDB {
    fn retain_options(&mut self, options: &Options) {
        OptimisticTransactionDB::retain_options(self, options);
    }
}
