//! Independently owned column families for shared optimistic databases.
use crate::{
    ColumnFamily, DBPinnableSlice, Error, OptimisticTransactionDB, Options, ReadOptions, ffi,
    handle::Handle,
};
use std::{collections::BTreeMap, ops::Deref, sync::Arc};

/// A column-family handle that keeps its database and configured resources alive.
///
/// Dropping the last owner releases the native handle. Removing the family from
/// the database is a separate, fallible operation: [`Self::drop_from_database`].
/// Existing owners and snapshots can still read a dropped family.
pub struct OwnedColumnFamily {
    pub(crate) inner: ColumnFamily,
    pub(crate) db: Arc<OptimisticTransactionDB>,
    name: String,
}

// A CF handle is immutable; RocksDB synchronizes operations on its database.
// Arc ownership prevents handle destruction while another thread is using it.
unsafe impl Sync for OwnedColumnFamily {}

impl OwnedColumnFamily {
    /// The physical column-family name supplied at creation or open.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Remove the family from the persistent database, retaining this handle on error.
    ///
    /// All writes using this family must have finished before calling this method.
    /// Reads using existing handles and snapshots may finish afterwards. Calling
    /// this method again after a successful drop returns RocksDB's error.
    /// An error can also occur after the native drop has taken effect (for
    /// example, writing OPTIONS). Reopen or re-enumerate before deciding to retry.
    pub fn drop_from_database(&self) -> Result<(), Error> {
        unsafe {
            ffi_try!(ffi::rocksdb_drop_column_family(
                self.db.base_db_ptr(),
                self.inner.inner,
            ));
        }
        Ok(())
    }

    /// Read a value whose pin owns this family and database, without a copy.
    /// The result may outlive both the calling `Arc` and the read options.
    pub fn get_pinned<K: AsRef<[u8]>>(
        self: &Arc<Self>,
        key: K,
        options: &ReadOptions,
    ) -> Result<Option<DBPinnableSlice<'static>>, Error> {
        let key = key.as_ref();
        unsafe {
            let value = ffi_try!(ffi::rocksdb_get_pinned_cf(
                self.db.base_db_ptr(),
                options.handle(),
                self.inner.inner,
                key.as_ptr().cast(),
                key.len(),
            ));
            if value.is_null() {
                return Ok(None);
            }
            let mut pin = DBPinnableSlice::from_c(value);
            pin.column_family = Some(Arc::clone(self));
            Ok(Some(pin))
        }
    }
}

impl Deref for OwnedColumnFamily {
    type Target = ColumnFamily;
    fn deref(&self) -> &ColumnFamily {
        &self.inner
    }
}

impl Drop for OwnedColumnFamily {
    fn drop(&mut self) {
        // Each handle was removed from the database's owning map or created below.
        // It is never copied, and `db` is released only after this destructor.
        unsafe { ffi::rocksdb_column_family_handle_destroy(self.inner.inner) }
    }
}

impl OptimisticTransactionDB {
    /// Share a database, transferring its column-family handles to independent owners.
    ///
    /// The returned database's name map is empty. Callers
    /// own the returned map and must retain a family while using its handle. This
    /// allows application-level routing to publish immutable groups of families.
    pub fn into_shared_columns(mut self) -> (Arc<Self>, BTreeMap<String, Arc<OwnedColumnFamily>>) {
        use crate::ops::GetColumnFamilys;
        let columns = std::mem::take(self.get_mut_cfs());
        let db = Arc::new(self);
        let columns = columns
            .into_iter()
            .map(|(name, inner)| {
                let column = Arc::new(OwnedColumnFamily {
                    inner,
                    name: name.clone(),
                    db: Arc::clone(&db),
                });
                (name, column)
            })
            .collect();
        (db, columns)
    }

    /// Create an independently owned column family while the database is shared.
    /// An error can occur after the family was created. Reopening resolves that
    /// outcome; resources from `options` remain retained by the database.
    pub fn create_owned_cf(
        self: &Arc<Self>,
        name: &str,
        options: &Options,
    ) -> Result<Arc<OwnedColumnFamily>, Error> {
        // Native CFs remain part of a DB even when their last application handle
        // closes. Cache/environment ownership must therefore live with the DB.
        let c_name = crate::ffi_util::to_cstring(name, "column family name contains a NUL byte")?;
        self.retain_options(options);
        let inner = ColumnFamily::create(self.as_ref(), &c_name, options)?;
        let column = Arc::new(OwnedColumnFamily {
            inner,
            name: name.to_owned(),
            db: Arc::clone(self),
        });
        Ok(column)
    }

    /// Create families with identical options, preserving their input order.
    ///
    /// Creation can fail after a prefix, or all, of the families already exists.
    /// On error, native handles are released but their families and option
    /// resources remain in the database. Reopen to resolve that namespace.
    pub fn create_owned_cfs<S: AsRef<str>>(
        self: &Arc<Self>,
        names: &[S],
        options: &Options,
    ) -> Result<Vec<Arc<OwnedColumnFamily>>, Error> {
        let count = i32::try_from(names.len())
            .map_err(|_| Error::new("too many column families".to_owned()))?;
        let c_names = names
            .iter()
            .map(|name| crate::ffi_util::to_cstring(name, "column family name contains a NUL byte"))
            .collect::<Result<Vec<_>, _>>()?;
        if names.is_empty() {
            return Ok(Vec::new());
        }
        let pointers: Vec<_> = c_names.iter().map(|name| name.as_ptr()).collect();
        self.retain_options(options);
        let mut error = std::ptr::null_mut();
        let mut length = 0;
        // The native result includes every successfully created handle even on
        // error. The array and its handles have separate ownership.
        let handles = unsafe {
            let array = ffi::rocksdb_create_column_families(
                self.base_db_ptr(),
                options.inner,
                count,
                pointers.as_ptr(),
                &mut length,
                &mut error,
            );
            let handles: Vec<_> = (0..length).map(|i| *array.add(i)).collect();
            ffi::rocksdb_create_column_families_destroy(array);
            handles
        };
        if !error.is_null() {
            for handle in handles {
                unsafe { ffi::rocksdb_column_family_handle_destroy(handle) }
            }
            return Err(Error::new(crate::ffi_util::error_message(error)));
        }
        Ok(handles
            .into_iter()
            .zip(names)
            .map(|(handle, name)| {
                Arc::new(OwnedColumnFamily {
                    inner: ColumnFamily::new(handle),
                    db: Arc::clone(self),
                    name: name.as_ref().to_owned(),
                })
            })
            .collect())
    }
}
