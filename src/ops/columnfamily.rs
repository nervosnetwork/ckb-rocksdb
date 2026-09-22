use crate::ffi;

use crate::{ColumnFamily, Error, Options, handle::Handle};

use std::collections::BTreeMap;

pub trait GetColumnFamilys {
    fn get_cfs(&self) -> &BTreeMap<String, ColumnFamily>;

    /// # Safety
    /// Removed handles must not escape their DB or be used after destruction.
    /// The caller is responsible for destroying each transferred handle once.
    unsafe fn get_mut_cfs(&mut self) -> &mut BTreeMap<String, ColumnFamily>;

    /// Return the underlying column family handle.
    fn cf_handle(&self, name: &str) -> Option<&ColumnFamily> {
        self.get_cfs().get(name)
    }
}

pub trait CreateCF {
    fn create_cf<N: AsRef<str>>(&mut self, name: N, opts: &Options) -> Result<(), Error>;
}

pub trait DropCF {
    fn drop_cf(&mut self, name: &str) -> Result<(), Error>;
}

impl<T> CreateCF for T
where
    T: Handle<ffi::rocksdb_t> + super::Write + GetColumnFamilys + crate::db_options::RetainOptions,
{
    fn create_cf<N: AsRef<str>>(&mut self, name: N, opts: &Options) -> Result<(), Error> {
        let name = name.as_ref();
        let c_name = crate::ffi_util::to_cstring(name, "column family name contains a NUL byte")?;
        self.retain_options(opts);
        let column = ColumnFamily::create(self, &c_name, opts)?;
        // The new family remains owned by this database.
        unsafe { self.get_mut_cfs() }.insert(name.to_owned(), column);
        Ok(())
    }
}

impl<T> DropCF for T
where
    T: Handle<ffi::rocksdb_t> + super::Write + GetColumnFamilys,
{
    fn drop_cf(&mut self, name: &str) -> Result<(), Error> {
        let cf = self
            .get_cfs()
            .get(name)
            .ok_or_else(|| Error::new(format!("Invalid column family: {}", name)))?;
        unsafe {
            ffi_try!(ffi::rocksdb_drop_column_family(self.handle(), cf.inner,));
            // Keep the mapping on error, and release the native handle exactly once
            // after a successful drop. Removing the Rust value alone leaked it.
            let cf = self
                .get_mut_cfs()
                .remove(name)
                .expect("column checked above");
            ffi::rocksdb_column_family_handle_destroy(cf.inner);
        }
        Ok(())
    }
}
