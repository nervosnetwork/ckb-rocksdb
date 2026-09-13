use crate::ffi;
use crate::{ColumnFamily, Error, FlushOptions, handle::Handle};

pub trait Flush {
    /// Flushes database memtables to SST files on the disk.
    fn flush_opt(&self, flushopts: &FlushOptions) -> Result<(), Error>;

    /// Flushes database memtables to SST files on the disk using default options.
    fn flush(&self) -> Result<(), Error> {
        self.flush_opt(&FlushOptions::default())
    }
}

pub trait FlushWal {
    /// Writes buffered WAL data to the filesystem, optionally syncing it to disk.
    /// This does not flush memtables or replace a subsequent synchronous write
    /// that publishes data written after this call.
    fn flush_wal(&self, sync: bool) -> Result<(), Error>;
}

impl<T> FlushWal for T
where
    T: Handle<ffi::rocksdb_t> + super::Write,
{
    fn flush_wal(&self, sync: bool) -> Result<(), Error> {
        unsafe {
            ffi_try!(ffi::rocksdb_flush_wal(self.handle(), u8::from(sync)));
        }
        Ok(())
    }
}

#[allow(dead_code)]
pub trait FlushCF {
    /// Flushes database memtables to SST files on the disk for a given column family.
    fn flush_cf_opt(&self, cf: &ColumnFamily, flushopts: &FlushOptions) -> Result<(), Error>;

    /// Flushes database memtables to SST files on the disk for a given column family using default
    /// options.
    fn flush_cf(&self, cf: &ColumnFamily) -> Result<(), Error> {
        self.flush_cf_opt(cf, &FlushOptions::default())
    }
}

impl<T> Flush for T
where
    T: Handle<ffi::rocksdb_t> + super::Write,
{
    fn flush_opt(&self, flushopts: &FlushOptions) -> Result<(), Error> {
        unsafe {
            ffi_try!(ffi::rocksdb_flush(self.handle(), flushopts.inner,));
        }
        Ok(())
    }
}

impl<T> FlushCF for T
where
    T: Handle<ffi::rocksdb_t> + super::Write,
{
    fn flush_cf_opt(&self, cf: &ColumnFamily, flushopts: &FlushOptions) -> Result<(), Error> {
        unsafe {
            ffi_try!(ffi::rocksdb_flush_cf(
                self.handle(),
                flushopts.inner,
                cf.inner,
            ));
        }
        Ok(())
    }
}
