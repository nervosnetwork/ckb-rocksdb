use crate::ffi;
use crate::ffi_util::to_cpath;
use crate::{ColumnFamily, Error, IngestExternalFileOptions, handle::Handle};
use std::ffi::CString;
use std::path::Path;

pub trait IngestExternalFile {
    fn ingest_external_file_full<P: AsRef<Path>>(
        &self,
        paths: Vec<P>,
        opts: Option<&IngestExternalFileOptions>,
    ) -> Result<(), Error>;

    /// Loads a list of external SST files created with SstFileWriter into the DB with default opts
    fn ingest_external_file<P: AsRef<Path>>(&self, paths: Vec<P>) -> Result<(), Error> {
        self.ingest_external_file_full(paths, None)
    }

    /// Loads a list of external SST files created with SstFileWriter into the DB
    fn ingest_external_file_opts<P: AsRef<Path>>(
        &self,
        paths: Vec<P>,
        opts: &IngestExternalFileOptions,
    ) -> Result<(), Error> {
        self.ingest_external_file_full(paths, Some(opts))
    }
}

pub trait IngestExternalFileCF {
    fn ingest_external_file_cf_full<P: AsRef<Path>>(
        &self,
        cf: Option<&ColumnFamily>,
        paths: Vec<P>,
        opts: Option<&IngestExternalFileOptions>,
    ) -> Result<(), Error>;

    /// Loads a list of external SST files created with SstFileWriter into the DB for given Column Family
    /// with default opts
    fn ingest_external_file_cf<P: AsRef<Path>>(
        &self,
        cf: &ColumnFamily,
        paths: Vec<P>,
    ) -> Result<(), Error> {
        self.ingest_external_file_cf_full(Some(cf), paths, None)
    }

    /// Loads a list of external SST files created with SstFileWriter into the DB for given Column Family
    fn ingest_external_file_opts<P: AsRef<Path>>(
        &self,
        cf: &ColumnFamily,
        paths: Vec<P>,
        opts: &IngestExternalFileOptions,
    ) -> Result<(), Error> {
        self.ingest_external_file_cf_full(Some(cf), paths, Some(opts))
    }
}

impl<T> IngestExternalFile for T
where
    T: IngestExternalFileCF,
{
    fn ingest_external_file_full<P: AsRef<Path>>(
        &self,
        paths: Vec<P>,
        opts: Option<&IngestExternalFileOptions>,
    ) -> Result<(), Error> {
        self.ingest_external_file_cf_full(None, paths, opts)
    }
}

impl<T> IngestExternalFileCF for T
where
    T: Handle<ffi::rocksdb_t> + super::Write,
{
    fn ingest_external_file_cf_full<P: AsRef<Path>>(
        &self,
        cf: Option<&ColumnFamily>,
        paths: Vec<P>,
        opts: Option<&IngestExternalFileOptions>,
    ) -> Result<(), Error> {
        let paths_v: Vec<CString> = paths
            .iter()
            .map(|path| {
                to_cpath(
                    path,
                    "Failed to convert path to CString when IngestExternalFile.",
                )
            })
            .collect::<Result<Vec<_>, _>>()?;

        let cpaths: Vec<_> = paths_v.iter().map(|path| path.as_ptr()).collect();

        let mut default_opts = None;

        let ief_handle = IngestExternalFileOptions::input_or_default(opts, &mut default_opts)?;

        unsafe {
            match cf {
                Some(cf) => ffi_try!(ffi::rocksdb_ingest_external_file_cf(
                    self.handle(),
                    cf.handle(),
                    cpaths.as_ptr(),
                    paths_v.len(),
                    ief_handle
                )),
                None => ffi_try!(ffi::rocksdb_ingest_external_file(
                    self.handle(),
                    cpaths.as_ptr(),
                    paths_v.len(),
                    ief_handle
                )),
            };

            Ok(())
        }
    }
}
