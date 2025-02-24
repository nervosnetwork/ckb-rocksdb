use crate::ffi;
use crate::{Error, checkpoint::Checkpoint, handle::Handle};
use std::marker::PhantomData;

pub trait CreateCheckpointObject {
    unsafe fn create_checkpoint_object_raw(&self) -> Result<*mut ffi::rocksdb_checkpoint_t, Error>;
    fn create_checkpoint_object(&self) -> Result<Checkpoint<'_>, Error> {
        let checkpoint: *mut ffi::rocksdb_checkpoint_t;

        unsafe { checkpoint = self.create_checkpoint_object_raw()? };

        if checkpoint.is_null() {
            return Err(Error::new("Could not create checkpoint object.".to_owned()));
        }

        Ok(Checkpoint {
            inner: checkpoint,
            _db: PhantomData,
        })
    }
}

impl<T> CreateCheckpointObject for T
where
    T: Handle<ffi::rocksdb_t>,
{
    unsafe fn create_checkpoint_object_raw(&self) -> Result<*mut ffi::rocksdb_checkpoint_t, Error> {
        unsafe {
            Ok(ffi_try!(ffi::rocksdb_checkpoint_object_create(
                self.handle(),
            )))
        }
    }
}
