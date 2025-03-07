use crate::{ColumnFamily, Error, handle::Handle};
use libc::{c_char, c_void};
use std::ffi::{CStr, CString};

pub trait GetProperty {
    /// Retrieves a RocksDB property by name.
    ///
    /// For a full list of properties, see
    /// https://github.com/facebook/rocksdb/blob/08809f5e6cd9cc4bc3958dd4d59457ae78c76660/include/rocksdb/db.h#L428-L634
    fn property_value(&self, name: &str) -> Result<Option<String>, Error>;

    /// Retrieves a RocksDB property and casts it to an integer.
    ///
    /// For a full list of properties that return int values, see
    /// https://github.com/facebook/rocksdb/blob/08809f5e6cd9cc4bc3958dd4d59457ae78c76660/include/rocksdb/db.h#L654-L689
    fn property_int_value(&self, name: &str) -> Result<Option<u64>, Error>;
}

pub trait GetPropertyCF {
    /// Retrieves a RocksDB property by name, for a specific column family.
    ///
    /// For a full list of properties, see
    /// https://github.com/facebook/rocksdb/blob/08809f5e6cd9cc4bc3958dd4d59457ae78c76660/include/rocksdb/db.h#L428-L634
    fn property_value_cf(&self, cf: &ColumnFamily, name: &str) -> Result<Option<String>, Error>;

    /// Retrieves a RocksDB property for a specific column family and casts it to an integer.
    ///
    /// For a full list of properties that return int values, see
    /// https://github.com/facebook/rocksdb/blob/08809f5e6cd9cc4bc3958dd4d59457ae78c76660/include/rocksdb/db.h#L654-L689
    fn property_int_value_cf(&self, cf: &ColumnFamily, name: &str) -> Result<Option<u64>, Error>;
}

impl<T> GetProperty for T
where
    T: Handle<ffi::rocksdb_t>,
{
    fn property_value(&self, name: &str) -> Result<Option<String>, Error> {
        property_value_impl(
            name,
            |prop_name| unsafe { ffi::rocksdb_property_value(self.handle(), prop_name) },
            |str_value| Ok(str_value.to_owned()),
        )
    }

    fn property_int_value(&self, name: &str) -> Result<Option<u64>, Error> {
        property_value_impl(
            name,
            |prop_name| unsafe { ffi::rocksdb_property_value(self.handle(), prop_name) },
            parse_property_int_value,
        )
    }
}

impl<T> GetPropertyCF for T
where
    T: Handle<ffi::rocksdb_t>,
{
    fn property_value_cf(&self, cf: &ColumnFamily, name: &str) -> Result<Option<String>, Error> {
        property_value_impl(
            name,
            |prop_name| unsafe {
                ffi::rocksdb_property_value_cf(self.handle(), cf.inner, prop_name)
            },
            |str_value| Ok(str_value.to_owned()),
        )
    }

    fn property_int_value_cf(&self, cf: &ColumnFamily, name: &str) -> Result<Option<u64>, Error> {
        property_value_impl(
            name,
            |prop_name| unsafe {
                ffi::rocksdb_property_value_cf(self.handle(), cf.inner, prop_name)
            },
            parse_property_int_value,
        )
    }
}

fn parse_property_int_value(value: &str) -> Result<u64, Error> {
    value.parse::<u64>().map_err(|err| {
        Error::new(format!(
            "Failed to convert property value {} to int: {}",
            value, err
        ))
    })
}

/// Implementation for property_value et al methods.
///
/// `name` is the name of the property.  It will be converted into a CString
/// and passed to `get_property` as argument.  `get_property` reads the
/// specified property and either returns NULL or a pointer to a C allocated
/// string; this method takes ownership of that string and will free it at
/// the end.  That string is parsed using `parse` callback which produces
/// the returned result.
fn property_value_impl<R>(
    name: &str,
    get_property: impl FnOnce(*const c_char) -> *mut c_char,
    parse: impl FnOnce(&str) -> Result<R, Error>,
) -> Result<Option<R>, Error> {
    let value = match CString::new(name) {
        Ok(prop_name) => get_property(prop_name.as_ptr()),
        Err(e) => {
            return Err(Error::new(format!(
                "Failed to convert property name to CString: {}",
                e
            )));
        }
    };
    if value.is_null() {
        return Ok(None);
    }
    let result = match unsafe { CStr::from_ptr(value) }.to_str() {
        Ok(s) => parse(s).map(Some),
        Err(e) => Err(Error::new(format!(
            "Failed to convert property value to string: {}",
            e
        ))),
    };
    unsafe {
        ffi::rocksdb_free(value as *mut c_void);
    }
    result
}
