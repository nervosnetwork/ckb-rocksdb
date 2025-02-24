extern crate ckb_rocksdb as rocksdb;

use crate::rocksdb::{
    ColumnFamilyDescriptor, DBWithTTL, TTLOpenDescriptor, TemporaryDBPath, prelude::*,
};

#[test]
fn open_ttl_db_default() {
    let path = TemporaryDBPath::new();

    {
        let db = DBWithTTL::open_default(&path).unwrap();
        assert!(db.put(b"k1", b"v1111").is_ok());
        let r: Result<Option<DBVector>, Error> = db.get(b"k1");

        assert!(r.unwrap().unwrap().to_utf8().unwrap() == "v1111");
    }
}

#[test]
fn open_ttl_db_cf() {
    let path = TemporaryDBPath::new();

    {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        let db = DBWithTTL::open_cf(&opts, &path, ["cf1"]).unwrap();
        let cf1 = db.cf_handle("cf1").unwrap();

        assert!(db.put_cf(cf1, b"k1", b"v1111").is_ok());
        let r: Result<Option<DBVector>, Error> = db.get_cf(cf1, b"k1");

        assert!(r.unwrap().unwrap().to_utf8().unwrap() == "v1111");
    }
}

#[test]
fn open_ttl_db_cf_with_descriptor_by_default() {
    let path = TemporaryDBPath::new();

    {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        let cf_descriptors = vec![ColumnFamilyDescriptor::new("cf1", Options::default())];

        let ttls = TTLOpenDescriptor::by_default(-1);

        let db = DBWithTTL::open_cf_descriptors_with_descriptor(&opts, &path, cf_descriptors, ttls)
            .unwrap();
        let cf1 = db.cf_handle("cf1").unwrap();

        assert!(db.put_cf(cf1, b"k1", b"v1111").is_ok());
        let r: Result<Option<DBVector>, Error> = db.get_cf(cf1, b"k1");

        assert!(r.unwrap().unwrap().to_utf8().unwrap() == "v1111");
    }
}

#[test]
fn open_ttl_db_cf_with_descriptor_by_columns() {
    let path = TemporaryDBPath::new();

    {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        let cf_descriptors = vec![
            // ColumnFamilyDescriptor::new("default", Options::default()),
            ColumnFamilyDescriptor::new("cf1", Options::default()),
        ];

        // default
        let ttls = TTLOpenDescriptor::by_columns(vec![-1, 100]);

        let db = DBWithTTL::open_cf_descriptors_with_descriptor(&opts, &path, cf_descriptors, ttls)
            .unwrap();
        let cf1 = db.cf_handle("cf1").unwrap();

        assert!(db.put_cf(cf1, b"k1", b"v1111").is_ok());
        let r: Result<Option<DBVector>, Error> = db.get_cf(cf1, b"k1");

        assert!(r.unwrap().unwrap().to_utf8().unwrap() == "v1111");
    }
}
