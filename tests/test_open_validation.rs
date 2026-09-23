use ckb_rocksdb::{
    ColumnFamilyDescriptor, DB, DBWithTTL, OptimisticTransactionDB, Options, ReadOnlyDB,
    SecondaryDB, TransactionDB, prelude::*,
};
use std::sync::Arc;

fn assert_invalid_column_name<T: OpenCF>() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("uncreated");
    let mut options = Options::default();
    options.create_if_missing(true);
    options.create_missing_column_families(true);
    let result = T::open_cf(&options, &path, ["valid", "invalid\0name"]);
    let error = result.err().expect("invalid names must return an error");
    assert!(error.to_string().contains("NUL"), "{error}");
    assert!(!path.exists(), "validation must precede database creation");
}

#[test]
fn invalid_column_names_return_errors_for_every_database_type() {
    assert_invalid_column_name::<DB>();
    assert_invalid_column_name::<OptimisticTransactionDB>();
    assert_invalid_column_name::<TransactionDB>();
    assert_invalid_column_name::<DBWithTTL>();
    assert_invalid_column_name::<ReadOnlyDB>();
    assert_invalid_column_name::<SecondaryDB>();
}

#[test]
fn duplicate_column_names_are_rejected_before_opening() {
    let directory = tempfile::tempdir().unwrap();
    let mut options = Options::default();
    options.create_if_missing(true);
    options.create_missing_column_families(true);
    drop(DB::open_cf(&options, directory.path(), ["data"]).unwrap());
    let result = DB::open_cf(&options, directory.path(), ["data", "data"]);
    let error = result.expect_err("duplicate names must return an error");
    assert!(
        error.to_string().contains("Duplicate column family"),
        "{error}"
    );
    // A rejected open must not leave the database locked.
    drop(DB::open_cf(&options, directory.path(), ["data"]).unwrap());
}

#[test]
fn transaction_database_releases_column_family_resources_on_close() {
    let directory = tempfile::tempdir().unwrap();
    let retained = Arc::new(());
    let weak = Arc::downgrade(&retained);
    let mut column_options = Options::default();
    column_options.set_merge_operator_associative("retained-resource", move |_, value, _| {
        let _keep_alive = &retained;
        value.map(<[u8]>::to_vec)
    });
    let mut options = Options::default();
    options.create_if_missing(true);
    options.create_missing_column_families(true);
    let db = TransactionDB::open_cf_descriptors(
        &options,
        directory.path(),
        [ColumnFamilyDescriptor::new("data", column_options)],
    )
    .unwrap();
    let cf = db.cf_handle("data").unwrap();
    db.put_cf(cf, b"key", b"value").unwrap();
    assert!(weak.upgrade().is_some());
    drop(db);
    assert!(
        weak.upgrade().is_none(),
        "column family resources leaked after close"
    );
    drop(TransactionDB::open_cf(&options, directory.path(), ["data"]).unwrap());
}
