use ckb_rocksdb::{
    ColumnFamily, DB, OptimisticTransactionDB, Options, ReadOptions, TransactionDB, WriteOptions,
    prelude::*,
};

fn populate<W>(db: &(impl Put<W> + PutCF<W>), left: &ColumnFamily, right: &ColumnFamily) {
    for (key, value) in [(b"".as_slice(), b"empty".as_slice()), (b"key", b"value")] {
        db.put(key, value).unwrap();
        db.put_cf(left, key, value).unwrap();
    }
    db.put_cf(right, b"key", b"other").unwrap();
}

fn check(
    db: &(impl MultiGet<ReadOptions> + MultiGetCF<ReadOptions> + IterateCF),
    left: &ColumnFamily,
    right: &ColumnFamily,
) {
    // Owned, variable-length inputs must remain alive through the native call;
    // empty keys, missing keys and repeated keys keep their input positions.
    let input = ["key", "", "missing", "key"].map(String::from);
    let values: Vec<_> = db
        .multi_get(input)
        .into_iter()
        .map(|value| value.unwrap().map(|value| value.to_vec()))
        .collect();
    assert_eq!(
        values,
        [
            Some(b"value".to_vec()),
            Some(b"empty".to_vec()),
            None,
            Some(b"value".to_vec())
        ]
    );
    let input = [(right, "key"), (left, "key"), (left, "missing"), (left, "")]
        .map(|(cf, key)| (cf, key.to_owned()));
    let values: Vec<_> = db
        .multi_get_cf(input)
        .into_iter()
        .map(|value| value.unwrap().map(|value| value.to_vec()))
        .collect();
    assert_eq!(
        values,
        [
            Some(b"other".to_vec()),
            Some(b"value".to_vec()),
            None,
            Some(b"empty".to_vec())
        ]
    );
    assert!(db.multi_get(Vec::<String>::new()).is_empty());
    assert!(
        db.multi_get_cf(Vec::<(&ColumnFamily, String)>::new())
            .is_empty()
    );
    // A transaction must consume these keys before locking its native state.
    let values = db.multi_get(
        db.iterator(ckb_rocksdb::IteratorMode::Start)
            .map(|(key, _)| key),
    );
    assert_eq!(values.len(), 2);
    assert!(values.into_iter().all(|value| value.unwrap().is_some()));
    let keys = db
        .iterator_cf(left, ckb_rocksdb::IteratorMode::Start)
        .unwrap();
    let values = db.multi_get_cf(keys.map(|(key, _)| (left, key)));
    assert_eq!(values.len(), 2);
    assert!(values.into_iter().all(|value| value.unwrap().is_some()));

    // AsRef may return a different slice each time; length must describe the
    // actual owned key passed to native code.
    struct ChangingKey(std::cell::Cell<bool>);
    impl AsRef<[u8]> for ChangingKey {
        fn as_ref(&self) -> &[u8] {
            if self.0.replace(true) { b"k" } else { b"key" }
        }
    }
    let values = db.multi_get([ChangingKey(std::cell::Cell::new(false))]);
    assert_eq!(
        values[0].as_ref().unwrap().as_ref().unwrap().as_ref(),
        b"value"
    );
}

fn check_database<T>()
where
    T: OpenCF
        + GetColumnFamilys
        + Put<WriteOptions>
        + PutCF<WriteOptions>
        + MultiGet<ReadOptions>
        + MultiGetCF<ReadOptions>
        + IterateCF,
{
    let directory = tempfile::tempdir().unwrap();
    let mut options = Options::default();
    options.create_if_missing(true);
    options.create_missing_column_families(true);
    let db = T::open_cf(&options, directory.path(), ["left", "right"]).unwrap();
    let left = db.cf_handle("left").unwrap();
    let right = db.cf_handle("right").unwrap();
    populate(&db, left, right);
    check(&db, left, right);
}

#[test]
fn multi_get_keeps_owned_keys_and_column_families_in_input_order() {
    check_database::<DB>();
    check_database::<TransactionDB>();
    check_database::<OptimisticTransactionDB>();
}

#[test]
fn transaction_multi_get_includes_uncommitted_writes() {
    let directory = tempfile::tempdir().unwrap();
    let mut options = Options::default();
    options.create_if_missing(true);
    options.create_missing_column_families(true);
    {
        let db = TransactionDB::open_cf(&options, directory.path(), ["left", "right"]).unwrap();
        let (left, right) = (
            db.cf_handle("left").unwrap(),
            db.cf_handle("right").unwrap(),
        );
        let txn = db.transaction(&Default::default(), &Default::default());
        populate(&txn, left, right);
        check(&txn, left, right);
        check(&txn.snapshot(), left, right);
    }
    let db = std::sync::Arc::new(
        OptimisticTransactionDB::open_cf(&options, directory.path(), ["left", "right"]).unwrap(),
    );
    let (left, right) = (
        db.cf_handle("left").unwrap(),
        db.cf_handle("right").unwrap(),
    );
    let txn = db.transaction_default();
    populate(&txn, left, right);
    check(&txn, left, right);
    check(&txn.snapshot(), left, right);
}
