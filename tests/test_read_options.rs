use ckb_rocksdb::{
    DB, DBWithTTL, IteratorMode, OptimisticTransactionDB, Options, ReadOnlyDB, ReadOptions,
    SecondaryDB, SecondaryOpenDescriptor, TransactionDB, prelude::*,
};

fn keys(mut iter: ckb_rocksdb::DBIterator<'_>) -> Vec<Vec<u8>> {
    let keys = iter.by_ref().map(|(key, _)| key.into_vec()).collect();
    iter.status().unwrap();
    keys
}

#[test]
fn lower_bound_is_inclusive_and_upper_bound_is_exclusive() {
    let directory = tempfile::tempdir().unwrap();
    let db = DB::open_default(directory.path()).unwrap();
    for key in [b"k1", b"k2", b"k3", b"k4"] {
        db.put(key, key).unwrap();
    }
    let mut options = ReadOptions::default();
    options.set_iterate_lower_bound(b"k2");
    options.set_iterate_upper_bound(b"k4");
    assert_eq!(
        keys(db.iterator_opt(IteratorMode::Start, &options)),
        [b"k2".to_vec(), b"k3".to_vec()]
    );
    assert_eq!(
        keys(db.iterator_opt(IteratorMode::End, &options)),
        [b"k3".to_vec(), b"k2".to_vec()]
    );
}

#[test]
fn cloned_read_options_preserve_snapshot() {
    let directory = tempfile::tempdir().unwrap();
    let db = DB::open_default(directory.path()).unwrap();
    db.put(b"key", b"old").unwrap();
    let snapshot = db.snapshot();
    db.put(b"key", b"new").unwrap();
    let mut options = ReadOptions::default();
    // The DB and snapshot outlive the options, clone and iterator.
    unsafe { options.set_snapshot(&snapshot) };
    let cloned = options.clone();
    drop(options);
    assert_eq!(
        db.get_opt(b"key", &cloned).unwrap().unwrap().as_ref(),
        b"old"
    );
    let mut iter = db.iterator_opt(IteratorMode::Start, &cloned);
    assert_eq!(iter.next().unwrap().1.as_ref(), b"old");
    iter.status().unwrap();
}

fn assert_iterator_owns_bounds(db: &impl Iterate) {
    let mut options = ReadOptions::default();
    options.set_iterate_upper_bound(b"k4");
    let mut iter = db.get_raw_iter(&options);
    // Updating the caller's options must not change an existing iterator's range.
    options.set_iterate_upper_bound(b"k2");
    iter.seek_to_last();
    assert_eq!(iter.key(), Some(b"k3".as_slice()));
    // The iterator must also work after the caller's options have been released.
    drop(options);
    iter.seek_to_first();
    assert_eq!(iter.key(), Some(b"k1".as_slice()));
    iter.seek_to_last();
    assert_eq!(iter.key(), Some(b"k3".as_slice()));
    iter.status().unwrap();
}

#[test]
fn all_database_iterators_own_their_read_options() {
    let directory = tempfile::tempdir().unwrap();
    let db = DB::open_default(directory.path()).unwrap();
    for key in [b"k1", b"k2", b"k3", b"k4"] {
        db.put(key, key).unwrap();
    }
    assert_iterator_owns_bounds(&db);
    assert_iterator_owns_bounds(&db.snapshot());
    db.flush().unwrap();
    let secondary_path = tempfile::tempdir().unwrap();
    let secondary = SecondaryDB::open_with_descriptor(
        &Options::default(),
        directory.path(),
        SecondaryOpenDescriptor::new(secondary_path.path().to_str().unwrap().to_owned()),
    )
    .unwrap();
    assert_iterator_owns_bounds(&secondary);
    drop((secondary, db));
    let read_only = ReadOnlyDB::open(&Options::default(), directory.path()).unwrap();
    assert_iterator_owns_bounds(&read_only);
    drop(read_only);

    let db = std::sync::Arc::new(OptimisticTransactionDB::open_default(directory.path()).unwrap());
    assert_iterator_owns_bounds(db.as_ref());
    assert_iterator_owns_bounds(&db.snapshot());
    let txn = db.transaction_default();
    assert_iterator_owns_bounds(&txn);
    assert_iterator_owns_bounds(&txn.snapshot());
    drop(txn);
    drop(db);

    let db = TransactionDB::open_default(directory.path()).unwrap();
    assert_iterator_owns_bounds(&db);
    assert_iterator_owns_bounds(&db.snapshot());
    let txn = db.transaction(&Default::default(), &Default::default());
    assert_iterator_owns_bounds(&txn);
    assert_iterator_owns_bounds(&txn.snapshot());
    drop(txn);
    drop(db);

    let ttl_directory = tempfile::tempdir().unwrap();
    let db = DBWithTTL::open_default(ttl_directory.path()).unwrap();
    for key in [b"k1", b"k2", b"k3", b"k4"] {
        db.put(key, key).unwrap();
    }
    assert_iterator_owns_bounds(&db);
}

#[test]
fn column_family_and_snapshot_iterators_preserve_bounds() {
    let directory = tempfile::tempdir().unwrap();
    let mut options = Options::default();
    options.create_if_missing(true);
    options.create_missing_column_families(true);
    let db = DB::open_cf(&options, directory.path(), ["data"]).unwrap();
    let cf = db.cf_handle("data").unwrap();
    for key in [b"k1", b"k2", b"k3", b"k4"] {
        db.put_cf(cf, key, b"old").unwrap();
    }
    let snapshot = db.snapshot();
    db.put_cf(cf, b"k2", b"new").unwrap();
    let mut read_options = ReadOptions::default();
    read_options.set_iterate_lower_bound(b"k2");
    read_options.set_iterate_upper_bound(b"k4");
    let mut iter = snapshot
        .iterator_cf_opt(cf, IteratorMode::Start, &read_options)
        .unwrap();
    drop(read_options);
    let entries: Vec<_> = iter.by_ref().collect();
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].0.as_ref(), b"k2");
    assert_eq!(entries[0].1.as_ref(), b"old");
    assert_eq!(entries[1].0.as_ref(), b"k3");
    iter.status().unwrap();
}
