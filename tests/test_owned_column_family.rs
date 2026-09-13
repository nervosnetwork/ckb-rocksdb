use ckb_rocksdb::{
    IteratorMode, OptimisticTransactionDB, Options, ReadOptions,
    ops::{DropCF, GetColumnFamilys, IterateCF, OpenCF, PutCF},
};
use std::sync::Arc;

#[test]
fn owned_columns_keep_reads_and_database_alive_after_drop() {
    let directory = tempfile::tempdir().unwrap();
    let mut options = Options::default();
    options.create_if_missing(true);
    options.create_missing_column_families(true);
    let db = OptimisticTransactionDB::open_cf(&options, directory.path(), ["body"]).unwrap();
    let (db, mut columns) = db.into_shared_columns();
    let old = columns.remove("body").unwrap();
    for _ in 0..64 {
        assert!(db.create_owned_cf("body", &options).is_err());
    }
    assert!(db.get_cfs().is_empty());
    let weak = Arc::downgrade(&db);
    {
        let transaction = db.transaction_default();
        transaction.put_cf(&old, b"a", b"old-a").unwrap();
        transaction.put_cf(&old, b"b", b"old-b").unwrap();
        transaction.commit().unwrap();
    }
    let pin = old
        .get_pinned(b"a", &ReadOptions::default())
        .unwrap()
        .unwrap();
    let snapshot = db.snapshot();
    let mut snapshot_options = ReadOptions::default();
    snapshot_options.set_snapshot(&snapshot);
    let mut iterator = db
        .iterator_cf_opt(&old, IteratorMode::Start, &snapshot_options)
        .unwrap();
    let next = db.create_owned_cf("next", &options).unwrap();
    db.put_cf(&next, b"a", b"new-a").unwrap();
    let dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let (ready, reading) = std::sync::mpsc::channel();
    let reader = {
        let old = Arc::clone(&old);
        let dropped = Arc::clone(&dropped);
        std::thread::spawn(move || {
            let options = ReadOptions::default();
            assert!(old.get_pinned(b"a", &options).unwrap().is_some());
            ready.send(()).unwrap();
            let mut reads = 0;
            while !dropped.load(std::sync::atomic::Ordering::Acquire) {
                assert_eq!(
                    old.get_pinned(b"a", &options).unwrap().unwrap().as_ref(),
                    b"old-a"
                );
                reads += 1;
            }
            assert_eq!(
                old.get_pinned(b"b", &options).unwrap().unwrap().as_ref(),
                b"old-b"
            );
            reads
        })
    };
    reading.recv().unwrap();
    old.drop_from_database().unwrap();
    dropped.store(true, std::sync::atomic::Ordering::Release);
    reader.join().unwrap();
    assert_eq!(
        old.get_pinned(b"b", &snapshot_options)
            .unwrap()
            .unwrap()
            .as_ref(),
        b"old-b"
    );
    assert_eq!(
        iterator
            .by_ref()
            .map(|(_, value)| value.into_vec())
            .collect::<Vec<_>>(),
        [b"old-a".to_vec(), b"old-b".to_vec()]
    );
    iterator.status().unwrap();
    assert_eq!(
        next.get_pinned(b"a", &ReadOptions::default())
            .unwrap()
            .unwrap()
            .as_ref(),
        b"new-a"
    );
    drop(iterator);
    drop(snapshot);
    drop(old);
    drop(next);
    drop(columns);
    drop(db);
    assert_eq!(pin.as_ref(), b"old-a");
    assert!(weak.upgrade().is_some());
    drop(pin);
    assert!(weak.upgrade().is_none());
    let reopened = OptimisticTransactionDB::open_cf(&options, directory.path(), ["next"]).unwrap();
    assert!(reopened.cf_handle("body").is_none());
    assert!(reopened.cf_handle("next").is_some());
}

#[test]
fn failed_drop_keeps_legacy_and_owned_default_handles_usable() {
    let directory = tempfile::tempdir().unwrap();
    let mut options = Options::default();
    options.create_if_missing(true);
    let mut db = OptimisticTransactionDB::open_cf(&options, directory.path(), ["default"]).unwrap();
    assert!(db.drop_cf("default").is_err());
    db.put_cf(db.cf_handle("default").unwrap(), b"a", b"before")
        .unwrap();
    let (db, columns) = db.into_shared_columns();
    let default = &columns["default"];
    assert!(default.drop_from_database().is_err());
    db.put_cf(default, b"a", b"after").unwrap();
    assert_eq!(
        default
            .get_pinned(b"a", &ReadOptions::default())
            .unwrap()
            .unwrap()
            .as_ref(),
        b"after"
    );
}

#[test]
fn repeated_owned_column_creation_and_retirement_reopens_cleanly() {
    let directory = tempfile::tempdir().unwrap();
    let mut options = Options::default();
    options.create_if_missing(true);
    let db = OptimisticTransactionDB::open_cf(&options, directory.path(), ["default"]).unwrap();
    let (db, columns) = db.into_shared_columns();
    for generation in 0..64u64 {
        let column = db
            .create_owned_cf(&format!("payload.{generation}"), &options)
            .unwrap();
        db.put_cf(&column, b"key", generation.to_le_bytes())
            .unwrap();
        let peer = Arc::clone(&column);
        std::thread::spawn(move || {
            assert_eq!(
                peer.get_pinned(b"key", &ReadOptions::default())
                    .unwrap()
                    .unwrap()
                    .as_ref(),
                generation.to_le_bytes()
            );
        })
        .join()
        .unwrap();
        column.drop_from_database().unwrap();
    }
    drop(columns);
    drop(db);
    let families = ckb_rocksdb::DB::list_cf(&options, directory.path()).unwrap();
    assert_eq!(families, ["default"]);
    OptimisticTransactionDB::open_cf(&options, directory.path(), families).unwrap();
}

#[test]
fn column_batches_keep_order_and_partial_failures_keep_the_created_prefix() {
    let directory = tempfile::tempdir().unwrap();
    let mut options = Options::default();
    options.create_if_missing(true);
    let db = OptimisticTransactionDB::open_cf(&options, directory.path(), ["default"]).unwrap();
    let (db, columns) = db.into_shared_columns();
    assert!(
        db.create_owned_cfs::<&str>(&[], &options)
            .unwrap()
            .is_empty()
    );
    let batch = db
        .create_owned_cfs(&["first", "second", "third"], &options)
        .unwrap();
    assert_eq!(
        batch.iter().map(|column| column.name()).collect::<Vec<_>>(),
        ["first", "second", "third"]
    );
    for column in &batch {
        db.put_cf(column, b"key", column.name()).unwrap();
        assert_eq!(
            column
                .get_pinned(b"key", &ReadOptions::default())
                .unwrap()
                .unwrap()
                .as_ref(),
            column.name().as_bytes()
        );
    }
    assert!(
        db.create_owned_cfs(&["first", "never-created"], &options)
            .is_err()
    );
    assert!(
        db.create_owned_cfs(&["prefix", "first", "also-not-created"], &options)
            .is_err()
    );
    assert!(
        db.create_owned_cfs(&["valid-before-nul", "bad\0name"], &options)
            .is_err()
    );
    drop((batch, columns, db));
    let mut names = ckb_rocksdb::DB::list_cf(&options, directory.path()).unwrap();
    names.sort();
    assert_eq!(names, ["default", "first", "prefix", "second", "third"]);
    let reopened = OptimisticTransactionDB::open_cf(&options, directory.path(), names).unwrap();
    for name in ["first", "second", "third"] {
        let column = reopened.cf_handle(name).unwrap();
        use ckb_rocksdb::ops::GetPinnedCF;
        assert_eq!(
            reopened
                .get_pinned_cf(column, b"key")
                .unwrap()
                .unwrap()
                .as_ref(),
            name.as_bytes()
        );
    }
}
