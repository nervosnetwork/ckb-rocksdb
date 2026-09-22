use ckb_rocksdb::{
    IteratorMode, OptimisticTransactionDB, OptimisticTransactionOptions, TransactionDB, prelude::*,
};
use std::sync::Arc;

#[test]
fn optimistic_transaction_keeps_its_database_alive() {
    let path = tempfile::tempdir().unwrap();
    let (txn, weak) = {
        let db = Arc::new(OptimisticTransactionDB::open_default(path.path()).unwrap());
        (db.transaction_default(), Arc::downgrade(&db))
    };
    assert!(weak.upgrade().is_some());
    txn.put(b"key", b"value").unwrap();
    txn.commit().unwrap();
    assert_eq!(txn.get(b"key").unwrap().unwrap().as_ref(), b"value");
    drop(txn);
    assert!(weak.upgrade().is_none());
}

#[test]
fn optimistic_views_prevent_invalidation_and_pins_keep_their_bytes() {
    let path = tempfile::tempdir().unwrap();
    let db = Arc::new(OptimisticTransactionDB::open_default(path.path()).unwrap());
    let mut options = OptimisticTransactionOptions::default();
    options.set_snapshot(true);
    let txn = db.transaction(&Default::default(), &options);
    txn.put(b"key", b"old").unwrap();
    let pin = txn.get_pinned(b"key").unwrap().unwrap();
    txn.put(b"key", b"new").unwrap();
    assert_eq!(pin.as_ref(), b"old");
    txn.set_savepoint();
    let snapshot = txn.snapshot();
    assert!(
        txn.commit()
            .unwrap_err()
            .as_ref()
            .contains("active snapshots")
    );
    assert!(txn.rollback().is_err());
    assert!(txn.rollback_to_savepoint().is_err());
    txn.put(b"another", b"value").unwrap();
    assert_eq!(snapshot.get(b"key").unwrap().unwrap().as_ref(), b"new");
    drop(snapshot);

    let mut iter = txn.raw_iterator();
    iter.seek(b"key");
    let bytes = iter.value().unwrap();
    let overlapping_snapshot = txn.snapshot();
    assert_eq!(
        txn.commit().unwrap_err().as_ref(),
        "transaction has active iterators"
    );
    drop(overlapping_snapshot);
    assert!(txn.put(b"key", b"changed").is_err());
    assert!(txn.delete(b"key").is_err());
    assert!(txn.commit().is_err());
    assert!(txn.rollback_to_savepoint().is_err());
    assert_eq!(bytes, b"new");
    std::thread::scope(|scope| {
        scope
            .spawn(|| assert!(txn.commit().is_err()))
            .join()
            .unwrap();
        scope
            .spawn(|| assert_eq!(txn.get(b"key").unwrap().unwrap().as_ref(), b"new"))
            .join()
            .unwrap();
    });
    drop(iter);
    txn.rollback_to_savepoint().unwrap();
    txn.commit().unwrap();
    assert_eq!(pin.as_ref(), b"old");
}

#[test]
fn pessimistic_transaction_views_obey_the_same_invalidation_rules() {
    let path = tempfile::tempdir().unwrap();
    let db = TransactionDB::open_default(path.path()).unwrap();
    let txn = db.transaction_default();
    txn.put(b"key", b"value").unwrap();
    let snapshot = txn.snapshot();
    assert!(txn.commit().is_err());
    drop(snapshot);
    let mut iter = txn.raw_iterator();
    iter.seek_to_first();
    let overlapping_snapshot = txn.snapshot();
    assert_eq!(
        txn.rollback().unwrap_err().as_ref(),
        "transaction has active iterators"
    );
    drop(overlapping_snapshot);
    assert!(txn.rollback().is_err());
    assert_eq!(iter.value(), Some(b"value".as_slice()));
    drop(iter);
    txn.commit().unwrap();
}

#[test]
fn shared_transaction_serializes_parallel_writers() {
    let path = tempfile::tempdir().unwrap();
    let db = Arc::new(OptimisticTransactionDB::open_default(path.path()).unwrap());
    let txn = db.transaction_default();
    std::thread::scope(|scope| {
        for writer in 0..4u8 {
            let txn = &txn;
            scope.spawn(move || {
                for key in 0..100u8 {
                    txn.put([writer, key], [key, writer]).unwrap();
                    assert_eq!(
                        txn.get([writer, key]).unwrap().unwrap().as_ref(),
                        [key, writer]
                    );
                }
            });
        }
    });
    txn.commit().unwrap();
    assert_eq!(db.iterator(IteratorMode::Start).count(), 400);
}

#[test]
fn caller_key_conversion_can_read_the_transaction() {
    struct Key<F>(F);
    impl<F: Fn()> AsRef<[u8]> for Key<F> {
        fn as_ref(&self) -> &[u8] {
            (self.0)();
            b"key"
        }
    }
    let path = tempfile::tempdir().unwrap();
    let db = Arc::new(OptimisticTransactionDB::open_default(path.path()).unwrap());
    let txn = db.transaction_default();
    txn.put(b"key", b"value").unwrap();
    let key = Key(|| {
        assert!(txn.get(b"key").unwrap().is_some());
    });
    assert!(txn.get(&key).unwrap().is_some());
    assert!(txn.get_pinned(&key).unwrap().is_some());
    assert!(txn.get_for_update(&key).unwrap().is_some());
    txn.put(&key, &key).unwrap();
    let snapshot = txn.snapshot();
    assert!(snapshot.get_pinned(&key).unwrap().is_some());
    drop(snapshot);
    let mut iter = txn.raw_iterator();
    iter.seek(&key);
    iter.seek_for_prev(&key);
    assert!(iter.valid());
    drop(iter);
    txn.put(&key, b"updated").unwrap();
    txn.delete(&key).unwrap();
    txn.commit().unwrap();
}

#[test]
fn owned_cf_iterator_retains_its_database_and_resolves_transaction_merges() {
    use ckb_rocksdb::{MergeOperands, Options, ReadOptions};
    let path = tempfile::tempdir().unwrap();
    let db = Arc::new(OptimisticTransactionDB::open_default(path.path()).unwrap());
    let mut options = Options::default();
    options.set_merge_operator_associative(
        "merge",
        |_: &[u8], base: Option<&[u8]>, operands: &mut MergeOperands| {
            let mut value = base.unwrap_or_default().to_vec();
            for operand in operands {
                value.extend_from_slice(operand);
            }
            Some(value)
        },
    );
    let cf = db.create_owned_cf("owned", &options).unwrap();
    let txn = db.transaction_default();
    txn.merge_cf(&cf, b"key", b"value").unwrap();
    let mut iter = txn.raw_iterator_cf(&cf).unwrap();
    iter.seek_to_first();
    assert_eq!(iter.value(), Some(b"value".as_slice()));
    drop(iter);
    txn.commit().unwrap();
    drop(txn);

    let weak = Arc::downgrade(&cf);
    let iter = cf.iterator_opt(IteratorMode::Start, &ReadOptions::default());
    drop((cf, db));
    assert!(weak.upgrade().is_some());
    let values: Vec<_> = iter.collect();
    assert_eq!(&*values[0].1, b"value");
    assert!(weak.upgrade().is_none());
}

#[test]
fn unsafe_native_contracts_require_explicit_acknowledgement() {
    trybuild::TestCases::new().compile_fail("tests/fail/contracts/*.rs");
}
