use ckb_rocksdb::{OptimisticTransactionDB, Options, prelude::*};
use std::sync::Arc;

fn main() {
    let path = tempfile::tempdir().unwrap();
    let db = Arc::new(OptimisticTransactionDB::open_default(path.path()).unwrap());
    let cf = db.create_owned_cf("owned", &Options::default()).unwrap();
    let txn = db.transaction_default();
    let mut iter = txn.raw_iterator_cf(&cf).unwrap();
    drop(cf);
    iter.seek_to_first();
}
