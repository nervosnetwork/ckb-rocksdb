extern crate ckb_rocksdb as rocksdb;

use rocksdb::OptimisticTransactionDB;
use rocksdb::ops::Open;

fn main() {
    let db = OptimisticTransactionDB::open_default("foo").unwrap();
    let txn = db.transaction_default();
    std::thread::scope(|scope| {
        let snapshot = txn.snapshot();
        scope.spawn(move || drop(snapshot));
    });
}
