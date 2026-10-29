extern crate ckb_rocksdb as rocksdb;

use rocksdb::OptimisticTransactionDB;
use rocksdb::ops::Open;

fn main() {
    let _txn = {
        let db = OptimisticTransactionDB::open_default("foo").unwrap();
        db.transaction_default()
    };
}
