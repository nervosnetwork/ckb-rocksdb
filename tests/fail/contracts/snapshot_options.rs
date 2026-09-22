use ckb_rocksdb::{DB, ReadOptions, prelude::*};
fn main() {
    let db = DB::open_default("unused").unwrap();
    let mut options = ReadOptions::default();
    options.set_snapshot(&db.snapshot());
}
