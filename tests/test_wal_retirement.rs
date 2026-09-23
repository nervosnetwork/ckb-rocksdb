use ckb_rocksdb::{
    OptimisticTransactionDB, Options,
    ops::{Flush, FlushWal, Get, GetCF, GetColumnFamilys, OpenCF, Put, PutCF},
};
use std::{
    fs, thread,
    time::{Duration, Instant},
};

#[test]
fn metadata_flush_reclaims_wal_after_its_last_column_is_dropped() {
    check_retirement(false);
}

#[test]
fn metadata_flush_reclaims_wal_with_atomic_flush() {
    check_retirement(true);
}

fn check_retirement(atomic_flush: bool) {
    let directory = tempfile::tempdir().unwrap();
    let mut options = Options::default();
    options.create_if_missing(true);
    options.set_atomic_flush(atomic_flush);
    options.set_max_total_wal_size(1 << 20);
    options.set_write_buffer_size(64 << 20);
    let db = OptimisticTransactionDB::open_cf(&options, directory.path(), ["default"]).unwrap();
    let (db, columns) = db.into_shared_columns();
    let old = db.create_owned_cf("old", &options).unwrap();
    db.put(b"marker", b"durable").unwrap();
    db.put_cf(&old, b"payload", vec![b'o'; 2 << 20]).unwrap();
    // Rotate the WAL while the old CF still needs its predecessor. After the
    // drop, no live CF needs that predecessor, but the persisted minimum WAL
    // number must still advance through a flush before it can be deleted.
    db.flush().unwrap();
    let current = db.create_owned_cf("current", &options).unwrap();
    old.drop_from_database().unwrap();
    drop(old);
    // DropCF does not advance the persisted WAL boundary in upstream 11.8.1.
    // An empty flush is a no-op. Refreshing an application-owned metadata key
    // and flushing its CF gives WAL pressure a durable boundary to advance to.
    db.put(b"marker", b"durable").unwrap();
    db.flush().unwrap();
    let value = vec![b'n'; 256 << 10];
    for key in 0u8..32 {
        db.put_cf(&current, [key], &value).unwrap();
        thread::sleep(Duration::from_millis(10));
    }
    db.flush_wal(false).unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let bytes: u64 = fs::read_dir(directory.path())
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|path| path.extension().is_some_and(|ext| ext == "log"))
            .map(|path| match fs::metadata(path) {
                Ok(metadata) => metadata.len(),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => 0,
                Err(error) => panic!("WAL metadata: {error}"),
            })
            .sum();
        // The WAL limit is soft; allow an in-flight flush and the current WAL.
        // Without progress this fixture retains more than 10 MiB indefinitely.
        if bytes < 3 << 20 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "WAL retention stalled at {bytes} bytes"
        );
        // WAL pressure is checked by writes. A pending flush may have finished
        // since the last payload write, so give that check another opportunity.
        db.put(b"pressure-probe", b"").unwrap();
        thread::sleep(Duration::from_millis(10));
    }
    drop((current, columns, db));
    let db = OptimisticTransactionDB::open_cf(&options, directory.path(), ["current"]).unwrap();
    assert_eq!(db.get(b"marker").unwrap().unwrap().as_ref(), b"durable");
    for key in 0u8..32 {
        assert_eq!(
            db.get_cf(db.cf_handle("current").unwrap(), [key])
                .unwrap()
                .unwrap()
                .as_ref(),
            value
        );
    }
}
