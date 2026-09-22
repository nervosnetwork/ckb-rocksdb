use ckb_rocksdb::{CompactionDecision, DB, Options, prelude::*};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

struct Dropped(Arc<AtomicUsize>);
impl Drop for Dropped {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn direct_callbacks_outlive_options_clones_and_dynamic_column_families() {
    let path = tempfile::tempdir().unwrap();
    let mut db = DB::open_default(path.path()).unwrap();
    let drops = Arc::new(AtomicUsize::new(0));
    let marker = Dropped(Arc::clone(&drops));
    let mut options = Options::default();
    options.set_compaction_filter("owned-filter", move |_, _, _| {
        let _keep = &marker;
        CompactionDecision::Keep
    });
    let clone = options.clone();
    db.create_cf("dynamic", &clone).unwrap();
    drop((options, clone));
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    let cf = db.cf_handle("dynamic").unwrap();
    db.put_cf(cf, b"key", b"value").unwrap();
    db.compact_range_cf(cf, None::<&[u8]>, None::<&[u8]>);
    assert_eq!(db.get_cf(cf, b"key").unwrap().unwrap().as_ref(), b"value");
    db.drop_cf("dynamic").unwrap();
    // Native background work may still retain a dropped CF until DB shutdown.
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(db);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[test]
fn replacing_an_unused_filter_releases_its_owner() {
    let drops = Arc::new(AtomicUsize::new(0));
    let marker = Dropped(Arc::clone(&drops));
    let mut options = Options::default();
    options.set_compaction_filter("first", move |_, _, _| {
        let _keep = &marker;
        CompactionDecision::Keep
    });
    options.set_compaction_filter("second", |_, _, _| CompactionDecision::Keep);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}
