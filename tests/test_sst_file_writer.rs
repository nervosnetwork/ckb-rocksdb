extern crate ckb_rocksdb as rocksdb;

use crate::rocksdb::{TemporaryDBPath, prelude::*};
use rocksdb::SstFileWriter;

#[test]
fn sst_file_writer_works() {
    let path = TemporaryDBPath::new();
    let dir = tempfile::Builder::new()
        .prefix("_rust_rocksdb_sstfilewritertest")
        .tempdir()
        .expect("Failed to create temporary path for file writer.");

    let writer_path = dir.path().join("filewriter");
    {
        let opts = Options::default();
        let mut writer = SstFileWriter::create(&opts);
        writer.open(&writer_path).unwrap();
        writer.put(b"k1", b"v1").unwrap();

        writer.put(b"k2", b"v2").unwrap();

        writer.delete(b"k3").unwrap();
        writer.finish().unwrap();
        assert!(writer.file_size() > 0);
    }
    {
        let db = DB::open_default(&path).unwrap();
        db.put(b"k3", b"v3").unwrap();
        db.ingest_external_file(vec![&writer_path]).unwrap();
        let r: Result<Option<DBVector>, Error> = db.get(b"k1");
        assert_eq!(r.unwrap().unwrap().to_utf8().unwrap(), "v1");
        let r: Result<Option<DBVector>, Error> = db.get(b"k2");
        assert_eq!(r.unwrap().unwrap().to_utf8().unwrap(), "v2");
        assert!(db.get(b"k3").unwrap().is_none());
    }
}
