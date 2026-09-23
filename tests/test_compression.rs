use ckb_rocksdb::{DB, DBCompressionType, Options, prelude::*};
use std::{ffi::OsStr, path::Path};

fn sst_count(path: &Path) -> usize {
    std::fs::read_dir(path)
        .unwrap()
        .map(Result::unwrap)
        .filter(|entry| entry.path().extension() == Some(OsStr::new("sst")))
        .count()
}

#[test]
#[cfg(all(feature = "snappy", feature = "lz4"))]
fn new_databases_default_to_lz4_and_explicit_options_take_precedence() {
    for codec in [DBCompressionType::Lz4, DBCompressionType::Snappy] {
        let directory = tempfile::tempdir().unwrap();
        let mut options = Options::default();
        options.create_if_missing(true);
        if codec == DBCompressionType::Snappy {
            options.set_compression_type(codec);
        }
        let db = DB::open(&options, directory.path()).unwrap();
        db.put(b"key", vec![b'x'; 8192]).unwrap();
        db.flush().unwrap();
        assert!(sst_count(directory.path()) > 0);
        drop(db);
        let saved = std::fs::read_dir(directory.path())
            .unwrap()
            .map(Result::unwrap)
            .map(|entry| entry.path())
            .filter(|path| {
                path.file_name()
                    .unwrap()
                    .to_string_lossy()
                    .starts_with("OPTIONS-")
            })
            .max()
            .unwrap();
        let text = std::fs::read_to_string(saved).unwrap();
        let expected = if codec == DBCompressionType::Lz4 {
            "compression=kLZ4Compression"
        } else {
            "compression=kSnappyCompression"
        };
        assert!(text.lines().any(|line| line.trim() == expected));
    }
}

#[test]
#[cfg(all(feature = "snappy", feature = "lz4"))]
fn snappy_ssts_remain_readable_after_switching_new_writes_to_lz4() {
    let directory = tempfile::tempdir().unwrap();
    let value = vec![b'x'; 8192];
    let mut snappy = Options::default();
    snappy.create_if_missing(true);
    snappy.set_compression_type(DBCompressionType::Snappy);
    {
        let db = DB::open(&snappy, directory.path()).unwrap();
        db.put(b"legacy", &value).unwrap();
        db.flush().unwrap();
    }
    assert_eq!(sst_count(directory.path()), 1);
    let mut lz4 = Options::default();
    lz4.set_disable_auto_compactions(true);
    {
        let db = DB::open(&lz4, directory.path()).unwrap();
        assert_eq!(
            db.get(b"legacy").unwrap().unwrap().as_ref(),
            value.as_slice()
        );
        db.put(b"new", &value).unwrap();
        db.flush().unwrap();
        assert_eq!(sst_count(directory.path()), 2);
    }
    let db = DB::open(&lz4, directory.path()).unwrap();
    for key in [b"legacy".as_slice(), b"new".as_slice()] {
        assert_eq!(db.get(key).unwrap().unwrap().as_ref(), value.as_slice());
    }
}

#[test]
fn enabled_codecs_round_trip_flushed_ssts() {
    for codec in [
        DBCompressionType::None,
        #[cfg(feature = "snappy")]
        DBCompressionType::Snappy,
        #[cfg(feature = "lz4")]
        DBCompressionType::Lz4,
        #[cfg(feature = "lz4")]
        DBCompressionType::Lz4hc,
        #[cfg(feature = "zstd")]
        DBCompressionType::Zstd,
        #[cfg(feature = "zlib")]
        DBCompressionType::Zlib,
        #[cfg(feature = "bzip2")]
        DBCompressionType::Bz2,
    ] {
        let directory = tempfile::tempdir().unwrap();
        let value = vec![b'x'; 16384];
        let mut options = Options::default();
        options.create_if_missing(true);
        options.set_compression_type(codec);
        {
            let db = DB::open(&options, directory.path()).unwrap();
            db.put(b"key", &value).unwrap();
            db.flush().unwrap();
        }
        assert!(sst_count(directory.path()) > 0);
        let db = DB::open(&Options::default(), directory.path()).unwrap();
        assert_eq!(db.get(b"key").unwrap().unwrap().as_ref(), value.as_slice());
    }
}
