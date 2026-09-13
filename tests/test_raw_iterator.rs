// Copyright 2014 Tyler Neely
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
extern crate ckb_rocksdb as rocksdb;

use crate::rocksdb::{TemporaryDBPath, prelude::*};

#[test]
pub fn test_forwards_iteration() {
    let n = TemporaryDBPath::new();
    {
        let db = DB::open_default(&n).unwrap();
        db.put(b"k1", b"v1").unwrap();
        db.put(b"k2", b"v2").unwrap();
        db.put(b"k3", b"v3").unwrap();
        db.put(b"k4", b"v4").unwrap();

        let mut iter = db.raw_iterator();
        iter.seek_to_first();

        assert!(iter.valid());
        assert_eq!(iter.key(), Some(b"k1".as_ref()));
        assert_eq!(iter.value(), Some(b"v1".as_ref()));
        assert_eq!(iter.item(), Some((b"k1".as_slice(), b"v1".as_slice())));

        iter.next();

        assert!(iter.valid());
        assert_eq!(iter.key(), Some(b"k2".as_ref()));
        assert_eq!(iter.value(), Some(b"v2".as_ref()));

        iter.next(); // k3
        iter.next(); // k4
        iter.next(); // invalid!

        assert!(!iter.valid());
        assert_eq!(iter.key(), None);
        assert_eq!(iter.value(), None);
        assert_eq!(iter.item(), None);
    }
}

#[test]
pub fn test_seek_last() {
    let n = TemporaryDBPath::new();
    {
        let db = DB::open_default(&n).unwrap();
        db.put(b"k1", b"v1").unwrap();
        db.put(b"k2", b"v2").unwrap();
        db.put(b"k3", b"v3").unwrap();
        db.put(b"k4", b"v4").unwrap();

        let mut iter = db.raw_iterator();
        iter.seek_to_last();

        assert!(iter.valid());
        assert_eq!(iter.key(), Some(b"k4".as_ref()));
        assert_eq!(iter.value(), Some(b"v4".as_ref()));

        iter.prev();

        assert!(iter.valid());
        assert_eq!(iter.key(), Some(b"k3".as_ref()));
        assert_eq!(iter.value(), Some(b"v3".as_ref()));

        iter.prev(); // k2
        iter.prev(); // k1
        iter.prev(); // invalid!

        assert!(!iter.valid());
        assert_eq!(iter.key(), None);
        assert_eq!(iter.value(), None);
    }
}

#[test]
pub fn test_seek() {
    let n = TemporaryDBPath::new();
    {
        let db = DB::open_default(&n).unwrap();
        db.put(b"k1", b"v1").unwrap();
        db.put(b"k2", b"v2").unwrap();
        db.put(b"k4", b"v4").unwrap();

        let mut iter = db.raw_iterator();
        iter.seek(b"k2");

        assert!(iter.valid());
        assert_eq!(iter.key(), Some(b"k2".as_ref()));
        assert_eq!(iter.value(), Some(b"v2".as_ref()));

        // Check it gets the next key when the key doesn't exist
        iter.seek(b"k3");

        assert!(iter.valid());
        assert_eq!(iter.key(), Some(b"k4".as_ref()));
        assert_eq!(iter.value(), Some(b"v4".as_ref()));
    }
}

#[test]
pub fn test_seek_to_nonexistant() {
    let n = TemporaryDBPath::new();
    {
        let db = DB::open_default(&n).unwrap();
        db.put(b"k1", b"v1").unwrap();
        db.put(b"k3", b"v3").unwrap();
        db.put(b"k4", b"v4").unwrap();

        let mut iter = db.raw_iterator();
        iter.seek(b"k2");

        assert!(iter.valid());
        assert_eq!(iter.key(), Some(b"k3".as_ref()));
        assert_eq!(iter.value(), Some(b"v3".as_ref()));
    }
}

#[test]
pub fn test_seek_for_prev() {
    let n = TemporaryDBPath::new();
    {
        let db = DB::open_default(&n).unwrap();
        db.put(b"k1", b"v1").unwrap();
        db.put(b"k2", b"v2").unwrap();
        db.put(b"k4", b"v4").unwrap();

        let mut iter = db.raw_iterator();
        iter.seek(b"k2");

        assert!(iter.valid());
        assert_eq!(iter.key(), Some(b"k2".as_ref()));
        assert_eq!(iter.value(), Some(b"v2".as_ref()));

        // Check it gets the previous key when the key doesn't exist
        iter.seek_for_prev(b"k3");

        assert!(iter.valid());
        assert_eq!(iter.key(), Some(b"k2".as_ref()));
        assert_eq!(iter.value(), Some(b"v2".as_ref()));
    }
}

#[test]
fn iterator_test_past_end() {
    use crate::rocksdb::IteratorMode;

    let n = TemporaryDBPath::new();
    {
        let db = DB::open_default(&n).unwrap();
        db.put(b"k1", b"v1111").unwrap();
        let mut iter = db.iterator(IteratorMode::Start);
        assert!(iter.next().is_some());
        assert!(iter.next().is_none());
        assert!(iter.next().is_none());
    }
}

#[test]
fn iterator_status_reports_corrupt_data_block() {
    use rocksdb::{DBCompressionType, IteratorMode};
    use std::io::{Read, Seek, SeekFrom, Write};

    let directory = tempfile::tempdir().unwrap();
    let mut options = Options::default();
    options.create_if_missing(true);
    options.set_compression_type(DBCompressionType::None);
    options.set_disable_auto_compactions(true);
    let mut table_options = rocksdb::BlockBasedOptions::default();
    table_options.disable_cache();
    options.set_block_based_table_factory(&table_options);
    {
        let db = DB::open(&options, directory.path()).unwrap();
        for key in 0..128u32 {
            db.put(key.to_be_bytes(), [0x5a; 1024]).unwrap();
        }
        db.flush().unwrap();
        let mut iterator = db.iterator(IteratorMode::Start);
        assert_eq!(iterator.by_ref().count(), 128);
        iterator.status().unwrap();
    }
    let tables: Vec<_> = std::fs::read_dir(directory.path())
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.extension().is_some_and(|extension| extension == "sst"))
        .collect();
    assert_eq!(tables.len(), 1);
    let mut table = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&tables[0])
        .unwrap();
    // Preserve SST metadata so open succeeds, but invalidate the first data block.
    let mut byte = [0];
    table.seek(SeekFrom::Start(32)).unwrap();
    table.read_exact(&mut byte).unwrap();
    byte[0] ^= 1;
    table.seek(SeekFrom::Start(32)).unwrap();
    table.write_all(&byte).unwrap();
    table.sync_all().unwrap();
    drop(table);
    let db = DB::open(&options, directory.path()).unwrap();
    let mut raw = db.raw_iterator();
    raw.seek_to_first();
    assert!(!raw.valid());
    assert!(raw.status().unwrap_err().to_string().contains("Corruption"));
    let mut iterator = db.iterator(IteratorMode::Start);
    assert!(iterator.next().is_none());
    assert!(
        iterator
            .status()
            .unwrap_err()
            .to_string()
            .contains("Corruption")
    );
}
