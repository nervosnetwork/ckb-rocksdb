// Copyright 2019 Tyler Neely
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

extern crate ckb_rocksdb as rocksdb;
use libc::size_t;

use crate::rocksdb::{IteratorMode, TemporaryDBPath, WriteBatch, prelude::*};

#[test]
fn test_db_vector() {
    use std::mem;
    let len: size_t = 4;
    let data: *mut u8 = unsafe { libc::calloc(len, mem::size_of::<u8>()) as *mut u8 };
    let v = unsafe { DBVector::from_c(data, len) };
    let ctrl = [0u8, 0, 0, 0];
    assert_eq!(&*v, &ctrl[..]);
}

#[test]
fn external() {
    let path = TemporaryDBPath::new();

    {
        let db = DB::open_default(&path).unwrap();

        assert!(db.put(b"k1", b"v1111").is_ok());

        let r: Result<Option<DBVector>, Error> = db.get(b"k1");

        assert!(r.unwrap().unwrap().to_utf8().unwrap() == "v1111");
        assert!(db.delete(b"k1").is_ok());
        assert!(db.get(b"k1").unwrap().is_none());
    }
}

#[test]
fn db_vector_as_ref_byte_slice() {
    let path = TemporaryDBPath::new();

    {
        let db = DB::open_default(&path).unwrap();

        assert!(db.put(b"k1", b"v1111").is_ok());

        let r: Result<Option<DBVector>, Error> = db.get(b"k1");
        let vector = r.unwrap().unwrap();

        assert!(get_byte_slice(&vector) == b"v1111");
    }
}

fn get_byte_slice<T: AsRef<[u8]>>(source: &'_ T) -> &'_ [u8] {
    source.as_ref()
}

#[test]
fn errors_do_stuff() {
    let path = TemporaryDBPath::new();
    let _db = DB::open_default(&path).unwrap();
    let opts = Options::default();
    // The DB will still be open when we try to destroy it and the lock should fail.
    match DB::destroy(&opts, &path) {
        Err(s) => {
            let message = s.to_string();
            assert!(message.contains("IO error:"));
            assert!(message.contains("/LOCK:"));
        }
        Ok(_) => panic!("should fail"),
    }
}

#[test]
fn writebatch_works() {
    let path = TemporaryDBPath::new();
    {
        let db = DB::open_default(&path).unwrap();
        {
            // test put
            let mut batch = WriteBatch::default();
            assert!(db.get(b"k1").unwrap().is_none());
            assert_eq!(batch.len(), 0);
            assert!(batch.is_empty());
            let _ = batch.put(b"k1", b"v1111");
            assert_eq!(batch.len(), 1);
            assert!(!batch.is_empty());
            assert!(db.get(b"k1").unwrap().is_none());
            assert!(db.write(&batch).is_ok());
            let r: Result<Option<DBVector>, Error> = db.get(b"k1");
            assert!(r.unwrap().unwrap().to_utf8().unwrap() == "v1111");
        }
        {
            // test delete
            let mut batch = WriteBatch::default();
            let _ = batch.delete(b"k1");
            assert_eq!(batch.len(), 1);
            assert!(!batch.is_empty());
            assert!(db.write(&batch).is_ok());
            assert!(db.get(b"k1").unwrap().is_none());
        }
        {
            // test size_in_bytes
            let mut batch = WriteBatch::default();
            let before = batch.size_in_bytes();
            let _ = batch.put(b"k1", b"v1234567890");
            let after = batch.size_in_bytes();
            assert!(before + 10 <= after);
        }
    }
}

#[test]
fn iterator_test() {
    let path = TemporaryDBPath::new();
    {
        let data = [(b"k1", b"v1111"), (b"k2", b"v2222"), (b"k3", b"v3333")];
        let db = DB::open_default(&path).unwrap();

        for (key, value) in &data {
            assert!(db.put(key, value).is_ok());
        }

        let iter = db.iterator(IteratorMode::Start);

        for (idx, (db_key, db_value)) in iter.enumerate() {
            let (key, value) = data[idx];
            assert_eq!((&key[..], &value[..]), (db_key.as_ref(), db_value.as_ref()));
        }
    }
}

#[test]
fn snapshot_test() {
    let path = TemporaryDBPath::new();
    {
        let db = DB::open_default(&path).unwrap();

        assert!(db.put(b"k1", b"v1111").is_ok());

        let snap = db.snapshot();
        assert!(snap.get(b"k1").unwrap().unwrap().to_utf8().unwrap() == "v1111");

        assert!(db.put(b"k2", b"v2222").is_ok());

        assert!(db.get(b"k2").unwrap().is_some());
        assert!(snap.get(b"k2").unwrap().is_none());
    }
}

#[test]
fn set_option_test() {
    let path = TemporaryDBPath::new();
    {
        let db = DB::open_default(&path).unwrap();
        // set an option to valid values
        assert!(
            db.set_options(&[("disable_auto_compactions", "true")])
                .is_ok()
        );
        assert!(
            db.set_options(&[("disable_auto_compactions", "false")])
                .is_ok()
        );
        // invalid names/values should result in an error
        assert!(
            db.set_options(&[("disable_auto_compactions", "INVALID_VALUE")])
                .is_err()
        );
        assert!(
            db.set_options(&[("INVALID_NAME", "INVALID_VALUE")])
                .is_err()
        );
        // option names/values must not contain NULLs
        assert!(
            db.set_options(&[("disable_auto_compactions", "true\0")])
                .is_err()
        );
        assert!(
            db.set_options(&[("disable_auto_compactions\0", "true")])
                .is_err()
        );
        // empty options are not allowed
        assert!(db.set_options(&[]).is_err());
        // multiple options can be set in a single API call
        let multiple_options = [
            ("paranoid_file_checks", "true"),
            ("report_bg_io_stats", "true"),
        ];
        db.set_options(&multiple_options).unwrap();
    }
}

#[test]
fn set_option_cf_test() {
    let path = TemporaryDBPath::new();
    {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        let db = DB::open_cf(&opts, &path, ["cf1"]).unwrap();
        let cf1 = db.cf_handle("cf1").unwrap();
        // set an option to valid values
        assert!(
            db.set_options_cf(cf1, &[("disable_auto_compactions", "true")])
                .is_ok()
        );
        assert!(
            db.set_options_cf(cf1, &[("disable_auto_compactions", "false")])
                .is_ok()
        );
        // invalid names/values should result in an error
        assert!(
            db.set_options_cf(cf1, &[("disable_auto_compactions", "INVALID_VALUE")])
                .is_err()
        );
        assert!(
            db.set_options_cf(cf1, &[("INVALID_NAME", "INVALID_VALUE")])
                .is_err()
        );
        // option names/values must not contain NULLs
        assert!(
            db.set_options_cf(cf1, &[("disable_auto_compactions", "true\0")])
                .is_err()
        );
        assert!(
            db.set_options_cf(cf1, &[("disable_auto_compactions\0", "true")])
                .is_err()
        );
        // empty options are not allowed
        assert!(db.set_options_cf(cf1, &[]).is_err());
        // multiple options can be set in a single API call
        let multiple_options = [
            ("paranoid_file_checks", "true"),
            ("report_bg_io_stats", "true"),
        ];
        db.set_options_cf(cf1, &multiple_options).unwrap();
    }
}

#[test]
fn test_open_utf8_path() {
    let tmp = TemporaryDBPath::new();
    let path = tmp.join("_rust_rocksdb_utf8_path_temporärer_Ordner");
    {
        let db = DB::open_default(path).unwrap();

        assert!(db.put(b"k1", b"v1111").is_ok());

        let r = db.get(b"k1");
        assert_eq!(r.unwrap().unwrap().to_utf8().unwrap(), "v1111");
        assert!(db.delete(b"k1").is_ok());
        assert!(db.get(b"k1").unwrap().is_none());
    }
}

#[test]
fn multi_get() {
    let tmp = TemporaryDBPath::new();
    let path = tmp.join("_rust_rocksdb_multi_get");

    {
        let db = DB::open_default(path).unwrap();
        let initial_snap = db.snapshot();
        db.put(b"k1", b"v1").unwrap();
        let k1_snap = db.snapshot();
        db.put(b"k2", b"v2").unwrap();

        let _ = db.multi_get([b"k0"; 40]);

        let assert_values = |values: Vec<_>| {
            assert_eq!(3, values.len());
            assert_eq!(values[0], None);
            assert_eq!(values[1], Some(b"v1".to_vec()));
            assert_eq!(values[2], Some(b"v2".to_vec()));
        };

        let values = db
            .multi_get([b"k0", b"k1", b"k2"])
            .into_iter()
            .map(Result::unwrap)
            .map(|v| v.map(|v| v.to_vec()))
            .collect::<Vec<_>>();

        assert_values(values);

        let values = db
            .multi_get_opt([b"k0", b"k1", b"k2"], &Default::default())
            .into_iter()
            .map(Result::unwrap)
            .map(|v| v.map(|v| v.to_vec()))
            .collect::<Vec<_>>();

        assert_values(values);

        let values = db
            .snapshot()
            .multi_get([b"k0", b"k1", b"k2"])
            .into_iter()
            .map(Result::unwrap)
            .map(|v| v.map(|v| v.to_vec()))
            .collect::<Vec<_>>();

        assert_values(values);

        let none_values = initial_snap
            .multi_get([b"k0", b"k1", b"k2"])
            .into_iter()
            .map(Result::unwrap)
            .map(|v| v.map(|v| v.to_vec()))
            .collect::<Vec<_>>();

        assert_eq!(none_values, vec![None; 3]);

        let k1_only = k1_snap
            .multi_get([b"k0", b"k1", b"k2"])
            .into_iter()
            .map(Result::unwrap)
            .map(|v| v.map(|v| v.to_vec()))
            .collect::<Vec<_>>();

        assert_eq!(k1_only, vec![None, Some(b"v1".to_vec()), None]);
    }
}

#[test]
fn multi_get_cf() {
    let tmp = TemporaryDBPath::new();
    let path = tmp.join("_rust_rocksdb_multi_get_cf");

    {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        let db = DB::open_cf(&opts, path, ["cf0", "cf1", "cf2"]).unwrap();

        let cf0 = db.cf_handle("cf0").unwrap();

        let cf1 = db.cf_handle("cf1").unwrap();
        db.put_cf(cf1, b"k1", b"v1").unwrap();

        let cf2 = db.cf_handle("cf2").unwrap();
        db.put_cf(cf2, b"k2", b"v2").unwrap();

        let values = db
            .multi_get_cf(vec![(cf0, b"k0"), (cf1, b"k1"), (cf2, b"k2")])
            .into_iter()
            .map(Result::unwrap)
            .map(|v| v.map(|v| v.to_vec()))
            .collect::<Vec<_>>();
        assert_eq!(3, values.len());
        assert_eq!(values[0], None);
        assert_eq!(values[1], Some(b"v1".to_vec()));
        assert_eq!(values[2], Some(b"v2".to_vec()));
    }
}

#[test]
fn batched_multi_get_cf() {
    let tmp = TemporaryDBPath::new();
    let path = tmp.join("_rust_rocksdb_batched_multi_get_cf");

    {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        let db = DB::open_cf(&opts, path, ["cf0"]).unwrap();

        let cf = db.cf_handle("cf0").unwrap();
        db.put_cf(cf, b"k1", b"v1").unwrap();
        db.put_cf(cf, b"k2", b"v2").unwrap();

        let values = db
            .batched_multi_get_cf(cf, vec![b"k0", b"k1", b"k2"], true) // sorted_input
            .into_iter()
            .map(Result::unwrap)
            .map(|v| v.map(|v| v.to_vec()))
            .collect::<Vec<_>>();
        assert_eq!(3, values.len());
        assert!(values[0].is_none());
        assert!(values[1].is_some());
        assert_eq!(&(values[1].as_ref().unwrap())[0..2], b"v1");
        assert_eq!(&(values[2].as_ref().unwrap())[0..2], b"v2");
    }
}
