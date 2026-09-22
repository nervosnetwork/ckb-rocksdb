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

use crate::rocksdb::{ColumnFamilyDescriptor, MergeOperands, TemporaryDBPath, prelude::*};

#[test]
fn test_column_family() {
    let n = TemporaryDBPath::new();

    // should be able to create column families
    {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.set_merge_operator_associative("test operator", test_provided_merge);
        let mut db = DB::open(&opts, &n).unwrap();
        let opts = Options::default();
        match db.create_cf("cf1", &opts) {
            Ok(_db) => println!("cf1 created successfully"),
            Err(e) => {
                panic!("could not create column family: {}", e);
            }
        }
    }

    // should fail to open db without specifying same column families
    {
        let mut opts = Options::default();
        opts.set_merge_operator_associative("test operator", test_provided_merge);
        match DB::open(&opts, &n) {
            Ok(_db) => panic!(
                "should not have opened DB successfully without \
                        specifying column
            families"
            ),
            Err(e) => assert!(
                e.to_string()
                    .starts_with("Invalid argument: Column families not opened: cf1"),
                "error msg {}",
                e
            ),
        }
    }

    // should properly open db when specyfing all column families
    {
        let mut opts = Options::default();
        opts.set_merge_operator_associative("test operator", test_provided_merge);
        match DB::open_cf(&opts, &n, ["cf1"]) {
            Ok(_db) => println!("successfully opened db with column family"),
            Err(e) => panic!("failed to open db with column family: {}", e),
        }
    }

    // should be able to list a cf
    {
        let opts = Options::default();
        let vec = DB::list_cf(&opts, &n);
        match vec {
            Ok(vec) => assert_eq!(vec, vec!["default", "cf1"]),
            Err(e) => panic!("failed to drop column family: {}", e),
        }
    }

    // TODO should be able to use writebatch ops with a cf
    {}
    // TODO should be able to iterate over a cf
    {}
    // should b able to drop a cf
    {
        let mut db = DB::open_cf(&Options::default(), &n, ["cf1"]).unwrap();
        match db.drop_cf("cf1") {
            Ok(_) => println!("cf1 successfully dropped."),
            Err(e) => panic!("failed to drop column family: {}", e),
        }
    }
}

#[test]
fn test_can_open_db_with_results_of_list_cf() {
    // Test scenario derived from GitHub issue #175 and 177

    let n = TemporaryDBPath::new();

    {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        let mut db = DB::open(&opts, &n).unwrap();
        let opts = Options::default();

        assert!(db.create_cf("cf1", &opts).is_ok());
    }

    {
        let options = Options::default();
        let cfs = DB::list_cf(&options, &n).unwrap();
        let db = DB::open_cf(&options, &n, cfs).unwrap();

        assert!(db.cf_handle("cf1").is_some());
    }
}

#[test]
fn test_create_missing_column_family() {
    let n = TemporaryDBPath::new();

    // should be able to create new column families when opening a new database
    {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);

        match DB::open_cf(&opts, &n, ["cf1"]) {
            Ok(_db) => println!("successfully created new column family"),
            Err(e) => panic!("failed to create new column family: {}", e),
        }
    }
}

#[test]
fn test_merge_operator() {
    let path = TemporaryDBPath::new();
    let mut options = Options::default();
    options.create_if_missing(true);
    options.create_missing_column_families(true);
    options.set_merge_operator_associative("concat", test_provided_merge);
    let db = DB::open_cf_descriptors(
        &options,
        &path,
        [ColumnFamilyDescriptor::new("cf1", options.clone())],
    )
    .unwrap();
    let cf = db.cf_handle("cf1").unwrap();
    db.put_cf(cf, b"key", b"a").unwrap();
    db.merge_cf(cf, b"key", b"bc").unwrap();
    assert_eq!(db.get_cf(cf, b"key").unwrap().unwrap().as_ref(), b"abc");
    let values: Vec<_> = db
        .iterator_cf(cf, rocksdb::IteratorMode::Start)
        .unwrap()
        .collect();
    assert_eq!(values.len(), 1);
    assert_eq!(&*values[0].1, b"abc");
    db.delete_cf(cf, b"key").unwrap();
    assert!(db.get_cf(cf, b"key").unwrap().is_none());
}

#[allow(clippy::unnecessary_wraps)]
fn test_provided_merge(
    _: &[u8],
    existing_val: Option<&[u8]>,
    operands: &mut MergeOperands,
) -> Option<Vec<u8>> {
    let nops = operands.size_hint().0;
    let mut result: Vec<u8> = Vec::with_capacity(nops);
    if let Some(v) = existing_val {
        for e in v {
            result.push(*e);
        }
    }
    for op in operands {
        for e in op {
            result.push(*e);
        }
    }
    Some(result)
}

#[test]
fn test_column_family_with_options() {
    let n = TemporaryDBPath::new();
    {
        let mut cfopts = Options::default();
        cfopts.set_max_write_buffer_number(16);
        let cf_descriptor = ColumnFamilyDescriptor::new("cf1", cfopts);

        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);

        let cfs = vec![cf_descriptor];
        match DB::open_cf_descriptors(&opts, &n, cfs) {
            Ok(_db) => println!("created db with column family descriptors successfully"),
            Err(e) => {
                panic!(
                    "could not create new database with column family descriptors: {}",
                    e
                );
            }
        }
    }

    {
        let mut cfopts = Options::default();
        cfopts.set_max_write_buffer_number(16);
        let cf_descriptor = ColumnFamilyDescriptor::new("cf1", cfopts);

        let opts = Options::default();
        let cfs = vec![cf_descriptor];

        match DB::open_cf_descriptors(&opts, &n, cfs) {
            Ok(_db) => println!("successfully re-opened database with column family descriptors"),
            Err(e) => {
                panic!(
                    "unable to re-open database with column family descriptors: {}",
                    e
                );
            }
        }
    }
}

#[test]
fn test_create_duplicate_column_family() {
    let n = TemporaryDBPath::new();
    {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);

        let mut db = match DB::open_cf(&opts, &n, ["cf1"]) {
            Ok(d) => d,
            Err(e) => panic!("failed to create new column family: {}", e),
        };

        assert!(db.create_cf("cf1", &opts).is_err());
    }
}
