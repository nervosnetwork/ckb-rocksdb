extern crate ckb_rocksdb as rocksdb;

use rocksdb::{Options, SstFileWriter};

fn main() {
    let opts = Options::default();
    let writer = SstFileWriter::create(&opts);
    std::thread::scope(|scope| {
        scope.spawn(|| writer.file_size());
        scope.spawn(|| writer.file_size());
    });
}
